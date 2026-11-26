"""The words engine's protocol logic against a stub model and a daemon the suite plays."""

from __future__ import annotations

import itertools
import json
import os
import signal
import socket
import sys
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pytest

import transcribe
from transcribe import Conn, Words, flag, main, samples, session, socket_path, stand, terms

if TYPE_CHECKING:
    from collections.abc import Iterator


class Stub:
    """A stream that hears a level as a word: each run of one nonzero sample value fed is a line,
    `w<level in hundredths>`, timed by where it lies in what was fed.
    """

    def __init__(self) -> None:
        """Nothing fed."""
        self.fed: list[np.ndarray] = []

    def add(self, pcm: np.ndarray) -> None:
        """Keeps what is fed."""
        self.fed.append(pcm)

    def lines(self) -> list[tuple[str, float, float]]:
        """Each run of one nonzero level, as a line."""
        audio = np.concatenate([np.zeros(0, np.float32), *self.fed])
        edges = np.flatnonzero(np.diff(np.concatenate([[0], audio, [0]])))
        runs = itertools.pairwise(edges)
        return [
            (f"w{round(audio[low] * 100)}", low / 16_000, high / 16_000)
            for low, high in runs
            if audio[low]
        ]

    def stop(self) -> list[tuple[str, float, float]]:
        """The lines, as final."""
        return self.lines()


class Model:
    """Stub streams, each kept."""

    def __init__(self) -> None:
        """None made yet."""
        self.streams: list[Stub] = []

    def stream(self) -> Stub:
        """A fresh stub stream."""
        self.streams.append(Stub())
        return self.streams[-1]


def frame(level: float = 0.0) -> np.ndarray:
    """A 20 ms frame of one value."""
    return np.full(320, level, np.float32)


def same(text: str) -> str:
    """Text unchanged."""
    return text


def run(words: Words, events: list[tuple]) -> list[dict]:
    """Every report of `events` in order: `(t, level, count)` frames from `t` µs, `("on"|"off",
    t)` voice lines, `("turn", end)` turn lines.
    """
    said = []
    for event in events:
        match event:
            case ("on" | "off", start):
                words.voiced(event[0] == "on", start)
            case ("turn", end):
                said += words.turn(end)
            case (start, level, count):
                for at in range(count):
                    said += words.frame(start + at * 20_000, frame(level))
    return said


def finals(said: list[dict]) -> list[list[tuple[str, int, int]]]:
    """Each final's words as text and times."""
    return [
        [(word["text"], word["start"], word["end"]) for word in report["words"]]
        for report in said
        if report["op"] == "words"
    ]


def test_terms(tmp_path: Path) -> None:
    """A term in any case, or a heard form across punctuation, becomes the term."""
    path = tmp_path / "terms.txt"
    path.write_text("# a comment\n\nPostgres: post grass, postgress\nMoonshine\n:\n")
    rewrite = terms(str(path))
    assert (
        rewrite("ask Post, grass and MOONSHINE postgress") == "ask Postgres and Moonshine Postgres"
    )
    assert terms(None)("post grass") == "post grass"


def test_samples() -> None:
    """16-bit little-endian hex as floats."""
    assert samples("0000ff7f0080").tolist() == [0.0, 32767 / 32768, -1.0]


def test_quiet() -> None:
    """No voice line: nothing fed, nothing said, and no more held than the buffer."""
    model = Model()
    words = Words(model, same, None)
    assert run(words, [(0, 0.05, 2_000), ("turn", 30_000_000)]) == []
    assert (model.streams, len(words.kept)) == ([], 1_500)


def test_utterance() -> None:
    """A voice's stream from its start less the pre-roll, fed behind the newest frame to the
    voice's end, finished at the turn over its audio alone; a partial once while it changed.
    """
    model = Model()
    words = Words(model, str.upper, 500_000)
    said = run(
        words,
        [
            (0, 0.0, 50),
            (1_000_000, 0.05, 6),
            ("on", 1_000_000),
            (1_120_000, 0.05, 44),
            (2_000_000, 0.0, 13),
            ("off", 2_000_000),
            (2_260_000, 0.0, 6),
            ("turn", 2_000_000),
        ],
    )
    assert [report["op"] for report in said] == ["partial", "words"]
    assert said[-1] == {
        "end": 2_000_000,
        "op": "words",
        "start": 750_000,
        "words": [{"end": 2_000_000, "start": 1_000_000, "text": "W5"}],
    }
    assert sum(map(len, model.streams[0].fed)) == 20_000, "1.25 s: the pre-roll and the voice"


def test_held() -> None:
    """A turn held across two voices is one stream and one final, its pause included."""
    model = Model()
    said = run(
        Words(model, same, None),
        [
            ("on", 1_000_000),
            (1_000_000, 0.05, 25),
            ("off", 1_500_000),
            (1_500_000, 0.0, 25),
            ("on", 2_000_000),
            (2_000_000, 0.06, 25),
            ("off", 2_500_000),
            (2_500_000, 0.0, 15),
            ("turn", 2_500_000),
        ],
    )
    assert len(model.streams) == 1
    assert finals(said) == [
        [("w5", 1_000_000, 1_500_000), ("w6", 2_000_000, 2_500_000)],
    ]


def test_late_turn() -> None:
    """A turn line after the next voice began: the final decoded afresh over audio to its end
    alone, the next stream from there, no word in both finals.
    """
    model = Model()
    said = run(
        Words(model, same, None),
        [
            ("on", 1_000_000),
            (1_000_000, 0.05, 50),
            ("off", 2_000_000),
            (2_000_000, 0.0, 25),
            ("on", 2_500_000),
            (2_500_000, 0.07, 50),
            ("turn", 2_000_000),
            ("off", 3_500_000),
            (3_500_000, 0.0, 15),
            ("turn", 3_500_000),
        ],
    )
    assert finals(said) == [[("w5", 1_000_000, 2_000_000)], [("w7", 2_500_000, 3_500_000)]]
    assert len(model.streams) == 3, "the held stream, its fresh decode, the next"


def test_replayed() -> None:
    """A reconnect replaying a voice and turn its last final already holds: neither opens nor
    keeps a stream, so the fresh voice after it is one final from its own pre-roll, its words
    alone.
    """
    model = Model()
    said = run(
        Words(model, same, 2_000_000),
        [
            ("on", 1_000_000),
            (1_000_000, 0.05, 25),
            ("off", 1_500_000),
            (1_500_000, 0.0, 15),
            ("turn", 1_500_000),
            (1_800_000, 0.0, 60),
            ("on", 3_000_000),
            (3_000_000, 0.07, 25),
            ("off", 3_500_000),
            (3_500_000, 0.0, 15),
            ("turn", 3_500_000),
        ],
    )
    spans = [(report["start"], report["end"]) for report in said if report["op"] == "words"]
    assert spans == [(2_750_000, 3_500_000)]
    assert finals(said) == [[("w7", 3_000_000, 3_500_000)]]
    assert len(model.streams) == 1, "the fresh voice's alone"


def test_hole() -> None:
    """Words after a hole carry the times their audio was heard, not the stream's."""
    said = run(
        Words(Model(), same, None),
        [
            ("on", 1_000_000),
            (1_000_000, 0.05, 25),
            (2_000_000, 0.06, 25),
            ("off", 2_500_000),
            (2_500_000, 0.0, 15),
            ("turn", 2_500_000),
        ],
    )
    assert finals(said) == [[("w5", 1_000_000, 1_500_000), ("w6", 2_000_000, 2_500_000)]]


def test_cap(capsys: pytest.CaptureFixture) -> None:
    """A voice that never ends: finished at each 20 s by the engine's own rule, said once, each
    final from the last one's end; a turn already passed, or with no stream, says nothing.
    """
    words = Words(Model(), same, None)
    said = run(words, [("on", 1), (0, 0.05, 2_100), ("turn", 100)])
    stops = [report["end"] for report in said if report["op"] == "words"]
    starts = [report["start"] for report in said if report["op"] == "words"]
    assert (stops, starts) == ([20_000_000, 40_000_000], [0, 20_000_000])
    assert capsys.readouterr().err.count("finished there") == 1
    assert Words(Model(), same, None).turn(5) == []
    # a line heard past the end it is finished at gives no word
    words.heard = [(0, 0)]
    assert words._timed([("late", 1.0, 2.0)], 500_000) == []


def serve(sock: socket.socket) -> tuple[socket.socket, object, dict]:
    """The next connection, its hello answered: it, its lines and its request."""
    conn, _ = sock.accept()
    lines = conn.makefile("r")
    json.loads(lines.readline())
    send(conn, {"ok": True, "proto": 1})
    return conn, lines, json.loads(lines.readline())


def send(conn: socket.socket, line: dict) -> None:
    """One line."""
    conn.sendall((json.dumps(line) + "\n").encode())


@pytest.fixture
def daemon() -> Iterator[tuple[str, socket.socket]]:
    """A listening socket in a temp directory, and its path."""
    with tempfile.TemporaryDirectory(dir="/tmp") as root:
        path = str(Path(root) / "rift.sock")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        sock.listen()
        yield path, sock
        sock.close()


def test_sessions(
    daemon: tuple[str, socket.socket],
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A session from 30 s back, finals from the reply's through, voice and turn lines acted
    on, a refusal said, the end an error; `gone` connects live; another refusal ends it.
    """
    path, sock = daemon
    monkeypatch.setattr(transcribe.time, "time_ns", lambda: 40_000_000_000)
    ends = []

    def runs() -> None:
        for _ in range(3):
            try:
                session(path, "ear-desk", Model(), same)
            except ConnectionError as cause:
                ends.append(str(cause))

    thread = threading.Thread(target=runs)
    thread.start()
    conn, lines, request = serve(sock)
    assert request == {
        "from": 10_000_000,
        "kind": "words",
        "name": "transcribe",
        "op": "engine",
        "track": "ear-desk",
    }
    send(conn, {"ok": True, "seq": 1, "through": 0})
    send(conn, {"voice": {"on": True, "start": 20_000}})
    hexed = "cd0c" * 320
    for at in range(16):
        send(conn, {"pcm": hexed, "t": 20_000 + at * 20_000})
    send(conn, {"voice": {"on": False, "start": 340_000}})
    send(conn, {"turn": {"end": 340_000, "start": 20_000}})
    assert json.loads(lines.readline())["words"][0]["text"] == "w10"
    send(conn, {"code": "bad_request", "ok": False})
    conn.shutdown(socket.SHUT_RDWR)
    conn, lines, request = serve(sock)
    send(conn, {"code": "gone", "ok": False})
    conn, lines, request = serve(sock)
    assert request["from"] is None
    send(conn, {"ok": True, "seq": 2, "through": None})
    conn.shutdown(socket.SHUT_RDWR)
    conn, lines, request = serve(sock)
    send(conn, {"code": "taken", "ok": False})
    thread.join()
    assert ends == ["the connection ended"] * 2 + ['{"code":"taken","ok":false}']
    assert "transcribe: refused:" in capsys.readouterr().err


def test_places(monkeypatch: pytest.MonkeyPatch) -> None:
    """The socket under $RIFT_ROOT, else the home's; a flag's value is the word after it."""
    monkeypatch.setenv("RIFT_ROOT", "r")
    assert socket_path() == "r/run/rift.sock"
    monkeypatch.delenv("RIFT_ROOT")
    monkeypatch.setenv("HOME", "/home")
    assert socket_path() == "/home/.local/state/rift/run/rift.sock"
    assert (flag(["x", "--terms", "t"], "terms"), flag(["--terms"], "terms")) == ("t", None)
    assert Conn.__init__.__doc__


def test_stand(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """A failure said once while it repeats, another said, a clean end clearing it; a stop
    signal one line and exit 0.
    """
    causes = iter([OSError("down"), OSError("down"), None, OSError("down"), ValueError("bad")])
    waits = []

    def failing() -> None:
        cause = next(causes)
        if cause:
            raise cause

    def wait(_: float) -> None:
        waits.append(1)
        if len(waits) == 5:
            os.kill(os.getpid(), signal.SIGTERM)

    monkeypatch.setattr(transcribe.time, "sleep", wait)
    monkeypatch.setenv("RIFT_POLL_MS", "1")
    with pytest.raises(SystemExit) as stopped:
        stand(failing)
    assert stopped.value.code == 0
    assert capsys.readouterr().err.splitlines() == [
        "transcribe: down",
        "transcribe: down",
        "transcribe: bad",
        "transcribe: stopped by SIGTERM",
    ]
    for number in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(number, signal.SIG_DFL)


def test_main(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """No track or no model: usage, 2; else the model loaded from --model and the engine stood;
    anything escaping, one line and 1.
    """
    assert (main([]), main(["ear-desk"])) == (2, 2)
    assert capsys.readouterr().err.startswith("usage: transcribe <track> --model <dir>")
    from transcribe import moonshine  # noqa: PLC0415 -- the runtime, imported as main does

    loaded = []
    monkeypatch.setattr(moonshine, "Moonshine", lambda path: loaded.append(path) or Model())
    monkeypatch.setenv("RIFT_ROOT", "/nonexistent")
    stood = []

    def once(session: object) -> None:
        with pytest.raises(OSError, match="No such file"):
            session()
        stood.append(1)

    monkeypatch.setattr(transcribe, "stand", once)
    monkeypatch.setattr(sys, "argv", ["transcribe", "ear-desk", "--model", "m"])
    assert main() == 0
    assert (loaded, stood) == (["m"], [1])

    def broken(_: object) -> None:
        message = "no model here"
        raise RuntimeError(message)

    monkeypatch.setattr(moonshine, "Moonshine", broken)
    assert main(["ear-desk", "--model", "m"]) == 1
    assert capsys.readouterr().err == "transcribe: no model here\n"

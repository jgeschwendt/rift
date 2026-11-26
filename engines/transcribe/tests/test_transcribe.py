"""The words engine's protocol logic against a stub model and a daemon the suite plays."""

from __future__ import annotations

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
    """A stream whose hypothesis is a script keyed by the samples fed so far."""

    def __init__(self, script: dict[int, list[tuple[str, float, float]]]) -> None:
        """Hypotheses by samples fed: the latest key at or under what was fed."""
        self.script, self.fed = script, []

    def add(self, pcm: np.ndarray) -> None:
        """Keeps what is fed."""
        self.fed.append(len(pcm))

    def lines(self) -> list[tuple[str, float, float]]:
        """The scripted hypothesis."""
        fed = sum(self.fed)
        return self.script[max(key for key in self.script if key <= fed)]

    def stop(self) -> list[tuple[str, float, float]]:
        """The scripted final."""
        return self.lines()


class Model:
    """Streams from one script, each kept."""

    def __init__(self, script: dict[int, list[tuple[str, float, float]]]) -> None:
        """Every stream reads `script`."""
        self.script, self.streams = script, []

    def stream(self) -> Stub:
        """A fresh stub stream."""
        self.streams.append(Stub(self.script))
        return self.streams[-1]


def frame(level: float = 0.0) -> np.ndarray:
    """A 20 ms frame of one value."""
    return np.full(320, level, np.float32)


def same(text: str) -> str:
    """Text unchanged."""
    return text


SCRIPT = {
    0: [],
    5_120: [("hello there", 0.0, 0.4)],
    10_240: [("hello there", 0.0, 0.4)],
    15_360: [("hello there", 0.0, 0.4), ("again", 0.3, 0.6), ("late", 2.0, 2.5)],
}


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


def test_partials() -> None:
    """A pass each 320 ms of audio fed: a partial when the hypothesis changed, none when empty or
    the same; audio before `start` never fed.
    """
    model = Model({**SCRIPT, 15_360: []})
    words = Words(model, same, 10_000)
    said = [report for at in range(64) for report in words.frame(at * 20_000, frame())]
    assert said == [
        {
            "end": 340_000,
            "op": "partial",
            "start": 10_000,
            "words": [
                {"end": 210_000, "start": 10_000, "text": "hello"},
                {"end": 340_000, "start": 210_000, "text": "there"},
            ],
        }
    ]
    assert model.streams[0].fed[0] == 160


def test_turns() -> None:
    """The final at a turn line: words inside the span, rising across overlapping lines, none
    starting past the end; the next utterance fed what was kept past the end; a turn already
    done, with nothing fed, or with no words heard, gives nothing.
    """
    model = Model(SCRIPT)
    words = Words(model, str.upper, None)
    assert words.turn(100) == []
    for at in range(50):
        words.frame(at * 20_000, frame())
    assert words.turn(800_000) == [
        {
            "end": 800_000,
            "op": "words",
            "start": 0,
            "words": [
                {"end": 200_000, "start": 0, "text": "HELLO"},
                {"end": 400_000, "start": 200_000, "text": "THERE"},
                {"end": 600_000, "start": 400_000, "text": "AGAIN"},
            ],
        }
    ]
    assert sum(model.streams[1].fed) == 1_000_000 * 16 // 1000 - 800_000 * 16 // 1000
    # a turn that heard no words logs nothing, and the next starts after it
    assert (words.turn(900_000), words.start) == ([], 900_000)
    assert words.turn(700_000) == []


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


def test_sessions(daemon: tuple[str, socket.socket], capsys: pytest.CaptureFixture) -> None:
    """A session: `"from":"through"`, its words from the reply's through, frames and turn lines
    read, partials and finals sent, a refusal said, the end an error; a refused connect too.
    """
    path, sock = daemon
    ends = []

    def run() -> None:
        for _ in range(2):
            try:
                session(path, "ear-desk", Model(SCRIPT), same)
            except ConnectionError as cause:
                ends.append(str(cause))

    thread = threading.Thread(target=run)
    thread.start()
    conn, lines, request = serve(sock)
    assert request == {
        "from": "through",
        "kind": "words",
        "name": "transcribe",
        "op": "engine",
        "track": "ear-desk",
    }
    send(conn, {"ok": True, "seq": 1, "through": 0})
    for at in range(16):
        send(conn, {"pcm": "0000" * 320, "t": at * 20_000})
    assert json.loads(lines.readline())["op"] == "partial"
    send(conn, {"ok": True})
    send(conn, {"turn": {"end": 300_000, "start": 0}})
    assert json.loads(lines.readline())["op"] == "words"
    send(conn, {"ok": True, "seq": 2})
    for at in range(16):
        send(conn, {"pcm": "0000" * 320, "t": 320_000 + at * 20_000})
    assert json.loads(lines.readline())["op"] == "partial"
    send(conn, {"ok": True})
    send(conn, {"turn": {"end": 640_000, "start": 300_000}})
    assert json.loads(lines.readline())["end"] == 640_000
    send(conn, {"code": "bad_request", "ok": False})
    send(conn, {"turn": {"end": 640_000, "start": 300_000}})
    conn.shutdown(socket.SHUT_RDWR)
    conn, lines, request = serve(sock)
    assert request["from"] == "through"
    send(conn, {"code": "taken", "ok": False})
    thread.join()
    assert ends == ["the connection ended", '{"code":"taken","ok":false}']
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

    def session() -> None:
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
        stand(session)
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
    """No track: usage, 2; else the model loaded from --model and the engine stood."""
    assert main([]) == 2
    assert capsys.readouterr().err.startswith("usage: transcribe <track>")
    from transcribe import moonshine  # noqa: PLC0415 -- the runtime, imported as main does

    loaded = []
    monkeypatch.setattr(moonshine, "Moonshine", lambda path: loaded.append(path) or Model(SCRIPT))
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

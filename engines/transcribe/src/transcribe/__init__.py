"""transcribe: rift's reference words engine, Moonshine v2 Medium Streaming on rift's socket.

usage: transcribe <track> --model <dir> [--terms <file>]

A words engine on the ear (or clean) track <track> (docs/protocol.html#engines). Its frames go
to a rolling buffer of 30 s; the model hears only speech: a stream opens at a voice's start (less
250 ms), is fed 300 ms behind the newest frame so it stops at the voice's end, stays open across
the voices of a turn held unfinished, and is finished at the turn line over audio to the turn's
end alone (decoded afresh from the buffer if it was fed past it), a partial each 320 ms whose
hypothesis changed. A stream past 20 s is finished by the engine's own rule. With no voice lines
(no turn engine on the track) it transcribes nothing. It connects from 30 s back, so a resume
never chews through more than the buffer. Moonshine times lines, not words: a line's words share
its span by length, each mapped back to the time its audio was heard, holes included. --model is
the model's directory (engines/README.md); --terms names a file of preferred terms, each line
`Term` or `Term: heard, heard`, applied after decoding.
"""

from __future__ import annotations

import bisect
import json
import os
import re
import signal
import socket
import sys
import time
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType

CAP = 320_000  # samples a stream may reach before the engine finishes it itself: 20 s
CLIENT = "transcribe/0.1.0"
ENDED = "the connection ended"
FRAME = 20_000  # µs a frame spans
KEEP = 1_500  # frames buffered: 30 s
LAG = 300_000  # µs a frame waits before it is fed: past the close window and its voice's line
PASS = 5_120  # samples fed between partials: 320 ms
PREROLL = 250_000  # µs of audio before a voice's start fed with it
RATE = 16_000


class Stream(Protocol):
    """One utterance's model stream."""

    def add(self, pcm: np.ndarray) -> None:
        """Feeds 16 kHz samples."""

    def lines(self) -> list[tuple[str, float, float]]:
        """The hypothesis so far: each line's text, start and end in seconds."""

    def stop(self) -> list[tuple[str, float, float]]:
        """The final lines, the stream closed."""


class Model(Protocol):
    """A speech-to-text model."""

    def stream(self) -> Stream:
        """A fresh stream."""


def terms(path: str | None) -> Callable[[str], str]:
    """The rewrite of text toward the preferred terms in `path`: each line `Term` or `Term: heard,
    heard`, a heard form (or the term in any case) as whole words becoming the term.
    """
    rules = []
    for line in Path(path).read_text().splitlines() if path else []:
        term, _, heard = (part.strip() for part in line.partition(":"))
        for said in [term, *heard.split(",")] if term and not term.startswith("#") else []:
            words = said.split()
            pattern = r"\b" + r"\W*".join(map(re.escape, words)) + r"\b"
            rules.extend([(re.compile(pattern, re.IGNORECASE), term)] if words else [])

    def rewrite(text: str) -> str:
        for rule, term in rules:
            text = rule.sub(lambda _, term=term: term, text)
        return text

    return rewrite


def samples(hexed: str) -> np.ndarray:
    """A frame's samples, 16-bit little-endian hex, as floats in [-1, 1)."""
    return np.frombuffer(bytes.fromhex(hexed), "<i2").astype(np.float32) / 32768


def index(us: int) -> int:
    """The sample `us` µs in, rounded up."""
    return -(-us * RATE // 1_000_000)


class Words:
    """One connection's utterances, every time on the daemon's clock in µs."""

    def __init__(self, model: Model, rewrite: Callable[[str], str], floor: int | None) -> None:
        """Words from `floor`, the last final's end, on."""
        self.model, self.rewrite, self.floor = model, rewrite, floor or 0
        self.kept: deque[tuple[int, np.ndarray]] = deque(maxlen=KEEP)
        self.stream: Stream | None = None
        # the stream's first time and how far it is fed; each piece's first sample and time
        self.begin = self.fed = 0
        self.heard: list[tuple[int, int]] = []
        self.count = self.since = 0
        self.voice: int | None = None
        self.said, self.capped = "", False

    def frame(self, t: int, pcm: np.ndarray) -> list[dict]:
        """The frame at `t` buffered; while a voice is on, fed what has waited its lag."""
        self.kept.append((t, pcm))
        if self.voice is None:
            return []
        self._feed(t + FRAME - LAG)
        return self._pass()

    def voiced(self, on: bool, start: int) -> None:  # noqa: FBT001 -- the wire's own field
        """A voice line: on opens a stream from `start` less the pre-roll unless one is open (a
        turn held across voices); off feeds it to `start`, the voice's end, and stops feeding.
        """
        # a voice at or before the last final's end, a replay's, opens nothing
        if on and self.stream is None and start > self.floor:
            self._open(max(self.floor, start - PREROLL))
        elif self.stream is not None and not on:
            self._feed(start)
        self.voice = start if on and self.stream is not None else None

    def turn(self, end: int) -> list[dict]:
        """The final words to a turn's `end` over its audio alone, none when it holds no words;
        a voice still on opens the next stream from there.
        """
        # a turn already finished, a replay's, drops whatever it held: nothing of it is due
        if self.stream is None or end <= self.floor:
            self.stream = self.voice = None
            return []
        if self.fed > end + FRAME:
            self._open(self.begin)
        self._feed(end)
        return self._finish(end)

    def _open(self, begin: int) -> None:
        self.stream, self.begin, self.fed = self.model.stream(), begin, begin
        self.heard, self.count, self.since, self.said = [], 0, 0, ""

    def _feed(self, until: int) -> None:
        """Feeds the buffered audio from how far it is fed to `until`."""
        # only the frames not yet fed, found from the newest back
        unfed = []
        for t, pcm in reversed(self.kept):
            if t + FRAME <= self.fed:
                break
            unfed.append((t, pcm))
        for t, pcm in reversed(unfed):
            low, high = max(0, index(self.fed - t)), min(len(pcm), index(until - t))
            if low < high:
                self.heard.append((self.count, t + low * 1_000_000 // RATE))
                self.stream.add(pcm[low:high])
                self.count, self.since = self.count + high - low, self.since + high - low
                self.fed = t + high * 1_000_000 // RATE

    def _pass(self) -> list[dict]:
        """A partial when a pass's hypothesis changed; past the cap, the final by its own rule."""
        if self.count >= CAP:
            # said once: a voice that never ends is a stuck turn engine, not news each 20 s
            if not self.capped:
                print("transcribe: a stream reached 20 s and was finished there", file=sys.stderr)
            self.capped = True
            return self._finish(self.fed)
        if self.since < PASS:
            return []
        self.since, lines = 0, self.stream.lines()
        text, words = " ".join(line[0] for line in lines), self._timed(lines, self.fed)
        if not words or text == self.said:
            return []
        self.said = text
        return [{"end": self.fed, "op": "partial", "start": self.begin, "words": words}]

    def _finish(self, end: int) -> list[dict]:
        """The stream's final words to `end`; the next stream from there if a voice is on."""
        final = {"end": end, "op": "words", "start": self.begin}
        final["words"] = self._timed(self.stream.stop(), end)
        self.stream, self.floor = None, end
        if self.voice is not None:
            self._open(end)
        return [final] if final["words"] else []

    def _at(self, sample: int, *, end: bool = False) -> int:
        """The time the stream's `sample` was heard; an `end`, exclusive, by the piece it ends."""
        key = (sample, -1 if end else 1 << 62)
        first, at = self.heard[max(0, bisect.bisect_left(self.heard, key) - 1)]
        return at + (sample - first) * 1_000_000 // RATE

    def _timed(self, lines: list[tuple[str, float, float]], end: int) -> list[dict]:
        """Each line's words, rewritten, sharing its span by length, rising, none past `end`."""
        timed, floor = [], self.begin
        for text, begin, stop in lines:
            parts = self.rewrite(text).split()
            first, span = round(begin * RATE), round((stop - begin) * RATE)
            share, total = 0, sum(map(len, parts))
            for part in parts:
                at = self._at(first + span * share // total)
                share += len(part)
                if at >= end:
                    break
                floor = max(floor, at)
                stops = min(max(self._at(first + span * share // total, end=True), floor), end)
                timed.append({"end": stops, "start": floor, "text": part})
                floor = stops
        return timed


class Conn:
    """One connection to the daemon, past its hello."""

    def __init__(self, path: str, request: dict) -> None:
        """Connects to `path`, says hello and sends `request`, its reply kept as `reply`."""
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)
        self.read = self.sock.makefile("r", encoding="utf-8")
        for line in ({"client": CLIENT, "op": "hello", "proto": 1}, request):
            self.send(line)
            self.reply = self.line()
            if self.reply.get("ok") is not True:
                self.close()
                raise ConnectionError(json.dumps(self.reply, separators=(",", ":")))

    def line(self) -> dict:
        """The next line, its end an error."""
        text = self.read.readline()
        if not text:
            raise ConnectionError(ENDED)
        return json.loads(text)

    def send(self, line: dict) -> None:
        """Writes `line` and its newline."""
        self.sock.sendall((json.dumps(line, separators=(",", ":")) + "\n").encode())

    def close(self) -> None:
        """Closes it."""
        self.read.close()
        self.sock.close()


def session(path: str, track: str, model: Model, rewrite: Callable[[str], str]) -> None:
    """One connection as the words engine of `track`: frames from the buffer's span back (this
    machine's clock is the daemon's), live when that is gone, finals from the reply's `through`
    on, so a start or a reconnect resumes and never chews through more than the buffer; each
    voice and turn line acted on, a refused report said; ends with the connection, an error.
    """
    request = {"kind": "words", "name": "transcribe", "op": "engine", "track": track}
    back = time.time_ns() // 1000 - KEEP * FRAME
    try:
        conn = Conn(path, {**request, "from": back})
    except ConnectionError as cause:
        if '"gone"' not in str(cause):
            raise
        conn = Conn(path, {**request, "from": None})
    words = Words(model, rewrite, conn.reply.get("through"))
    try:
        while line := conn.line():
            # a report's reply: only a refusal is news
            if line.get("ok") is False:
                print(f"transcribe: refused: {json.dumps(line)}", file=sys.stderr)
            voice, turn = line.get("voice"), line.get("turn")
            if voice:
                words.voiced(voice["on"], voice["start"])
            reports = words.turn(turn["end"]) if turn else []
            reports += words.frame(line["t"], samples(line["pcm"])) if "pcm" in line else []
            for report in reports:
                conn.send(report)
    finally:
        conn.close()


def socket_path() -> str:
    """The daemon's socket: run/rift.sock under $RIFT_ROOT, else under ~/.local/state/rift."""
    return str(
        Path(os.environ.get("RIFT_ROOT") or Path.home() / ".local/state/rift") / "run/rift.sock"
    )


def stopped(number: int, _frame: FrameType | None) -> None:
    """A stop signal's one stderr line, then exit 0."""
    print(f"transcribe: stopped by {signal.Signals(number).name}", file=sys.stderr)
    sys.exit(0)


def stand(session: Callable[[], None]) -> None:
    """Runs `session` forever as a device stands, each end waited out $RIFT_POLL_MS (default
    1000) ms, a connection's failure one stderr line said once until another; a stop signal ends
    it. Registering the handlers failing raises, so the engine exits rather than stand unstoppable.
    """
    for number in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(number, stopped)
    poll, said = int(os.environ.get("RIFT_POLL_MS", "1000")) / 1000, ""
    while True:
        try:
            session()
            said = ""
        except (OSError, ValueError) as failed:
            cause = str(failed)
            if cause != said:
                print(f"transcribe: {cause}", file=sys.stderr)
            said = cause
        time.sleep(poll)


def flag(args: list[str], name: str) -> str | None:
    """The value after `--<name>` among `args`."""
    at = args.index(f"--{name}") if f"--{name}" in args else len(args)
    return args[at + 1] if at + 1 < len(args) else None


def main(args: list[str] | None = None) -> int:
    """The command line: usage and 2 without a track or a model; else stands until a stop
    signal; anything else that escapes is one stderr line and 1, for launchd to restart it.
    """
    args = sys.argv[1:] if args is None else args
    if not args or args[0].startswith("-") or not flag(args, "model"):
        print(__doc__.splitlines()[2], file=sys.stderr)
        return 2
    try:
        from . import moonshine  # noqa: PLC0415 -- the runtime loads only when run, never in tests

        model, rewrite = moonshine.Moonshine(flag(args, "model")), terms(flag(args, "terms"))
        stand(lambda: session(socket_path(), args[0], model, rewrite))
    except Exception as cause:  # noqa: BLE001 -- the top level: one line, then launchd's restart
        print(f"transcribe: {cause}", file=sys.stderr)
        return 1
    return 0

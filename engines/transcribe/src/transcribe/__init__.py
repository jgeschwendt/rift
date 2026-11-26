"""transcribe: rift's reference words engine, Moonshine v2 Medium Streaming on rift's socket.

usage: transcribe <track> [--model <dir>] [--terms <file>]

A words engine on the ear (or clean) track <track> (docs/protocol.html#engines): its frames fed
to one stream per utterance, a partial each 320 ms of audio whose hypothesis changed, the final
words at each turn line, from the last final's end to the turn's. Moonshine times lines, not
words: a line's words share its span by length. --terms names a file of preferred terms, each
line `Term` or `Term: heard, heard`, applied to the text after decoding. --model is the model's
directory, by default moonshine-voice's own, fetched into its cache on first use.
"""

from __future__ import annotations

import json
import math
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

CLIENT = "transcribe/0.1.0"
ENDED = "the connection ended"
RATE = 16_000
PASS = 5_120  # samples fed between partials: 320 ms
KEEP = 1_500  # frames kept past a turn's end for the next utterance, 30 s: its line comes late


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


class Words:
    """One connection's utterances, every time on the daemon's clock in µs."""

    def __init__(self, model: Model, rewrite: Callable[[str], str], start: int | None) -> None:
        """Words from `start`, the last final's end (none: from the first frame)."""
        self.model, self.rewrite, self.start = model, rewrite, start
        self.kept: deque[tuple[int, np.ndarray]] = deque(maxlen=KEEP)
        self.stream: Stream | None = None
        self.fed, self.said = 0, ""

    def frame(self, t: int, pcm: np.ndarray) -> list[dict]:
        """The frame at `t` fed; a partial when a pass's hypothesis changed."""
        self.kept.append((t, pcm))
        return self._feed(t, pcm)

    def _feed(self, t: int, pcm: np.ndarray) -> list[dict]:
        self.start = t if self.start is None else self.start
        skip = max(0, math.ceil((self.start - t) * RATE / 1_000_000))
        if skip >= len(pcm):
            return []
        self.stream = self.stream or self.model.stream()
        self.stream.add(pcm[skip:])
        self.fed += len(pcm) - skip
        if self.fed < PASS:
            return []
        self.fed, end = 0, t + len(pcm) * 1_000_000 // RATE
        lines = self.stream.lines()
        text, words = " ".join(line[0] for line in lines), self._timed(lines, end)
        if not words or text == self.said:
            return []
        self.said = text
        return [{"end": end, "op": "partial", "start": self.start, "words": words}]

    def turn(self, end: int) -> list[dict]:
        """The final words up to a turn's `end`, none when it holds no words; the next utterance
        from there, fed what is kept.
        """
        if self.stream is None or self.start is None or end <= self.start:
            return []
        final = {"end": end, "op": "words", "start": self.start}
        final["words"] = self._timed(self.stream.stop(), end)
        self.stream, self.start, self.fed, self.said = None, end, 0, ""
        for t, pcm in self.kept:
            self._feed(t, pcm)
        return [final] if final["words"] else []

    def _timed(self, lines: list[tuple[str, float, float]], end: int) -> list[dict]:
        """Each line's words, rewritten, sharing its span by length, rising, none past `end`."""
        timed, floor = [], self.start or 0
        for text, begin, stop in lines:
            parts = self.rewrite(text).split()
            first, span = self.start + round(begin * 1e6), round((stop - begin) * 1e6)
            share, total = 0, sum(map(len, parts))
            for part in parts:
                at = first + span * share // total
                share += len(part)
                if at >= end:
                    break
                floor = max(floor, at)
                stops = min(max(first + span * share // total, floor), end)
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
    """One connection as the words engine of `track`: frames from its kind's `through`
    (`"from":"through"`, live when none), so a fresh or reconnected engine resumes in one
    connection, each turn line's final sent from there and a refused report said; ends with the
    connection, an error.
    """
    request = {"from": "through", "kind": "words", "name": "transcribe", "op": "engine"}
    conn = Conn(path, {**request, "track": track})
    words = Words(model, rewrite, conn.reply.get("through"))
    try:
        while line := conn.line():
            # a report's reply: only a refusal is news
            if line.get("ok") is False:
                print(f"transcribe: refused: {json.dumps(line)}", file=sys.stderr)
            turn = line.get("turn")
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
    1000) ms, a failure one stderr line said once until another; a stop signal ends it.
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
    """The command line: usage and 2 without a track, else stands until a stop signal."""
    args = sys.argv[1:] if args is None else args
    if not args or args[0].startswith("-"):
        print(__doc__.splitlines()[2], file=sys.stderr)
        return 2
    from . import moonshine  # noqa: PLC0415 -- the runtime loads only when run, never in tests

    model, rewrite = moonshine.Moonshine(flag(args, "model")), terms(flag(args, "terms"))
    stand(lambda: session(socket_path(), args[0], model, rewrite))
    return 0

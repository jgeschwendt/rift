"""The reference engines end to end: `prove.py <rift> <engines bin dir> <out dir> [--fast]`.

With --fast the ear sends the fixture as fast as it is answered, for a machine under load: its
real-time pacing sleeps between frames, and a busy machine wakes it ~200 ms late, which it plays
on from as a hole (measured 2026-11-26 · 8 busy loops on 16 cores: 52 holes in 200 frames), so
the stream it places is no longer the fixture. Its times are then not real time.

A throwaway daemon with its own root, a fake output under $RIFT_SPEAKER (no sound reaches a
speaker), the three engines connected, the fixture fed by a file-fed ear: voice, a turn, a final
words event naming the fixture's words, then a say of a reply, its spoken count and the sound the
fake output took. The feed's lines land in <out>/feed.jsonl, the verdict on stdout, exit 0 when it
holds. $PROVE_WORDS names the words engine, transcribe (the default) or scribe, its weights in
$TRANSCRIBE_MODEL or $SCRIBE_MODEL; the other models are read from their defaults (README.md).
With $PROVE_TERMS the fixture is fixtures/term.pcm (Pocket TTS saying "Open the Phlogix
dashboard, please.", an invented word scribe hears as Flogix without a vocabulary, measured
2026-11-26) and the words engine takes --terms with that file: the final must name Phlogix.

With $PROVE_TALKERS (diarize's weights in $DIARIZE_MODEL) it proves the talkers engine instead:
endpoint alone hears fixtures/voices.pcm (Pocket TTS, alba then marius, twice each); the first
turn's voice is enrolled as `first` from the daemon's read of it (`diarize enrol --track`); then
diarize connects and the fixture is played again. alba's turns must be mostly `first`, scored over
0.5; marius's never `first` over 0.5 (LS-EEND alone names his first phrase `first`, its score
under). The verdict adds each turn's ids, `first`'s scores and how long after the turn event the
span reaching its end was logged.
"""

# ruff: noqa: S603 -- it runs the programs it was handed

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from array import array
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

TERMS = os.environ.get("PROVE_TERMS")
FIXTURE, HEARD = ("term.pcm", ("phlogix",)) if TERMS else ("phrase.pcm", ("kitchen", "lights"))
HERE = Path(__file__).parent
REPLY = "Sure, the kitchen lights are on now. Anything else?"
SURE = 0.5  # the score over which a talkers label is the enrolled voice's


def ask(sock: str, *requests: dict, rest: bool = False) -> list[dict]:
    """Each request's reply on one connection, after a hello; with `rest`, the lines until its
    end too.
    """
    with socket.socket(socket.AF_UNIX) as conn:
        conn.connect(sock)
        read = conn.makefile("r")
        said = []
        for line in ({"client": "prove/0", "op": "hello", "proto": 1}, *requests):
            conn.sendall((json.dumps(line) + "\n").encode())
            said.append(json.loads(read.readline()))
        said.extend(json.loads(line) for line in (read if rest else []))
        return said[1:]


def at(event: dict) -> int:
    """An event's log time in µs since the epoch, the clock the stream's times are on."""
    return round(datetime.fromisoformat(event["t"]).timestamp() * 1e6)


def wait(until: Callable[[], object], what: str, budget: float = 60) -> object:
    """Polls `until` every 50 ms until it is truthy; fails naming `what` past `budget` s."""
    deadline = time.monotonic() + budget
    while not (held := until()):
        if time.monotonic() > deadline:
            sys.exit(f"prove: no {what} in {budget} s")
        time.sleep(0.05)
    return held


class Proof:
    """One run: the daemon's root, its processes and the feed."""

    def __init__(self, rift: str, out: Path) -> None:
        """A fresh root under /tmp, its fake output named Desk, nothing started."""
        self.rift, self.out, self.held = rift, out, []
        self.root = Path(tempfile.mkdtemp(prefix="rift-", dir="/tmp"))
        self.fake = self.root / "fake"
        (self.fake / "outputs").mkdir(parents=True)
        (self.fake / "outputs/Desk").write_text("48000 1 0")
        (self.fake / "default").write_text("Desk\n")
        self.sock = str(self.root / "run/rift.sock")
        self.env = {**os.environ, "RIFT_POLL_MS": "100", "RIFT_ROOT": str(self.root)}
        self.env["RIFT_SPEAKER"] = str(self.fake)

    def run(self, *args: str, **kw: object) -> subprocess.Popen:
        """Starts `args`, in a group of its own so a failed proof kills uv's child with uv."""
        self.held.append(subprocess.Popen(args, env=self.env, start_new_session=True, **kw))
        return self.held[-1]

    def events(self, kind: str) -> list[dict]:
        """The feed's events of `kind` so far."""
        text = (self.out / "feed.jsonl").read_text()
        return [line for line in map(json.loads, text.splitlines()) if line["e"]["kind"] == kind]

    def start(self, bins: Path) -> None:
        """The daemon, the mouth, the ears on, the feed, the three engines, until all are in."""
        rift, quiet = self.rift, {"stdout": subprocess.DEVNULL}
        self.run(rift, "serve", **quiet)
        wait(lambda: Path(self.sock).exists(), "daemon")
        self.run(rift, "device", "mouth", "desk", "--speaker", **quiet)
        subprocess.run([rift, "ears", "on"], env=self.env, check=True, capture_output=True)
        self.run(rift, "feed", stdout=(self.out / "feed.jsonl").open("w"))
        uv = ["uv", "run", "--project", str(HERE / "transcribe"), "--locked", "transcribe"]
        words = os.environ.get("PROVE_WORDS", "transcribe")
        model = [
            "--model",
            os.environ[f"{words.upper()}_MODEL"],
            *(["--terms", TERMS] * bool(TERMS)),
        ]
        engine = {"scribe": [str(bins / "scribe")], "transcribe": uv}[words]
        for name, args in (
            ("endpoint", [str(bins / "endpoint"), "ear-desk"]),
            (words, [*engine, "ear-desk", *model]),
            ("read-aloud", [str(bins / "read-aloud"), "mouth-desk"]),
        ):
            self.run(*args, stderr=(self.out / f"{name}.err").open("w"))
        want = {"ear-desk": {"turn": "endpoint", "words": words}}
        want["mouth-desk"] = {"speech": "read-aloud"}
        health = lambda: ask(self.sock, {"op": "health"})[0]  # noqa: E731
        ready = lambda: health().get("engines") == want and "mouth-desk" in health()["devices"]  # noqa: E731
        wait(ready, "engines", 120)

    def drive(self) -> tuple[dict, dict, int]:
        """The fixture through the ear, its final words, the say of the reply, the sound taken."""
        ear = [self.rift, "device", "ear", "desk", str(HERE / "fixtures" / FIXTURE)]
        self.run(*ear, *sys.argv[4:5]).wait()
        heard = lambda: [  # noqa: E731
            w for w in self.events("words") if HEARD[-1] in json.dumps(w["e"]["words"]).lower()
        ]
        (words,) = wait(heard, f"final words naming {HEARD[-1]}", 30)
        say = [{"op": "say", "track": "mouth-desk"}, {"op": "text", "text": REPLY}, {"op": "end"}]
        spoken = ask(self.sock, *say, rest=True)[-1]
        wait(lambda: self.events("played"), "played", 30)
        taken = self.fake / "heard/Desk"
        pcm = array("h", taken.read_bytes() if taken.exists() else b"")
        # the engines, then the mouth: each stop signal's one line
        for process in self.held[3:6] + self.held[1:2]:
            process.send_signal(signal.SIGTERM)
            process.wait()
        ask(self.sock, {"op": "shutdown"})
        self.held[0].wait()
        self.held[2].wait()
        return words, spoken, sum(1 for sample in pcm if sample)

    def stop(self) -> None:
        """Kills whatever is left and removes the root."""
        for process in self.held:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
        shutil.rmtree(self.root)


def shares(spans: list[dict], start: int, end: int) -> dict:
    """Each id's µs over [start, end) from the talkers events, nobody's under "", the unknown
    under "?", as a client joins them.
    """
    held: dict = {"?": end - start}
    for span in spans:
        cover = min(end, span["end"]) - max(start, span["start"])
        if cover > 0:
            held["?"] -= cover
            for talker in span["talkers"] or [{"id": ""}]:
                held[talker["id"]] = held.get(talker["id"], 0) + cover
    return held


def talkers(rift: str, bins: Path, out: Path) -> None:
    """The talkers proof and its verdict."""
    proof, model = Proof(rift, out), os.environ["DIARIZE_MODEL"]
    profiles = str(proof.root / "profiles")
    ear = [rift, "device", "ear", "desk", str(HERE / "fixtures/voices.pcm"), *sys.argv[4:5]]
    health = lambda: ask(proof.sock, {"op": "health"})[0].get("engines")  # noqa: E731

    def heard(after: int) -> list[dict]:
        """The turns, once more than `after` are in and the last voice is closed by one."""
        turns, voices = proof.events("turn"), proof.events("voice")
        last = voices[-1]["e"] if voices else {"on": True}
        closed = not last["on"] and turns and turns[-1]["e"]["end"] >= last["start"]
        return turns if closed and len(turns) > after else []

    try:
        proof.run(rift, "serve", stdout=subprocess.DEVNULL)
        wait(lambda: Path(proof.sock).exists(), "daemon")
        subprocess.run([rift, "ears", "on"], env=proof.env, check=True, capture_output=True)
        proof.run(rift, "feed", stdout=(out / "feed.jsonl").open("w"))
        endpoint = proof.run(
            str(bins / "endpoint"), "ear-desk", stderr=(out / "endpoint.err").open("w")
        )
        wait(lambda: health() == {"ear-desk": {"turn": "endpoint"}}, "endpoint", 120)
        proof.run(*ear).wait()
        first = wait(lambda: heard(0), "turns")[0]["e"]
        span = [
            "--track",
            "ear-desk",
            "--from",
            str(first["start"] - 250_000),
            "--to",
            str(first["end"]),
        ]
        enrol = [str(bins / "diarize"), "enrol", "first", "--model", model, "--profiles", profiles]
        enrolled = subprocess.run(
            [*enrol, *span], env=proof.env, check=True, capture_output=True, text=True
        )
        diarize = [str(bins / "diarize"), "ear-desk", "--model", model, "--profiles", profiles]
        engine = proof.run(*diarize, stderr=(out / "diarize.err").open("w"))
        want = {"ear-desk": {"talkers": "diarize", "turn": "endpoint"}}
        wait(lambda: health() == want, "diarize", 120)
        before = len(proof.events("turn"))
        proof.run(*ear).wait()
        turns = wait(lambda: heard(before), "turns of the second pass")[before:]
        reach = lambda: max([span["e"]["end"] for span in proof.events("talkers")] or [0])  # noqa: E731
        wait(lambda: reach() >= turns[-1]["e"]["end"], "talkers spans reaching the last turn", 30)
        for process in (engine, endpoint):
            process.send_signal(signal.SIGTERM)
            process.wait()
        ask(proof.sock, {"op": "shutdown"})
        proof.held[0].wait()
        proof.held[2].wait()
    finally:
        proof.stop()
    spans = proof.events("talkers")
    rows = []
    for turn in turns:
        start, end = turn["e"]["start"], turn["e"]["end"]
        share = shares([span["e"] for span in spans], start, end)
        reaching = next(span for span in spans if span["e"]["end"] >= end)
        scores = [
            talker["score"]
            for span in spans
            if span["e"]["start"] < end and span["e"]["end"] > start
            for talker in span["e"]["talkers"]
            if talker["id"] == "first"
        ]
        rows.append(
            {
                "first_scores": scores,
                "ids_ms": {str(key): value // 1000 for key, value in share.items()},
                "mostly": max(
                    (key for key in share if key not in {"", "?"}), key=share.get, default=None
                ),
                "span_logged_after_turn_event_ms": (at(reaching) - at(turn)) // 1000,
                "span_logged_after_turn_end_ms": (at(reaching) - end) // 1000,
                "turn": (start, end),
                "turn_logged_after_its_end_ms": (at(turn) - end) // 1000,
            }
        )
    stops = {path.stem: path.read_text().strip() for path in sorted(out.glob("*.err"))}
    print(
        json.dumps({"enrolled": enrolled.stderr.strip(), "stops": stops, "turns": rows}, indent=1)
    )
    # the fixture's four phrases, alba's first: hers mostly `first`, scored over 0.5; marius's
    # never `first` over 0.5, which LS-EEND alone may name him
    over = [any((score or 0) > SURE for score in row["first_scores"]) for row in rows]
    sys.exit(
        0 if over == [True, False] * 2 and rows[0]["mostly"] == rows[2]["mostly"] == "first" else 1
    )


def main() -> None:
    """The proof and its verdict."""
    rift, bins, out = (Path(arg).resolve() for arg in sys.argv[1:4])
    if os.environ.get("PROVE_TALKERS"):
        out.mkdir(parents=True, exist_ok=True)
        talkers(str(rift), bins, out)
    out.mkdir(parents=True, exist_ok=True)
    proof = Proof(str(rift), out)
    try:
        proof.start(bins)
        words, spoken, sounded = proof.drive()
    finally:
        proof.stop()
    voices, turns = proof.events("voice"), proof.events("turn")
    (say,), (play,) = proof.events("say"), proof.events("play")
    text = " ".join(word["text"] for word in words["e"]["words"])
    ms = {
        "voice_off_logged_after_speech_end": [
            (at(voice) - voice["e"]["start"]) // 1000 for voice in voices if not voice["e"]["on"]
        ],
        "turn_logged_after_its_end": [(at(turn) - turn["e"]["end"]) // 1000 for turn in turns],
        "final_logged_after_its_turn": (at(words) - at(turns[-1])) // 1000,
        "play_opened_after_say": (at(play) - at(say)) // 1000,
    }
    stops = {path.stem: path.read_text().strip() for path in sorted(out.glob("*.err"))}
    verdict = {
        "holes": len(proof.events("hole")),
        "ms": ms,
        "sounded_samples": sounded,
        "spoken": spoken,
        "stops": stops,
        "turns": [(turn["e"]["start"], turn["e"]["end"]) for turn in turns],
        "voices": [(voice["e"]["on"], voice["e"]["start"]) for voice in voices],
        "words": text,
    }
    print(json.dumps(verdict, indent=1))
    held = (
        all(word in text.lower() for word in HEARD)
        and len(turns) == 1
        and spoken["spoken"] == len(REPLY)
        and spoken["reason"] == "end"
        and sounded > 0
        and [voice["e"]["on"] for voice in voices][:1] == [True]
    )
    sys.exit(0 if held else 1)


if __name__ == "__main__":
    main()

"""The reference engines end to end: `prove.py <rift> <engines bin dir> <out dir>`.

A throwaway daemon with its own root, a fake output under $RIFT_SPEAKER (no sound reaches a
speaker), the three engines connected, the fixture fed by a file-fed ear: voice, a turn, a final
words event, then a say of a reply, its spoken count and the sound the fake output took. The
feed's lines land in <out>/feed.jsonl, the verdict on stdout. $TRANSCRIBE_MODEL names Moonshine's
directory; the other models are read from their defaults (README.md).
"""

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

REPLY = "Sure, let's go for a walk. I'll grab my coat."
HERE = Path(__file__).parent


def lines(path: Path) -> list[dict]:
    """The feed's lines so far."""
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def wait(until: object, what: str, budget: float = 60) -> object:
    """Polls `until` every 50 ms until it is truthy; fails naming `what` past `budget` s."""
    deadline = time.monotonic() + budget
    while not (held := until()):
        if time.monotonic() > deadline:
            sys.exit(f"prove: no {what} in {budget} s")
        time.sleep(0.05)
    return held


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


def main() -> None:
    """The proof."""
    rift, bins, out = (str(Path(arg).resolve()) for arg in sys.argv[1:4])
    bins, out = Path(bins), Path(out)
    out.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="rift-", dir="/tmp"))
    fake = root / "fake"
    (fake / "outputs").mkdir(parents=True)
    (fake / "outputs/Desk").write_text("48000 1 0")
    (fake / "default").write_text("Desk\n")
    env = {**os.environ, "RIFT_POLL_MS": "100", "RIFT_ROOT": str(root), "RIFT_SPEAKER": str(fake)}
    sock = str(root / "run/rift.sock")
    held = []
    # each in a group of its own, so a failed proof kills uv's child with uv
    spawn = lambda args, kw: subprocess.Popen(args, env=env, start_new_session=True, **kw)  # noqa: E731
    run = lambda *args, **kw: held.append(spawn(args, kw)) or held[-1]  # noqa: E731
    errs = {name: out / f"{name}.err" for name in ("endpoint", "transcribe", "read-aloud")}
    feed_path = out / "feed.jsonl"
    events = lambda kind: [line for line in lines(feed_path) if line["e"]["kind"] == kind]  # noqa: E731
    try:
        run(rift, "serve", stdout=subprocess.DEVNULL)
        wait(lambda: Path(sock).exists(), "daemon")
        run(rift, "device", "mouth", "desk", "--speaker", stdout=subprocess.DEVNULL)
        subprocess.run([rift, "ears", "on"], env=env, check=True, capture_output=True)
        run(rift, "feed", stdout=feed_path.open("w"))
        transcribe = ["uv", "run", "--project", str(HERE / "transcribe"), "--locked", "transcribe"]
        model = ["--model", os.environ["TRANSCRIBE_MODEL"]]
        run(str(bins / "endpoint"), "ear-desk", stderr=errs["endpoint"].open("w"))
        run(*transcribe, "ear-desk", *model, stderr=errs["transcribe"].open("w"))
        run(str(bins / "read-aloud"), "mouth-desk", stderr=errs["read-aloud"].open("w"))
        want = {"ear-desk": {"turn": "endpoint", "words": "transcribe"}}
        want["mouth-desk"] = {"speech": "read-aloud"}
        health = lambda: ask(sock, {"op": "health"})[0]  # noqa: E731
        wait(
            lambda: health().get("engines") == want and "mouth-desk" in health()["devices"],
            "engines",
            120,
        )
        ear = run(
            rift,
            "device",
            "ear",
            "desk",
            str(HERE / "fixtures/phrase.pcm"),
            stdout=subprocess.DEVNULL,
        )
        ear.wait()
        heard = lambda: [w for w in events("words") if "walk" in json.dumps(w["e"]["words"])]  # noqa: E731
        (words,) = wait(heard, "final words naming the walk", 30)
        reply = ask(
            sock,
            {"op": "say", "track": "mouth-desk"},
            {"op": "text", "text": REPLY},
            {"op": "end"},
            rest=True,
        )
        spoken = reply[-1]
        wait(lambda: events("played"), "played", 30)
        taken = fake / "heard/Desk"
        sounded = sum(
            1 for sample in array("h", taken.read_bytes() if taken.exists() else b"") if sample
        )
        # the engines, then the mouth: each stop signal's one line
        for engine in held[3:6] + held[1:2]:
            engine.send_signal(signal.SIGTERM)
            engine.wait()
        ask(sock, {"op": "shutdown"})
        held[0].wait()
        held[2].wait()
    finally:
        for process in held:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
        shutil.rmtree(root)
    voices, turns = events("voice"), events("turn")
    (say,), (play,) = events("say"), events("play")
    text = " ".join(word["text"] for word in words["e"]["words"])
    verdict = {
        "holes": len(events("hole")),
        "voices": [(voice["e"]["on"], voice["e"]["start"]) for voice in voices],
        "turns": [(turn["e"]["start"], turn["e"]["end"]) for turn in turns],
        "words": text,
        "spoken": spoken,
        "sounded_samples": sounded,
        "ms": {
            "voice_off_logged_after_speech_end": [
                (at(voice) - voice["e"]["start"]) // 1000
                for voice in voices
                if not voice["e"]["on"]
            ],
            "turn_logged_after_its_end": [(at(turn) - turn["e"]["end"]) // 1000 for turn in turns],
            "final_logged_after_its_turn": (at(words) - at(turns[-1])) // 1000,
            "play_opened_after_say": (at(play) - at(say)) // 1000,
        },
        "stops": {name: path.read_text().strip() for name, path in errs.items()},
    }
    print(json.dumps(verdict, indent=1))
    ok = (
        "thinking" in text.lower()
        and spoken["spoken"] == len(REPLY)
        and spoken["reason"] == "end"
        and sounded > 0
        and [voice["e"]["on"] for voice in voices][:1] == [True]
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

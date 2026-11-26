"""The model over the fixture: its phrase among the final words. Runs only where the model is."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

from transcribe import Words, terms

FIXTURE = Path(__file__).parents[2] / "fixtures/phrase.pcm"
MODEL = os.environ.get("TRANSCRIBE_MODEL", "")


@pytest.mark.skipif(
    not Path(MODEL, "encoder.ort").is_file(),
    reason="skipped: set TRANSCRIBE_MODEL to Moonshine v2 Medium Streaming's directory",
)
def test_fixture() -> None:
    """Pocket TTS saying "So I was thinking that we..." then "could go for a walk.", fed as
    20 ms frames, finalized at the turn's end: those words, in order, among the final's.
    """
    from transcribe import moonshine  # noqa: PLC0415 -- the runtime, only where the model is

    pcm = np.fromfile(FIXTURE, "<i2").astype(np.float32) / 32768
    words = Words(moonshine.Moonshine(MODEL), terms(None), None)
    for at in range(len(pcm) // 320):
        words.frame(at * 20_000, pcm[at * 320 : at * 320 + 320])
    (final,) = words.turn(4_768_000)
    said = " ".join(word["text"] for word in final["words"]).lower()
    print(said)
    assert "thinking" in said
    assert "walk" in said
    assert said.index("thinking") < said.index("walk")

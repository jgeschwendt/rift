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
    """Pocket TTS saying "Please turn on the kitchen lights.", fed as 20 ms frames, finalized at
    the turn's end (2.496 s, the endpoint's on the same fixture): those words, in order.
    """
    from transcribe import moonshine  # noqa: PLC0415 -- the runtime, only where the model is

    pcm = np.fromfile(FIXTURE, "<i2").astype(np.float32) / 32768
    words = Words(moonshine.Moonshine(MODEL), terms(None), None)
    for at in range(len(pcm) // 320):
        words.frame(at * 20_000, pcm[at * 320 : at * 320 + 320])
    (final,) = words.turn(2_496_000)
    said = " ".join(word["text"] for word in final["words"]).lower()
    assert "turn on the kitchen lights" in said, said

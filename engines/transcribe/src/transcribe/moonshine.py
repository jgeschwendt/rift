"""Moonshine v2 Medium Streaming behind the engine's model interface, on moonshine-voice.

The runtime's own update pacing is off: the engine asks for a pass itself.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moonshine_voice import ModelArch, Transcriber, get_model_for_language

if TYPE_CHECKING:
    import numpy as np

RATE = 16_000
NEVER = 1e9  # the runtime's own update interval, in seconds: off


def spans(transcript: object) -> list[tuple[str, float, float]]:
    """Each line of `transcript` with text: the text, its start and end in seconds."""
    lines = getattr(transcript, "lines", None) or []
    return [
        (line.text.strip(), line.start_time, line.start_time + line.duration)
        for line in lines
        if line.text.strip()
    ]


class Stream:
    """One utterance's stream."""

    def __init__(self, transcriber: Transcriber) -> None:
        """Opens a stream on `transcriber`."""
        self.stream = transcriber.create_stream(NEVER)
        self.stream.start()

    def add(self, pcm: np.ndarray) -> None:
        """Feeds 16 kHz samples."""
        self.stream.add_audio(pcm.tolist(), RATE)

    def lines(self) -> list[tuple[str, float, float]]:
        """The hypothesis so far."""
        return spans(self.stream.update_transcription())

    def stop(self) -> list[tuple[str, float, float]]:
        """The final lines; the stream closed."""
        final = spans(self.stream.stop() or self.stream.update_transcription())
        self.stream.close()
        return final


class Moonshine:
    """The model, loaded once and shared by every stream."""

    def __init__(self, path: str | None) -> None:
        """Loads the model in `path`, else moonshine-voice's own, fetched into its cache."""
        arch = ModelArch.MEDIUM_STREAMING
        path = path or get_model_for_language("en", arch)[0]
        self.transcriber = Transcriber(path, arch)

    def stream(self) -> Stream:
        """A fresh stream."""
        return Stream(self.transcriber)

# rift's reference engines

Today's models on rift's socket, one engine per kind and track: `endpoint`, `read-aloud`, two
words engines to choose from, `transcribe` and `scribe` (macOS only), and `diarize` for talkers
(macOS only).

| engine | model | licence | weights, fetched by `install-engines` at their pin |
| --- | --- | --- | --- |
| diarize | LS-EEND, DIHARD III, 100 ms steps, Core ML | MIT | `FluidInference/ls-eend-coreml` at `28ce1b1f8ef186729df63b3886fbaae7bc10c4a1`, `optimized/dih3/100ms/` alone, under the install's `models/diarize/ls-eend/` |
| diarize | WeSpeaker ResNet34-LM (`wespeaker_v2`, via pyannote), and pyannote segmentation 3.0, which FluidAudio's embedder loads beside it, Core ML | CC-BY-4.0, attribution required (WeSpeaker, pyannote, Fluid Inference; modified Core ML conversions); the segmentation MIT | `FluidInference/speaker-diarization-coreml` at `7eab74f3463c736418d0eb10296261ae581146c9`, those two `.mlmodelc` alone, under `models/diarize/speaker-diarization/` |
| endpoint | Silero VAD v6.2 | MIT | Hugging Face `Tinysoft/silero-vad-6.2` at `3034960e5d3beb943b860ec0ea1a857bdae4ae96` |
| endpoint | Smart Turn v3.2, CPU | BSD-2-Clause | `pipecat-ai/smart-turn-v3` at `f766f81d3cfdf7737ac64aad813d91bbfd56bf93` |
| read-aloud | Pocket TTS, without voice cloning | CC-BY-4.0, Kyutai | `kyutai/pocket-tts-without-voice-cloning` at `00eac05ed3d16bdc3f6b5d598874019c34a89214` |
| scribe | Parakeet Ultra (moondream's post-training of NVIDIA Parakeet TDT 0.6B v3), Core ML | CC-BY-4.0, attribution required | `FluidInference/parakeet-ultra-coreml` at `95eaa59a39d4394f047a4dc5cce480388a60d1b6`, under the install's `models/parakeet/` |
| scribe | Parakeet CTC 110M (from NVIDIA `parakeet-tdt_ctc-110m`), Core ML, the vocabulary rescorer | CC-BY-4.0, attribution required | `FluidInference/parakeet-ctc-110m-coreml` at `accdafd8cf8a2ff1cabe3c11e54416b405d409aa`, beside it |
| transcribe | Moonshine v2 Medium Streaming, English | MIT | moonshine-voice 0.1.5's own manifest (sizes, CRC32C), under the install's `models/` |

Libraries: onnxruntime 1.28.0 via `ort` (MIT; `ort-sys` fetches it at build time from its pinned
list, SHA-256 checked, linked statically), `ptts`, `xn`, `rustfft` (MIT/Apache-2.0), `tokenizers`
(Apache-2.0), `moonshine-voice` (MIT), numpy (BSD-3-Clause), FluidAudio 0.18.4 (Apache-2.0,
`e0c6103`, `scribe/Package.resolved` and `diarize/Package.resolved`, scribe built with no traits:
ASR alone; diarize with `Diarizer` alone). No engine fetches anything as it runs: `transcribe`,
`scribe` and `diarize` refuse to start without `--model <dir>`.

Installed as rift's engine jobs: `mise run install-engines` builds and installs them into
`~/.local/libexec/rift/` (Moonshine and Parakeet under its `models/`), fetches the models (rerun
to upgrade) and prints this machine's lines, which you run; `scribe`'s only on macOS with Swift,
in place of `transcribe`'s. Outside that directory it writes the Hugging Face cache
(`~/.cache/huggingface/hub`, the models `endpoint` and `read-aloud` read there), uv's cache
(`~/.cache/uv`) and uv's managed Python (`~/.local/share/uv/python`), which the `transcribe` venv
links to; removing any of them breaks the engine that needs it until the task is rerun. Core ML
keeps `scribe`'s compiled model under `~/Library/Caches/scribe/`.

```sh
rift engine add turn -- ~/.local/libexec/rift/endpoint ear-<host>
rift engine add words -- ~/.local/libexec/rift/transcribe/bin/transcribe ear-<host> --model <dir>
rift engine add speech -- ~/.local/libexec/rift/read-aloud mouth-<host>
rift engine add words -- ~/.local/libexec/rift/scribe ear-<host> --model ~/.local/libexec/rift/models/parakeet  # in place of transcribe
rift engine add talkers -- ~/.local/libexec/rift/diarize ear-<host> --model ~/.local/libexec/rift/models/diarize --profiles ~/.config/diarize
```

`diarize` enrols a voice by hand, then names it by its label: `diarize enrol <label> --model <dir>
--profiles <dir> --file <pcm>` (16 kHz 16-bit mono little-endian) or `--track <track> --from <t>
--to <t>` (the daemon's `read` of that span), writing `<dir>/<label>.json` 0600 in a 0700
directory: WeSpeaker's embedding of the audio and the audio itself (its first 30 s), since
LS-EEND is enrolled with audio, not an embedding, each time the engine connects. `diarize forget
<label> --profiles <dir>` removes it; the engine reads the directory at each connect, so either
takes effect on the job's restart. A label is a track name's form starting with a letter. No
profile is ever committed.

`endpoint` commits a turn when Smart Turn scores its close over `--bar` (0.5), else holds it for
the next voice up to `--ceiling-ms` (3000) of silence. Smart Turn scores a short opening word and
its pause as a finished turn (0.81–0.99 at those closes, observed 2026-11-26), so a turn over the
bar spanning under `--short-ms` (1500) of voice is held `--grace-ms` (512) of silence first, and
a voice reported inside it joins the turn; either flag at 0 turns that off. A voice that starts
inside the grace but spans the 96 ms that report it only after the grace is due does not join.
Replayed offline through the engine's segmenter on recordings of one speaker, 75 sentences: turns
cut mid-sentence 23 → 6, the close's median delay after speech ends 310 ms and p90 350 ms
unchanged, 3 sentence ends held into the next either way; a 512 ms grace on every turn cuts 2 but
closes every turn at 830 ms, and a bar of 0.9 still cuts 17 (observed 2026-11-26). A turn under
1.5 s of voice closes 512 ms later: `prove-engines` logs the fixture's turn 279 ms after its
speech ends (1.66 s of voice), 770 ms with `--short-ms 2000` (observed 2026-11-26).

`read-aloud` speaks a say's text in spans of whole sentences as it comes, a say's queued texts
and its end joined first. A span ends at the last sentence end (`.`, `!` or `?` then a space, so
an abbreviation's period ends one too) within 390 bytes, Pocket's 120-token piece at 3.25 bytes
a token, the fewest of three texts (3.26 to 3.72); a longer sentence is one span. Each span is
one model call and one mark, and the model starts afresh per piece (a sentence past 120 tokens
alone, its frames cut to what the 1024-step state leaves past the voice's 125, one stderr line
saying so, and `spoken` then counting characters that did not sound). Inside a piece a sentence
join is the voice's own pause, 240 to 480 ms; across pieces there is no gap (0 to 160 ms of
quiet) but the intonation restarts. Moonshine heard two paragraphs as written at up to 118
tokens a piece, repeated words at 167 and dropped a clause at 203; a 60-word sentence with 11
commas, 97 tokens, ended itself at 208 of its 430-frame cap (measured 2026-11-27 · alba, seed as
in `pocket.rs`, frames of 80 ms under 0.01 RMS counted as quiet).

`transcribe` transcribes only where a turn engine on its track says there is speech: with no
`voice` lines it transcribes nothing and holds no more than its 30 s buffer. It takes
`--terms <file>` (lines `Term` or `Term: heard, heard`), a file of your own, never committed.
`scribe` reads the same file as FluidAudio's custom vocabulary (each term, its heard forms as
aliases) and rescores every decoded turn against it with the CTC model; without `--terms` it
loads no CTC model and rescores nothing.

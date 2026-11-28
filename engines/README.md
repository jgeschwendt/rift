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
LS-EEND is enrolled with audio, not an embedding, each time the engine connects or starts its
stream afresh. `diarize forget
<label> --profiles <dir>` removes it; the engine reads the directory at each connect, so either
takes effect on the job's restart, and loads nothing from a directory not this user's or
writable by others, saying so once. A label is a track name's form starting with a letter. No
profile is ever committed.

After 30 s in which LS-EEND hears nobody the engine starts its stream afresh: LS-EEND reset and
each profile enrolled again (0.28 to 0.31 s for a 30 s profile), the audio not yet given a step
fed again, so no step is lost. A stream left running drifts on a quiet room, its running mean
(LS-EEND's cumulative mean normalisation never forgets) and its state coming to be the room's:
on the fixture, alba enrolled, after 40 s to 10 min of noise at about −55 dBFS marius's first
phrase opened 0.3 s early and his second took alba's label, and after 30 min 0.7 s of his first
went unheard; restarting, the spans after 1 to 30 min of it are a fresh stream's (measured
2026-11-28 · the fixture after leads of uniform and Gaussian noise, the model test `rested`;
validated privately on real speech). A voice not enrolled heard again after a restart takes a
new integer.

A label is LS-EEND's guess, never proof of who speaks: with alba enrolled it labelled files of four
other voices as her, 1.8 to 13.6 s of each, and the fixture's first marius phrase too. A span's
score is WeSpeaker's cosine between its audio (at most 10 s) and the profile, mapped linearly
from 0.06 (0) to 1 (1); null means not identified, and is every span under 2 s, where other
voices' windows outscored the enrolled one's. The score is evidence, not authentication: the
protocol promises nothing of it beyond its order within one engine and release.

Measured on synthetic voices only, Pocket TTS's eight presets, against alba's profile; no rate of
false accepts has been measured on people (measured 2026-11-27 · the fixture and the sentences
in AGENTS.md). Windows slide 0.25 s. The profile from 2.8 s (the fixture's first phrase) or 10 s
of her, through each phrase of alba's other sentences (own) and of the seven other voices:

| window | own, 2.8 s profile, lowest | own, 10 s profile, lowest | others, 2.8 s profile, highest | others, 10 s profile, highest |
| --- | --- | --- | --- | --- |
| 0.5 s | 0.138 | 0.242 | 0.327 | 0.408 |
| 1 s | 0.323 | 0.493 | 0.417 | 0.539 |
| 1.5 s | 0.428 | 0.548 | 0.389 | 0.502 |
| 2 s | 0.481 | 0.628 | 0.394 | 0.466 |
| whole phrase | 0.560 | 0.698 | 0.359 | 0.508 |

The profile enrolled by `diarize enrol` from her whole 14.5 s file, through every window over
0.02 RMS of each file; own is alba's sentences not enrolled:

| window | own, lowest | marius, highest | the six others, highest |
| --- | --- | --- | --- |
| 2 s | 0.180 | 0.649 | 0.419 |
| 3 s | 0.596 | 0.647 | 0.429 |
| 4 s | 0.607 | 0.637 | 0.412 |
| 6 s | 0.732 | 0.622 | 0.412 |
| 10 s | 0.830 | 0.642 | 0.402 |

Separation depends on the profile and the voice. Under the longer profile marius's windows score
up to 0.649 (a score of 0.627), above the own voice's up to 4 s. A voice close to the enrolled
one, or a recording of it, can score as high as the voice itself. So a client acts only on a
label with a non-null score, and when it needs a decision, chooses and validates its own cut on
its own voices; no value of the score is a cut this engine vouches for.

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

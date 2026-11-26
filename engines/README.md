# rift's reference engines

Today's models on rift's socket, one engine per kind: `endpoint`, `transcribe`, `read-aloud`.

| engine | model | licence | weights, fetched by `install-engines` at their pin |
| --- | --- | --- | --- |
| endpoint | Silero VAD v6.2 | MIT | Hugging Face `Tinysoft/silero-vad-6.2` at `3034960e5d3beb943b860ec0ea1a857bdae4ae96` |
| endpoint | Smart Turn v3.2, CPU | BSD-2-Clause | `pipecat-ai/smart-turn-v3` at `f766f81d3cfdf7737ac64aad813d91bbfd56bf93` |
| read-aloud | Pocket TTS, without voice cloning | CC-BY-4.0, Kyutai | `kyutai/pocket-tts-without-voice-cloning` at `00eac05ed3d16bdc3f6b5d598874019c34a89214` |
| transcribe | Moonshine v2 Medium Streaming, English | MIT | moonshine-voice 0.1.5's own manifest (sizes, CRC32C), under the install's `models/` |

Libraries: onnxruntime 1.28.0 via `ort` (MIT; `ort-sys` fetches it at build time from its pinned
list, SHA-256 checked, linked statically), `ptts`, `xn`, `rustfft` (MIT/Apache-2.0), `tokenizers`
(Apache-2.0), `moonshine-voice` (MIT), numpy (BSD-3-Clause). No engine fetches anything as it
runs: `transcribe` refuses to start without `--model <dir>`.

Installed as rift's engine jobs: `mise run install-engines` builds and installs all three into
`~/.local/libexec/rift/` (Moonshine under its `models/`), fetches the models (rerun to upgrade)
and prints this machine's three lines, which you run. Outside that directory it writes the Hugging
Face cache (`~/.cache/huggingface/hub`, the other three models, which `endpoint` and `read-aloud`
read there), uv's cache (`~/.cache/uv`) and uv's managed Python (`~/.local/share/uv/python`),
which the `transcribe` venv links to; removing any of them breaks the engine that needs it until
the task is rerun.

```sh
rift engine add turn -- ~/.local/libexec/rift/endpoint ear-<host>
rift engine add words -- ~/.local/libexec/rift/transcribe/bin/transcribe ear-<host> --model <dir>
rift engine add speech -- ~/.local/libexec/rift/read-aloud mouth-<host>
```

`transcribe` transcribes only where a turn engine on its track says there is speech: with no
`voice` lines it transcribes nothing and holds no more than its 30 s buffer. It takes
`--terms <file>` (lines `Term` or `Term: heard, heard`), a file of your own, never committed.

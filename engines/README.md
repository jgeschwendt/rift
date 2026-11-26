# rift's reference engines

Today's models on rift's socket, one engine per kind: `endpoint`, `transcribe`, `read-aloud`.

| engine | model | licence | weights |
| --- | --- | --- | --- |
| endpoint | Silero VAD v6.2 | MIT | `uvx --from huggingface_hub==2.4.0 hf download Tinysoft/silero-vad-6.2 silero_vad.onnx --revision 3034960e5d3beb943b860ec0ea1a857bdae4ae96` |
| endpoint | Smart Turn v3.2, CPU | BSD-2-Clause | `uvx --from huggingface_hub==2.4.0 hf download pipecat-ai/smart-turn-v3 smart-turn-v3.2-cpu.onnx --revision f766f81d3cfdf7737ac64aad813d91bbfd56bf93` |
| read-aloud | Pocket TTS, without voice cloning | CC-BY-4.0, Kyutai | `uvx --from huggingface_hub==2.4.0 hf download kyutai/pocket-tts-without-voice-cloning --revision 00eac05ed3d16bdc3f6b5d598874019c34a89214` |
| transcribe | Moonshine v2 Medium Streaming, English | MIT | fetched by moonshine-voice into its cache on first run, or `--model <dir>` |

Libraries: onnxruntime via `ort`, `ptts`, `xn`, `rustfft` (MIT/Apache-2.0), `tokenizers`
(Apache-2.0), `moonshine-voice` (MIT), numpy (BSD-3-Clause).

Installed as rift's engine jobs, from a checkout's root, each command recorded verbatim, so its
paths are absolute (keep the release build where it is, or install copies and name those):

```sh
cargo build --release --manifest-path engines/Cargo.toml
host=$(scutil --get LocalHostName | tr '[:upper:]' '[:lower:]')
rift engine add turn -- "$PWD/engines/target/release/endpoint" "ear-$host"
rift engine add words -- "$(command -v uv)" run --project "$PWD/engines/transcribe" --locked transcribe "ear-$host"
rift engine add speech -- "$PWD/engines/target/release/read-aloud" "mouth-$host"
```

`transcribe` takes `--terms <file>` (lines `Term` or `Term: heard, heard`), a file of your own,
never committed.

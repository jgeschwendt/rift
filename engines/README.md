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

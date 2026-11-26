#!/bin/sh
# The reference engines installed beside rift's fixed copies, out of any checkout's target/ or
# working tree: endpoint and read-aloud built in release and renamed into place, transcribe
# synced from its lock into a venv of its own, not editable, and every model fetched here and
# now at its pin, so no engine fetches anything as it runs. Rerun to upgrade. Prints the
# `rift engine add` lines for this machine and runs none. $RIFT_ENGINES_PREFIX overrides
# ~/.local/libexec/rift (the suite's temp directory); $TRANSCRIBE_MODEL names a Moonshine
# directory already fetched, in place of fetching one under the prefix.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
prefix=${RIFT_ENGINES_PREFIX:-$HOME/.local/libexec/rift}
mkdir -p "$prefix"
cargo build --release --locked --manifest-path "$here/Cargo.toml" -p endpoint -p read-aloud
for bin in endpoint read-aloud; do
  # temp + rename, so a running engine keeps the file it mapped
  install -m 755 "$here/target/release/$bin" "$prefix/$bin.new"
  mv -f "$prefix/$bin.new" "$prefix/$bin"
done
UV_PROJECT_ENVIRONMENT="$prefix/transcribe" uv sync --project "$here/transcribe" --locked --no-dev --no-editable --quiet
# the Hugging Face models at their pinned revisions, into the hub cache the engines read
hf() { uv tool run --quiet --from huggingface_hub==2.4.0 hf download "$@" --quiet >/dev/null; }
hf Tinysoft/silero-vad-6.2 silero_vad.onnx --revision 3034960e5d3beb943b860ec0ea1a857bdae4ae96
hf pipecat-ai/smart-turn-v3 smart-turn-v3.2-cpu.onnx --revision f766f81d3cfdf7737ac64aad813d91bbfd56bf93
hf kyutai/pocket-tts-without-voice-cloning tts_b6369a24.safetensors tokenizer.json embeddings/alba.safetensors --revision 00eac05ed3d16bdc3f6b5d598874019c34a89214
# Moonshine as the locked moonshine-voice pins it (its manifest, each file's size and CRC32C)
fetch='import sys; from pathlib import Path; from moonshine_voice import ModelArch, get_model_for_language as get
print(get("en", ModelArch.MEDIUM_STREAMING, cache_root=Path(sys.argv[1]))[0])'
model=${TRANSCRIBE_MODEL:-$("$prefix/transcribe/bin/python" -c "$fetch" "$prefix/models" 2>/dev/null)}
host=$(scutil --get LocalHostName 2>/dev/null | tr '[:upper:]' '[:lower:]' || true)
host=${host:-<name>}
echo "rift engine add turn -- $prefix/endpoint ear-$host"
echo "rift engine add words -- $prefix/transcribe/bin/transcribe ear-$host --model $model"
echo "rift engine add speech -- $prefix/read-aloud mouth-$host"

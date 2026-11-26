#!/bin/sh
# The reference engines installed beside rift's fixed copies, out of any checkout's target/ or
# working tree: endpoint and read-aloud built in release and renamed into place, transcribe
# synced from its lock into a venv of its own, not editable. Rerun to upgrade. Prints the
# `rift engine add` lines for this machine and runs none. $RIFT_ENGINES_PREFIX overrides
# ~/.local/libexec/rift (the suite's temp directory).
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
host=$(scutil --get LocalHostName 2>/dev/null | tr '[:upper:]' '[:lower:]' || true)
host=${host:-<name>}
echo "rift engine add turn -- $prefix/endpoint ear-$host"
echo "rift engine add words -- $prefix/transcribe/bin/transcribe ear-$host"
echo "rift engine add speech -- $prefix/read-aloud mouth-$host"

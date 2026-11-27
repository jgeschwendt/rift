#!/bin/sh
# Exit 0 where scribe builds: macOS with Swift 6.2 or later (Package.swift's tools version);
# else 1, saying nothing, for check.sh and install.sh to skip it in one line of their own.
set -u
[ "$(uname 2>/dev/null)" = Darwin ] || exit 1
version=$(swift --version 2>/dev/null | sed -n 's/.*Swift version \([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1 \2/p' | head -n 1)
# shellcheck disable=SC2086
set -- $version
[ "${1:-0}" -gt 6 ] || { [ "${1:-0}" -eq 6 ] && [ "${2:-0}" -ge 2 ]; }

#!/bin/sh
# scribe's gate, run by check-engines; off macOS or without Swift, one line saying it is skipped.
# swift format's lint, then the suite, failing under 100 % of Sources/Engine's lines (the protocol
# logic): Parakeet.swift and main.swift hold the model and the process alone, held instead by the
# model test, which runs where $SCRIBE_MODEL names the weights and says skipped otherwise.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
if [ "$(uname)" != Darwin ] || ! command -v swift >/dev/null 2>&1; then
  echo "scribe: skipped: macOS and Swift only"
  exit 0
fi
swift format lint --strict --recursive "$here/Package.swift" "$here/Sources" "$here/Tests"
swift test --disable-keychain --enable-code-coverage --package-path "$here"
bin=$(swift build --disable-keychain --package-path "$here" --show-bin-path)
cov="-instr-profile $bin/codecov/default.profdata $bin/scribePackageTests.xctest/Contents/MacOS/scribePackageTests $here/Sources/Engine"
# word splitting of $cov is meant: the profile, the binary and the sources
# shellcheck disable=SC2086
xcrun llvm-cov report $cov | tail -1
# shellcheck disable=SC2086
if ! xcrun llvm-cov report $cov | awk '/^TOTAL/ { exit $10 != "100.00%" }'; then
  # shellcheck disable=SC2086
  xcrun llvm-cov show $cov | grep -E '^/|^ *[0-9]+[|] *0[|]'
  exit 1
fi

#!/bin/sh
# diarize's gate, run by check-engines; where supported.sh refuses, one line saying it is skipped.
# swift format's lint, then the suite at Package.resolved's pin, failing under 100 % of
# Sources/Engine's lines (the protocol logic): Fluid.swift and main.swift hold the models and
# the process alone, held instead by the model test, which runs where $DIARIZE_MODEL names the
# weights and says skipped otherwise.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
if ! "$here/supported.sh"; then
  echo "diarize: skipped: macOS with Swift 6.2 or later only"
  exit 0
fi
pin="--disable-keychain --force-resolved-versions --package-path $here"
swift format lint --strict --recursive "$here/Package.swift" "$here/Sources" "$here/Tests"
# shellcheck disable=SC2086
swift test $pin --enable-code-coverage
# shellcheck disable=SC2086
bin=$(swift build $pin --show-bin-path)
cov="-instr-profile $bin/codecov/default.profdata $bin/diarizePackageTests.xctest/Contents/MacOS/diarizePackageTests $here/Sources/Engine"
# word splitting of $cov is meant: the profile, the binary and the sources
# shellcheck disable=SC2086
xcrun llvm-cov report $cov | tail -1
# shellcheck disable=SC2086
if ! xcrun llvm-cov report $cov | awk '/^TOTAL/ { exit $10 != "100.00%" }'; then
  # shellcheck disable=SC2086
  xcrun llvm-cov show $cov | grep -E '^/|^ *[0-9]+[|] *0[|]'
  exit 1
fi

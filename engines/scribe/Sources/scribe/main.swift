import Dispatch
import Engine
import Foundation
import Parakeet

// a closed stderr or socket is an error a write returns, never a signal that ends the engine
signal(SIGPIPE, SIG_IGN)

/// Each stop signal's source, held for the process's life: one stderr line, then exit 0.
let stops = [(SIGHUP, "SIGHUP"), (SIGINT, "SIGINT"), (SIGTERM, "SIGTERM")].map { number, name in
  signal(number, SIG_IGN)
  // the main queue: this file's closures are the main actor's, checked where they run (a global
  // queue's trapped, observed 2026-11-26); it serves them only because `run` and `session`
  // are nonisolated, so their blocking socket reads never hold the main thread (`session`
  // asserts it)
  let source = DispatchSource.makeSignalSource(signal: number, queue: .main)
  source.setEventHandler {
    say("scribe: stopped by \(name)")
    exit(0)
  }
  source.resume()
  return source
}

exit(
  await run(
    Array(CommandLine.arguments.dropFirst()), env: ProcessInfo.processInfo.environment,
    load: { try await Parakeet(model: $0, terms: $1) },
    wait: {
      try await Task.sleep(for: $0)
      return true
    }))

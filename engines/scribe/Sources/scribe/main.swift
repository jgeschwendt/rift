import Dispatch
import Engine
import Foundation
import Parakeet

/// Each stop signal's source, held for the process's life: one stderr line, then exit 0.
let stops = [(SIGHUP, "SIGHUP"), (SIGINT, "SIGINT"), (SIGTERM, "SIGTERM")].map { number, name in
  signal(number, SIG_IGN)
  // the main queue: this file's closures are the main actor's, checked where they run (a global
  // queue's trapped, observed 2026-11-26), and the engine's work runs off it
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

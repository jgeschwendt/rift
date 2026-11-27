import Dispatch
import Engine
import Fluid
import Foundation

// a closed stderr or socket is an error a write returns, never a signal that ends the engine
signal(SIGPIPE, SIG_IGN)

/// Each stop signal's source, held for the process's life: one stderr line, then exit 0.
let stops = [(SIGHUP, "SIGHUP"), (SIGINT, "SIGINT"), (SIGTERM, "SIGTERM")].map { number, name in
  signal(number, SIG_IGN)
  // the main queue, as scribe's: `run` and `session` are nonisolated, so their blocking socket
  // reads never hold the main thread (`session` asserts it)
  let source = DispatchSource.makeSignalSource(signal: number, queue: .main)
  source.setEventHandler {
    say("diarize: stopped by \(name)")
    exit(0)
  }
  source.resume()
  return source
}

exit(
  await run(
    Array(CommandLine.arguments.dropFirst()), env: ProcessInfo.processInfo.environment,
    load: { try await Fluid(model: $0) },
    wait: {
      try await Task.sleep(for: $0)
      return true
    }))

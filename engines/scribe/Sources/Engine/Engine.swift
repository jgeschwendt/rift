import Foundation

/// The hello's client.
let client = "scribe/0.1.0"
/// A connection's end, as said.
let ended = "the connection ended"
/// The engine's name on the wire.
let engineName = "scribe"
/// The usage line.
let usage = "usage: scribe <track> --model <dir> [--terms <file>]"

/// A connection's failure (its end, a refusal, a line that is no JSON object): said, waited out
/// and connected again.
struct Failure: LocalizedError {
  let reason: String
  var errorDescription: String? { reason }
}

/// Writes one stderr line, by `write(2)` so a closed stderr cannot stop the engine.
func written(_ line: String) {
  _ = (line + "\n").withCString { write(2, $0, strlen($0)) }
}

/// What the engine says with: `written`, swapped by the tests to read what is said.
nonisolated(unsafe) public var say: (String) -> Void = written

/// `line` as compact JSON, its keys sorted.
func json(_ line: [String: Any]) -> String {
  // only ever handed what JSONSerialization parsed or the engine built
  String(
    decoding: try! JSONSerialization.data(withJSONObject: line, options: .sortedKeys), as: UTF8.self
  )
}

/// The unix socket address of `path`.
func address(_ path: String) -> sockaddr_un {
  var at = sockaddr_un()
  at.sun_family = sa_family_t(AF_UNIX)
  withUnsafeMutableBytes(of: &at.sun_path) { $0.copyBytes(from: path.utf8.prefix($0.count - 1)) }
  return at
}

/// One connection's socket, its lines read and written; closed as it is released.
final class Conn {
  let fd: Int32
  /// Bytes read past the last line.
  var pending: [UInt8] = []
  /// The request's reply.
  var reply: [String: Any] = [:]

  /// The socket `fd`, its writes never raising SIGPIPE.
  init(_ fd: Int32) {
    self.fd = fd
    var on: Int32 = 1
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, socklen_t(MemoryLayout<Int32>.size))
  }

  /// Connects to `path`, says hello and sends `request`, its reply kept as `reply`; a refusal is
  /// a failure naming it.
  convenience init(path: String, request: [String: Any]) throws {
    self.init(socket(AF_UNIX, SOCK_STREAM, 0))
    var at = address(path)
    let connected = withUnsafePointer(to: &at) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
      }
    }
    guard connected == 0 else { throw Failure(reason: String(cString: strerror(errno))) }
    for line in [["client": client, "op": "hello", "proto": 1], request] {
      try send(line)
      reply = try self.line() ?? [:]
      guard reply["ok"] as? Bool == true else {
        throw Failure(reason: reply.isEmpty ? ended : json(reply))
      }
    }
  }

  deinit { close(fd) }

  /// The next line, none at the connection's end.
  func line() throws -> [String: Any]? {
    var chunk = [UInt8](repeating: 0, count: 65_536)
    while !pending.contains(10) {
      let count = read(fd, &chunk, chunk.count)
      guard count > 0 else { return nil }
      pending += chunk[..<count]
    }
    let newline = pending.firstIndex(of: 10)!
    defer { pending.removeSubrange(...newline) }
    let text = Data(pending[..<newline])
    guard let line = try? JSONSerialization.jsonObject(with: text) as? [String: Any] else {
      throw Failure(reason: "not a JSON object: \(String(decoding: text, as: UTF8.self))")
    }
    return line
  }

  /// Writes `line` and its newline.
  func send(_ line: [String: Any]) throws {
    let data = Array((json(line) + "\n").utf8)
    guard write(fd, data, data.count) == data.count else { throw Failure(reason: ended) }
  }
}

/// The daemon's socket: run/rift.sock under $RIFT_ROOT, else under ~/.local/state/rift.
func socketPath(_ env: [String: String]) -> String {
  let root = env["RIFT_ROOT", default: ""]
  return (root.isEmpty ? env["HOME", default: ""] + "/.local/state/rift" : root) + "/run/rift.sock"
}

/// One connection as the words engine of `track`: frames from the buffer's span before `now`
/// (this machine's clock is the daemon's), live when that is gone, finals from the reply's
/// `through` on, so a start or a reconnect resumes and never chews through more than the buffer;
/// each voice and turn line acted on, a refused report said, any other line ignored, until the
/// connection ends.
func session(path: String, track: String, model: any Model, now: Int) async throws {
  let request = { (from: Any) -> [String: Any] in
    ["from": from, "kind": "words", "name": engineName, "op": "engine", "track": track]
  }
  let conn: Conn
  do {
    conn = try Conn(path: path, request: request(now - keptFrames * frameSpan))
  } catch let failure as Failure where failure.reason.contains(#""gone""#) {
    conn = try Conn(path: path, request: request(NSNull()))
  }
  let words = Words(model: model, floor: conn.reply["through"] as? Int)
  while let line = try conn.line() {
    // a report's reply: only a refusal is news
    if line["ok"] as? Bool == false { say("scribe: refused: \(json(line))") }
    if let voice = line["voice"] as? [String: Any] {
      words.voiced(voice["on"] as? Bool == true, voice["start"] as? Int ?? 0)
    }
    var reports: [[String: Any]] = []
    if let turn = line["turn"] as? [String: Any] {
      reports = try await words.turn(turn["end"] as? Int ?? 0)
    }
    if let pcm = line["pcm"] as? String {
      reports = try await words.frame(line["t"] as? Int ?? 0, samples(pcm))
    }
    for report in reports { try conn.send(report) }
  }
}

/// Runs `session` as a device stands, each end waited out by `wait` until it says to stop (in
/// the engine, never), its end or failure said once until another; any other error escapes.
func stand(_ session: () async throws -> Void, wait: () async throws -> Bool) async throws {
  var (said, going) = ("", true)
  while going {
    var reason = ended
    do {
      try await session()
    } catch let failure as Failure {
      reason = failure.reason
    }
    if reason != said { say("scribe: \(reason)") }
    (said, going) = (reason, try await wait())
  }
}

/// The value after `--<name>` among `args`.
func flag(_ args: [String], _ name: String) -> String? {
  args.firstIndex(of: "--\(name)").flatMap { $0 + 1 < args.count ? args[$0 + 1] : nil }
}

/// The command line: usage and 2 without a track or a model; else the model loaded by `load` from
/// `--model` (and `--terms`' file when named) before any connection, its readiness said once,
/// then the engine stood on the socket `env` names, `wait` handed $RIFT_POLL_MS (default 1000)
/// between connections, until a stop signal (or `wait` says to stop: 0); anything else that
/// escapes is one stderr line and 1, for launchd to restart it.
public func run(
  _ args: [String], env: [String: String],
  load: (String, String?) async throws -> any Model,
  wait: (Duration) async throws -> Bool
) async -> Int32 {
  guard let track = args.first, !track.hasPrefix("-"), let dir = flag(args, "model") else {
    say(usage)
    return 2
  }
  do {
    let clock = ContinuousClock()
    let started = clock.now
    let model = try await load(dir, flag(args, "terms"))
    say("scribe: ready in \(Int((clock.now - started) / .milliseconds(1))) ms")
    let poll = Duration.milliseconds(Int(env["RIFT_POLL_MS", default: ""]) ?? 1000)
    try await stand(
      {
        let now = Int(Date().timeIntervalSince1970 * 1_000_000)
        try await session(path: socketPath(env), track: track, model: model, now: now)
      }, wait: { try await wait(poll) })
    return 0
  } catch {
    say("scribe: \(error.localizedDescription)")
    return 1
  }
}

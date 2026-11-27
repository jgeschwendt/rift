import Foundation

/// The hello's client.
let client = "diarize/0.1.0"
/// A connection's end, as said.
let ended = "the connection ended"

/// A failure said and, on the engine's connection, waited out and connected again: its end, a
/// refusal, a line that is no JSON object; or a command's failure, said and exit 1.
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
    if reason != said { say("diarize: \(reason)") }
    (said, going) = (reason, try await wait())
  }
}

/// The value after `--<name>` among `args`.
func flag(_ args: [String], _ name: String) -> String? {
  args.firstIndex(of: "--\(name)").flatMap { $0 + 1 < args.count ? args[$0 + 1] : nil }
}

/// A frame's samples, 16-bit little-endian hex, as floats in [-1, 1).
func samples(_ hex: String) -> [Float] {
  // each hex digit's value; the daemon sends lowercase
  let digit = hex.utf8.map { UInt16($0 <= 57 ? $0 &- 48 : $0 &- 87) }
  return stride(from: 0, to: digit.count - 3, by: 4).map {
    let low = digit[$0] << 4 | digit[$0 + 1]
    return Float(Int16(bitPattern: digit[$0 + 2] << 12 | digit[$0 + 3] << 8 | low)) / 32768
  }
}

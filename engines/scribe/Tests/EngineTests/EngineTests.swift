import Foundation
import Testing

@testable import Engine

/// A model that hears each run of one nonzero sample value as a word, `w<level in hundredths>`,
/// timed by where it lies in what it was given; it keeps each span it was given.
final class Stub: Model {
  var given: [[Float]] = []

  func decode(_ pcm: [Float]) async throws -> [Heard] {
    given.append(pcm)
    var (words, low) = ([Heard](), 0)
    for at in stride(from: 1, through: pcm.count, by: 1)
    where at == pcm.count || pcm[at] != pcm[low] {
      let text = "w\(Int((pcm[low] * 100).rounded()))"
      words +=
        pcm[low] == 0
        ? [] : [Heard(text: text, start: Double(low) / 16_000, end: Double(at) / 16_000)]
      low = at
    }
    return words
  }
}

/// One line of what reaches a words engine, as the tests write it.
enum Line {
  /// `count` frames of one `level` from `t` µs, 20 ms apart.
  case frames(Int, Float, Int)
  case off(Int)
  case on(Int)
  case turn(Int)
}

/// Every report `lines` draw from `words`, in order.
func run(_ words: Words, _ lines: [Line]) async throws -> [[String: Any]] {
  var said: [[String: Any]] = []
  for line in lines {
    switch line {
    case .frames(let t, let level, let count):
      for at in 0..<count {
        said += try await words.frame(t + at * 20_000, [Float](repeating: level, count: 320))
      }
    case .off(let start): words.voiced(false, start)
    case .on(let start): words.voiced(true, start)
    case .turn(let end): said += try await words.turn(end)
    }
  }
  return said
}

/// Each final's words, `text start end`.
func finals(_ said: [[String: Any]]) -> [[String]] {
  said.filter { $0["op"] as? String == "words" }.map {
    ($0["words"] as! [[String: Any]]).map { "\($0["text"]!) \($0["start"]!) \($0["end"]!)" }
  }
}

/// Each final's span, `start end`.
func spans(_ said: [[String: Any]]) -> [String] {
  said.map { "\($0["start"]!) \($0["end"]!)" }
}

/// What the engine says on stderr while a test runs.
nonisolated(unsafe) var spoken: [String] = []

/// The engine's writer swapped for one the test reads in `spoken`, emptied; a closure-taking
/// helper here had Swift 6.2.4 hand one test's closure to another call (observed 2026-11-26).
func listen() {
  (spoken, say) = ([], { spoken.append($0) })
}

/// The error a test throws to leave a loop that never ends by itself.
struct Stop: Error {}

/// A daemon the suite plays: a socket listening at a root's run/rift.sock under /tmp, each
/// connection answered its hello, its request kept, then run by the next of `scripts`, on a
/// thread of its own; each script has written all it will by the time the engine sees its end.
final class Daemon: @unchecked Sendable {
  let root = "/tmp/scribe-\(getpid())-\(UUID().uuidString.prefix(8))"
  let fd = socket(AF_UNIX, SOCK_STREAM, 0)
  var requests: [[String: Any]] = []
  var heard: [[String: Any]] = []
  var path: String { root + "/run/rift.sock" }

  init(_ scripts: [@Sendable (Daemon, Conn) throws -> Void]) {
    try! FileManager.default.createDirectory(
      atPath: root + "/run", withIntermediateDirectories: true)
    var at = address(path)
    _ = withUnsafePointer(to: &at) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
      }
    }
    listen(fd, 8)
    Thread {
      for script in scripts {
        let conn = Conn(accept(self.fd, nil, nil))
        _ = try? conn.line()
        try? conn.send(["ok": true, "proto": 1])
        self.requests += [try! conn.line()!]
        try? script(self, conn)
      }
    }.start()
  }

  deinit {
    close(fd)
    try? FileManager.default.removeItem(atPath: root)
  }
}

@Suite(.serialized) struct Scribe {
  @Test("no voice line: nothing decoded, nothing said, no more kept than the buffer")
  func quiet() async throws {
    let model = Stub()
    let words = Words(model: model, floor: nil)
    #expect(try await run(words, [.frames(0, 0.05, 2_000), .turn(30_000_000)]).isEmpty)
    #expect(model.given.isEmpty && words.kept.count == 1_500)
  }

  @Test("a turn: one final at its line, decoded once from the voice's start less the pre-roll")
  func utterance() async throws {
    let model = Stub()
    let said = try await run(
      Words(model: model, floor: 500_000),
      [
        .frames(0, 0, 50), .frames(1_000_000, 0.05, 6), .on(1_000_000),
        .frames(1_120_000, 0.05, 44), .frames(2_000_000, 0, 13), .off(2_000_000),
        .frames(2_260_000, 0, 6), .turn(2_000_000),
      ])
    #expect(spans(said) == ["750000 2000000"] && finals(said) == [["w5 1000000 2000000"]])
    #expect(model.given.map(\.count) == [20_000], "1.25 s: the pre-roll and the voice")
  }

  @Test("a turn held across two voices: one decode, one final, its pause included")
  func held() async throws {
    let model = Stub()
    let said = try await run(
      Words(model: model, floor: nil),
      [
        .on(1_000_000), .frames(1_000_000, 0.05, 25), .off(1_500_000), .frames(1_500_000, 0, 25),
        .on(2_000_000), .frames(2_000_000, 0.06, 25), .off(2_500_000), .frames(2_500_000, 0, 15),
        .turn(2_500_000),
      ])
    #expect(finals(said) == [["w5 1000000 1500000", "w6 2000000 2500000"]])
    #expect(model.given.count == 1)
  }

  @Test("a turn line after the next voice began: no word in both finals, the next from its end")
  func lateTurn() async throws {
    let said = try await run(
      Words(model: Stub(), floor: nil),
      [
        .on(1_000_000), .frames(1_000_000, 0.05, 50), .off(2_000_000), .frames(2_000_000, 0, 25),
        .on(2_500_000), .frames(2_500_000, 0.07, 50), .turn(2_000_000), .off(3_500_000),
        .frames(3_500_000, 0, 15), .turn(3_500_000),
      ])
    #expect(spans(said) == ["750000 2000000", "2000000 3500000"])
    #expect(finals(said) == [["w5 1000000 2000000"], ["w7 2500000 3500000"]])
  }

  @Test("resumed from through: a replayed voice and turn open nothing, a late one keeps the span")
  func resumed() async throws {
    let model = Stub()
    let said = try await run(
      Words(model: model, floor: 2_000_000),
      [
        .on(1_000_000), .frames(1_000_000, 0.05, 25), .off(1_500_000), .frames(1_500_000, 0, 15),
        .turn(1_500_000), .frames(1_800_000, 0, 60), .on(3_000_000), .turn(1_500_000),
        .frames(3_000_000, 0.07, 25), .off(3_500_000), .frames(3_500_000, 0, 15), .turn(3_500_000),
      ])
    #expect(spans(said) == ["2750000 3500000"] && finals(said) == [["w7 3000000 3500000"]])
    #expect(model.given.count == 1, "the fresh voice's alone")
  }

  @Test("words after a hole carry the times their audio was heard")
  func hole() async throws {
    let said = try await run(
      Words(model: Stub(), floor: nil),
      [
        .on(1_000_000), .frames(1_000_000, 0.05, 25), .frames(2_000_000, 0.06, 25),
        .off(2_500_000), .frames(2_500_000, 0, 15), .turn(2_500_000),
      ])
    #expect(finals(said) == [["w5 1000000 1500000", "w6 2000000 2500000"]])
  }

  @Test("a voice that never ends: finished each 20 s by the engine's own rule, said once")
  func cap() async throws {
    listen()
    let said = try await run(
      Words(model: Stub(), floor: nil), [.on(1), .frames(0, 0.05, 2_100), .turn(100)])
    #expect(spans(said) == ["0 20000000", "20000000 40000000"])
    #expect(spoken == ["scribe: a span reached 20 s and was finished there"])
  }

  @Test(
    "a cap 300 ms behind the newest frame; the span it carries, closed by an off or a turn ending before it, never opens the next voice's"
  )
  func capped() async throws {
    // the voice ends at 19.9 s, its off and turn read only after the cap
    let capping: [Line] = [.on(1), .frames(0, 0.05, 1_015)]
    let next: [Line] = [
      .frames(20_300_000, 0, 235), .on(25_000_000), .frames(25_000_000, 0.07, 25),
      .off(25_500_000), .frames(25_500_000, 0, 15), .turn(25_500_000),
    ]
    for late in [[Line.off(19_900_000), .turn(19_900_000)], [.turn(19_900_000)]] {
      let said = try await run(Words(model: Stub(), floor: nil), capping + late + next)
      #expect(spans(said) == ["0 20000000", "24750000 25500000"])
      #expect(finals(said).last == ["w7 25000000 25500000"])
    }
  }

  @Test("a turn with no words: no final, and its audio is never decoded again")
  func silent() async throws {
    let model = Stub()
    let said = try await run(
      Words(model: model, floor: nil),
      [
        .on(1_000_000), .frames(1_000_000, 0, 25), .off(1_500_000), .turn(1_500_000),
        .on(1_400_000),
      ])
    #expect(said.isEmpty && model.given.count == 1)
    // a word heard past the span's end gives none; with no audio heard, times run from its start
    let words = Words(model: model, floor: nil)
    #expect(words.timed([Heard(text: "late", start: 1, end: 2)], [(0, 0)], 0, 500_000).isEmpty)
    #expect(
      spans(words.timed([Heard(text: "a", start: 0, end: 0.25)], [], 7, 500_000)) == ["7 250007"])
  }

  @Test("a rescored text keeps each shared word's times; a replaced run shares the span it took")
  func rescored() {
    let heard = [("ask", 0, 0.25), ("post", 0.5, 0.75), ("grass", 0.75, 1), ("now", 1.5, 2)].map {
      Heard(text: $0, start: $1, end: $2)
    }
    #expect(retime(heard, as: "ask post grass now") == heard)
    #expect(
      retime(heard, as: "ask Postgres now") == [
        heard[0], Heard(text: "Postgres", start: 0.25, end: 1.5), heard[3],
      ])
    #expect(
      retime([heard[0]], as: "a b") == [
        Heard(text: "a", start: 0, end: 0.125), Heard(text: "b", start: 0.125, end: 0.25),
      ])
    #expect(retime([], as: "x") == [Heard(text: "x", start: 0, end: 0)])
  }

  @Test("the wire's pieces: a vocabulary file, hex samples, a sample index, the socket, a flag")
  func pieces() {
    let terms = vocabulary("# a comment\n\nPostgres: post grass, postgress,\nMoonshine\n:\n  \n")
    #expect(terms.map(\.term) == ["Postgres", "Moonshine"])
    #expect(terms.map(\.heard) == [["post grass", "postgress"], []])
    // CRLF, a leading comment and a trailing blank line read as LF does
    let crlf = vocabulary("# a comment\r\nPostgres: post grass\r\nMoonshine\r\n\r\n")
    #expect(crlf.map(\.term) == ["Postgres", "Moonshine"] && crlf[0].heard == ["post grass"])
    #expect(samples("0000ff7f0080") == [0, Float(32_767) / 32_768, -1])
    #expect((index(1), index(62), index(63)) == (1, 1, 2))
    #expect(socketPath(["RIFT_ROOT": "r"]) == "r/run/rift.sock")
    #expect(socketPath(["HOME": "/home"]) == "/home/.local/state/rift/run/rift.sock")
    #expect(socketPath([:]) == "/.local/state/rift/run/rift.sock")
    #expect((flag(["x", "--terms", "t"], "terms"), flag(["--terms"], "terms")) == ("t", nil))
  }

  @Test(
    "a session from 30 s back, its finals from through, unknown lines ignored, a refusal said; gone connects live; another refusal ends it"
  )
  func sessions() async throws {
    let hexed = String(repeating: "cd0c", count: 320)
    let daemon = Daemon([
      { daemon, conn in
        let lines: [[String: Any]] =
          [
            // unknown lines, and known ones missing their fields, change nothing
            ["ok": true, "seq": 1, "through": 0], ["unknown": 1], ["voice": ["on": true]],
            ["turn": [:] as [String: Any]], ["pcm": ""],
            ["voice": ["on": true, "start": 20_000]],
          ]
          + (0..<16).map { ["pcm": hexed, "t": 20_000 + $0 * 20_000] }
          + [
            ["voice": ["on": false, "start": 340_000]],
            ["turn": ["end": 340_000, "start": 20_000]],
          ]
        for line in lines { try conn.send(line) }
        daemon.heard += [try conn.line()!]
        try conn.send(["code": "bad_request", "ok": false])
        shutdown(conn.fd, SHUT_RDWR)
      },
      { _, conn in try conn.send(["code": "gone", "ok": false]) },
      { _, conn in
        try conn.send(["ok": true, "seq": 2, "through": NSNull()])
        _ = write(conn.fd, "nope\n", 5)
      },
      { _, conn in try conn.send(["code": "taken", "ok": false]) },
      // closed before its reply
      { _, _ in },
    ])
    var ends: [String] = []
    listen()
    for _ in 0..<4 {
      do {
        try await session(path: daemon.path, track: "ear-desk", model: Stub(), now: 40_000_000)
        ends.append("returned")
      } catch let failure as Failure {
        ends.append(failure.reason)
      }
    }
    #expect(
      daemon.requests.map { $0["from"] as? Int } == [
        10_000_000, 10_000_000, nil, 10_000_000, 10_000_000,
      ])
    #expect(
      json(daemon.requests[0])
        == #"{"from":10000000,"kind":"words","name":"scribe","op":"engine","track":"ear-desk"}"#)
    #expect(spans(daemon.heard) == ["0 340000"] && finals(daemon.heard) == [["w10 20000 340000"]])
    #expect(
      ends == [
        "returned", "a line of 4 bytes not a JSON object", #"{"code":"taken","ok":false}"#, ended,
      ])
    #expect(spoken == [#"scribe: refused: {"code":"bad_request","ok":false}"#])
  }

  @Test("standing: a failure said once while it repeats, another said; any other error escapes")
  func stands() async throws {
    // nil: a session that ran to its connection's end
    var causes: [(any Error)?] =
      [Failure(reason: "down"), Failure(reason: "down"), nil, nil, Failure(reason: "down"), Stop()]
    var (waits, escaped) = (0, false)
    listen()
    do {
      try await stand(
        { if let cause = causes.removeFirst() { throw cause } },
        wait: {
          waits += 1
          return true
        })
    } catch is Stop {
      escaped = true
    }
    #expect(spoken == ["scribe: down", "scribe: \(ended)", "scribe: down"])
    #expect(waits == 5 && escaped)
    written("scribe: a line on the suite's stderr, by the engine's own writer")
  }

  @Test(
    "the command line: usage and 2; a load's failure one line and 1; loaded, it says so and stands")
  func command() async throws {
    let load: (String, String?) async throws -> any Model = { dir, terms in
      guard dir == "m" else { throw Failure(reason: "no model here") }
      return Stub()
    }
    listen()
    var codes: [Int32] = []
    for args in [[], ["ear-desk"], ["--model", "m"], ["ear-desk", "--model", "x"]] {
      codes.append(await run(args, env: [:], load: load, wait: { _ in true }))
    }
    // 0 only once the wait, handed the poll, says to stop
    let env = ["RIFT_POLL_MS": "5", "RIFT_ROOT": "/nonexistent"]
    let args = ["ear-desk", "--model", "m", "--terms", "t"]
    codes.append(await run(args, env: env, load: load, wait: { $0 != .milliseconds(5) }))
    // the poll's default, 1 s, and a daemon whose connection ends
    let daemon = Daemon([{ _, conn in try conn.send(["ok": true, "through": NSNull()]) }])
    codes.append(
      await run(args, env: ["RIFT_ROOT": daemon.root], load: load, wait: { $0 != .seconds(1) }))
    #expect(codes == [2, 2, 2, 1, 0, 0])
    #expect(Array(spoken.prefix(4)) == [usage, usage, usage, "scribe: no model here"])
    #expect(
      spoken.dropFirst(4).map { $0.hasPrefix("scribe: ready in ") } == [true, false, true, false])
    #expect(spoken[5] == "scribe: No such file or directory" && spoken[7] == "scribe: \(ended)")
  }
}

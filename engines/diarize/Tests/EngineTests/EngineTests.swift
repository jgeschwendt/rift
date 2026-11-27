import Foundation
import Testing

@testable import Engine

/// A level whose samples the stub hears as the slots of `mask`'s bits speaking.
func level(_ mask: Int) -> Float { Float(mask) / 1_048_576 }

/// A diarizer that hears each step's first sample as a mask of the slots speaking, giving each
/// step `lag` steps late; it enrols a voice at the slot its audio's first sample's lowest bit
/// names (none for silence), and embeds any audio as `vector`.
final class Stub: Model {
  let lag: Int
  var vector: [Float] = [1, 0]
  var split = false
  var queued: [[Float]] = []
  /// The samples of the step being heard.
  var heard: [Float] = []
  /// Each start's labels, and the length of each embedding's audio.
  var started: [[String]] = []
  var embedded: [Int] = []

  init(lag: Int = 0) { self.lag = lag }

  func start(_ voices: [(label: String, pcm: [Float])]) throws -> [Int: String] {
    (started, queued, heard) = (started + [voices.map(\.label)], [], [])
    var slots: [Int: String] = [:]
    for voice in voices {
      let mask = Int(((voice.pcm.first ?? 0) * 1_048_576).rounded())
      if mask > 0 { slots[mask.trailingZeroBitCount] = voice.label }
    }
    return slots
  }

  func feed(_ pcm: [Float]) throws -> [[Float]] {
    heard += pcm
    guard heard.count == 1_600 else { return [] }
    let mask = Int((heard[0] * 1_048_576).rounded())
    heard = []
    queued.append((0..<20).map { mask >> $0 & 1 == 1 ? 0.9 : 0.1 })
    return queued.count > lag ? [queued.removeFirst()] : []
  }

  func embed(_ pcm: [Float]) throws -> [Float] {
    embedded.append(pcm.count)
    // [1, 0] for a full 10 s, [0, 1] for a shorter piece, while `split`
    return split ? (pcm.count == 160_000 ? [1, 0] : [0, 1]) : vector
  }
}

/// One line of what reaches a talkers engine, as the tests write it.
enum Line {
  /// `count` frames of the slots `mask` names from `t` µs, 20 ms apart.
  case frames(Int, Int, Int)
  /// The same, marked masked by the echo stage (whose zeros the stub would hear as nobody).
  case masked(Int, Int, Int)
  case turn(Int)
}

/// Every report `lines` draw from `talkers`, in order.
func run(_ talkers: Talkers, _ lines: [Line]) throws -> [[String: Any]] {
  var said: [[String: Any]] = []
  for line in lines {
    switch line {
    case .frames(let t, let mask, let count), .masked(let t, let mask, let count):
      for at in 0..<count {
        let masked = if case .masked = line { true } else { false }
        said += try talkers.frame(
          t + at * 20_000, [Float](repeating: level(mask), count: 320), masked: masked)
      }
    case .turn(let end): said += try talkers.turn(end)
    }
  }
  return said
}

/// Each span, `start end` and its talkers, `id` or `id:score`.
func spans(_ said: [[String: Any]]) -> [String] {
  said.map { span in
    let talkers = (span["talkers"] as! [[String: Any]]).map {
      $0["score"] is NSNull ? "\($0["id"]!)" : "\($0["id"]!):\($0["score"]!)"
    }
    return (["\(span["start"]!)", "\(span["end"]!)"] + talkers).joined(separator: " ")
  }
}

/// A connection's engine with `voices` enrolled, each `(label, slot)`, its embedding [1, 0].
func talkers(_ model: Stub, _ voices: [(String, Int)] = [], floor: Int? = nil) throws -> Talkers {
  try Talkers(
    model: model,
    voices: voices.map { ($0.0, [level(1 << $0.1)], [1, 0]) }, floor: floor)
}

/// What the engine says on stderr while a test runs.
nonisolated(unsafe) var spoken: [String] = []

/// The engine's writer swapped for one the test reads in `spoken`, emptied, and the said-once
/// lines forgotten; a closure-taking helper miscompiled under Swift 6.2.4 (scribe's suite).
func listen() {
  (spoken, told, say) = ([], [], { spoken.append($0) })
}

/// The error a test throws to leave a loop that never ends by itself.
struct Stop: Error {}

/// A daemon the suite plays: a socket listening at a root's run/rift.sock under /tmp, each
/// connection answered its hello, its request kept, then run by the next of `scripts`, on a
/// thread of its own; each script has written all it will by the time the engine sees its end.
final class Daemon: @unchecked Sendable {
  let root = "/tmp/diarize-\(getpid())-\(UUID().uuidString.prefix(8))"
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

/// A frame line of `count` samples at `level`, at `t`.
func frameLine(_ t: Int, _ level: Float, masked: Bool = false) -> [String: Any] {
  let sample = UInt16(bitPattern: Int16((level * 32_768).rounded()))
  let hex = String(format: "%02x%02x", sample & 0xff, sample >> 8)
  let line: [String: Any] = ["pcm": String(repeating: hex, count: 320), "t": t]
  return masked ? line.merging(["masked": true]) { $1 } : line
}

/// A session's lines: unknown ones and known ones missing their fields, then 0.5 s of slot 5's
/// voice (the sample 1/32768, the stub's mask 32), 0.6 s of it masked, a turn line its floor
/// already covers, 0.5 s of it again and 0.3 s of silence.
func voiced() -> [[String: Any]] {
  let voice = Float(1) / 32_768
  var lines: [[String: Any]] = [["unknown": 1], ["turn": [String: Any]()], ["pcm": ""]]
  lines += (0..<25).map { frameLine($0 * 20_000, voice) }
  lines += (25..<55).map { frameLine($0 * 20_000, voice, masked: true) }
  lines.append(["turn": ["end": 500_000, "start": 0]])
  lines += (55..<80).map { frameLine($0 * 20_000, voice) }
  return lines + (80..<95).map { frameLine($0 * 20_000, 0) }
}

@Suite(.serialized) struct Diarize {
  @Test("settled spans only: none before its steps settle, each from its frames, none past them")
  func settled() throws {
    let model = Stub(lag: 9)
    let engine = try talkers(model)
    // 0.5 s of a voice, then nobody: the lag and the median's two steps hold the first span
    #expect(try run(engine, [.frames(0, 1, 25), .frames(500_000, 0, 55)]).isEmpty)
    #expect(try spans(run(engine, [.frames(1_600_000, 0, 5)])) == ["0 500000 1"])
    // the nobody span is held open, never past the frames given
    #expect(engine.open! == (500_000, 600_000, []))
  }

  @Test("a median over five steps: a voice or a pause of one step smoothed away, a change cut")
  func smoothed() throws {
    let said = try run(
      talkers(Stub()),
      [
        .frames(0, 0, 10), .frames(200_000, 1, 5), .frames(300_000, 0, 10),
        .frames(500_000, 2, 15), .frames(800_000, 0, 5), .frames(900_000, 2, 15),
        .frames(1_200_000, 0, 25),
      ])
    #expect(spans(said) == ["0 500000", "500000 1200000 1"])
  }

  @Test("nobody and unknown: a hole closes the span at its start and leaves its time unknown")
  func holes() throws {
    let said = try run(
      talkers(Stub()),
      [.frames(0, 1, 12), .frames(1_000_000, 1, 15), .frames(1_300_000, 0, 30)])
    // the step a hole cuts short is fed padded; the time between 240 ms and 1 s no span covers
    #expect(spans(said) == ["0 240000 1", "1000000 1300000 1"])
  }

  @Test("masked audio is nobody's, whatever the model hears in it")
  func masked() throws {
    let said = try run(
      talkers(Stub()), [.frames(0, 1, 25), .masked(500_000, 1, 10), .frames(700_000, 1, 40)])
    #expect(spans(said) == ["0 500000 1", "500000 700000"])
  }

  @Test("an enrolled voice by its label, scored; others numbered from 1 as first reported")
  func ids() throws {
    let model = Stub()
    let engine = try talkers(model, [("visitor", 3), ("alpha", 0)])
    let said = try run(
      engine,
      [
        .frames(0, 0b100, 25), .frames(500_000, 0b10, 25), .frames(1_000_000, 0b1011, 25),
        .frames(1_500_000, 0b1, 5), .frames(1_600_000, 0, 30),
      ])
    #expect(
      spans(said) == [
        "0 500000 1", "500000 1000000 2", "1000000 1500000 alpha visitor 2",
        "1500000 1600000 alpha",
      ])
    // under 2 s, unscored
    #expect(model.embedded.isEmpty)
    // a new connection's engine numbers afresh
    let again = try run(talkers(Stub()), [.frames(0, 0b10, 25), .frames(500_000, 0, 30)])
    #expect(spans(again) == ["0 500000 1"])
  }

  @Test("a score: the cosine mapped linearly, 0.53 at 0.5; a zero embedding's cosine 0")
  func scores() throws {
    #expect((score(0.53), score(0.06), score(-1), score(1), score(2)) == (0.5, 0, 0, 1, 1))
    #expect(
      (cosine([1, 0], [0, 1]), cosine([0, 0], [1, 0]), cosine([3, 4], [3, 4])) == (0, 0, 1))
    let model = Stub()
    model.vector = [1, 1]
    let said = try run(
      talkers(model, [("alpha", 0)]), [.frames(0, 1, 100), .frames(2_000_000, 0, 15)])
    #expect(spans(said) == ["0 2000000 alpha:0.688"] && model.embedded == [32_000])
  }

  @Test("a turn line cuts the span reaching its end there, once settled that far, or at once")
  func turns() throws {
    // read before its end has settled: cut when it does, the next span from it alike
    let early = try run(
      talkers(Stub()),
      [.frames(0, 1, 20), .turn(330_000), .frames(400_000, 1, 15), .frames(700_000, 0, 20)])
    #expect(spans(early) == ["0 330000 1", "330000 700000 1"])
    // read after: the open span cut at once; one at or before the floor, or the span's start,
    // cuts nothing
    let engine = try talkers(Stub())
    #expect(try run(engine, [.frames(0, 1, 40)]).isEmpty)
    #expect(
      try spans(run(engine, [.turn(250_000), .turn(250_000), .turn(100_000)])) == ["0 250000 1"])
    // one past the settled reach waits; one at it closes the span whole, the next opening there
    #expect(try run(engine, [.turn(900_000)]).isEmpty)
    #expect(try spans(run(engine, [.turn(600_000)])) == ["250000 600000 1"])
    let after = try run(engine, [.frames(800_000, 1, 25), .frames(1_300_000, 0, 15)])
    #expect(spans(after) == ["600000 900000 1", "900000 1300000 1"])
  }

  @Test("resumed from through: nothing reported starts before it, a span reaching it cut there")
  func resumed() throws {
    let said = try run(
      talkers(Stub(), floor: 300_000),
      [.turn(200_000), .turn(400_000), .frames(0, 1, 25), .frames(500_000, 0, 15), .turn(300_000)])
    #expect(spans(said) == ["300000 400000 1", "400000 500000 1"])
    // a span wholly before the floor is dropped
    let gone = try run(
      talkers(Stub(), floor: 600_000), [.frames(0, 1, 25), .frames(500_000, 0, 20)])
    #expect(gone.isEmpty)
  }

  @Test("a span held 20 s is cut there by the engine's own rule")
  func cap() throws {
    let said = try run(talkers(Stub()), [.frames(0, 1, 1_010), .frames(20_200_000, 0, 15)])
    #expect(spans(said) == ["0 20000000 1", "20000000 20200000 1"])
  }

  @Test("at most 16 talkers, labels first, the rest left out and said once")
  func crowded() throws {
    listen()
    let model = Stub()
    let engine = try talkers(model, [("alpha", 19)])
    let said = try run(
      engine,
      [
        .frames(0, (1 << 20) - 1, 25), .frames(500_000, 0b11, 25),
        .frames(1_000_000, (1 << 20) - 1, 25), .frames(1_500_000, 0, 15),
      ])
    let first = said[0]["talkers"] as! [[String: Any]]
    #expect(
      first.count == 16 && first[0]["id"] as? String == "alpha" && first[15]["id"] as? Int == 15)
    #expect(
      spans([said[1]]) == ["500000 1000000 1 2"] && (said[2]["talkers"] as! [Any]).count == 16)
    #expect(spoken == ["diarize: a span named more than 16 talkers; the rest were left out"])
  }

  @Test("a model giving more steps than fed, or trailing past 2 s, is a fault rather than paired")
  func paired() throws {
    final class Over: Model {
      let rows: Int
      init(_ rows: Int) { self.rows = rows }
      func start(_ voices: [(label: String, pcm: [Float])]) throws -> [Int: String] { [:] }
      func feed(_ pcm: [Float]) throws -> [[Float]] { Array(repeating: [0.9], count: rows) }
      func embed(_ pcm: [Float]) throws -> [Float] { [] }
    }
    var failed: [String] = []
    for (model, frames) in [(Over(2), 5), (Over(0), 105)] {
      let engine = try Talkers(model: model, voices: [], floor: nil)
      do {
        for at in 0..<frames { _ = try engine.frame(at * 20_000, [0], masked: false) }
      } catch let fault as Fault {
        // never a Failure, which standing would wait out and repeat unsaid
        failed.append(fault.localizedDescription)
      }
    }
    #expect(failed == ["the model gave 2 steps with 0 fed", "the model gave 0 steps with 21 fed"])
  }

  @Test("16 kHz to 8 kHz the same however the input is cut; 1 kHz passed, 4.2 and 6 kHz stopped")
  func decimated() {
    let tone = { (hz: Double) in
      (0..<16_000).map { Float(sin(2 * Double.pi * hz * Double($0) / 16_000)) }
    }
    let low = tone(1_000)
    var whole = Decimator()
    let once = whole(low)
    for size in [1, 7, 320, 1_600] {
      var cut = Decimator()
      let pieces = stride(from: 0, to: 16_000, by: size).flatMap {
        cut(Array(low[$0..<min(16_000, $0 + size)]))
      }
      #expect(pieces == once, "\(size)")
    }
    let power = { (pcm: [Float]) in pcm.dropFirst(100).map { $0 * $0 }.reduce(0, +) / 7_900 }
    #expect(once.count == 8_000 && abs(power(once) - 0.5) < 0.01)
    // over 60 dB down: 4.2 kHz would fold to 3.8 kHz
    for hz in [4_200.0, 6_000] {
      var high = Decimator()
      #expect(power(high(tone(hz))) < 0.5e-6, "\(hz)")
    }
  }

  @Test("the wire's pieces: a label's form, hex samples, PCM, the socket, a flag")
  func pieces() {
    #expect(["a", "alpha-2", "x_y", String(repeating: "a", count: 64)].allSatisfy(labelled))
    #expect(
      !["", "1a", "-a", "_a", "a b", "a.b", "é", String(repeating: "a", count: 65)].contains(
        where: labelled))
    #expect(samples("0000ff7f0080") == [0, Float(32_767) / 32_768, -1])
    #expect(floats(Data([0, 0, 0xff, 0x7f, 0, 0x80])) == [0, Float(32_767) / 32_768, -1])
    #expect(socketPath(["RIFT_ROOT": "r"]) == "r/run/rift.sock")
    #expect(socketPath(["HOME": "/home"]) == "/home/.local/state/rift/run/rift.sock")
    #expect(socketPath([:]) == "/.local/state/rift/run/rift.sock")
    #expect((flag(["x", "--model", "m"], "model"), flag(["--model"], "model")) == ("m", nil))
  }

  @Test("enrol and forget: 0600 in a 0700 directory, by temp and rename; refusals named")
  func profiles() throws {
    listen()
    let dir = NSTemporaryDirectory() + "diarize-\(UUID().uuidString.prefix(8))/profiles"
    defer {
      try? FileManager.default.removeItem(atPath: (dir as NSString).deletingLastPathComponent)
    }
    let model = Stub()
    // the sample 1/32768, which 16 bits keep: the stub's slot 5
    let audio = [Float](repeating: level(32), count: 170_000)
    // the 10 s and the 0.625 s tail weighted by their lengths
    model.split = true
    _ = try enrol("alpha", audio, in: dir, model: model)
    #expect(voices(dir)[0].embedding == [Float(160_000) / 170_000, Float(10_000) / 170_000])
    (model.split, model.embedded, model.started) = (false, [], [])
    let path = try enrol("alpha", audio, in: dir, model: model)
    let attributes = try FileManager.default.attributesOfItem(atPath: path)
    let parent = try FileManager.default.attributesOfItem(atPath: dir)
    #expect(path == dir + "/alpha.json" && attributes[.posixPermissions] as? Int == 0o600)
    #expect(parent[.posixPermissions] as? Int == 0o700)
    #expect(try FileManager.default.contentsOfDirectory(atPath: dir) == ["alpha.json"])
    // the embedding is the mean of each 10 s's; the enrolled audio comes back as it was
    #expect(model.embedded == [160_000, 10_000] && model.started == [["alpha"]])
    var read = voices(dir)
    #expect(read.map(\.label) == ["alpha"] && read[0].embedding == [1, 0])
    #expect(read[0].pcm.count == 170_000 && read[0].pcm[0] == level(32))
    // a profile written over keeps its mode
    _ = try enrol("alpha", audio, in: dir, model: model)
    #expect(
      try FileManager.default.attributesOfItem(atPath: path)[.posixPermissions] as? Int == 0o600)
    var refused: [String] = []
    for (label, given) in [
      ("1ada", audio), ("beta", [Float](repeating: 0, count: 16_000)), ("beta", [0]),
    ] {
      do { _ = try enrol(label, given, in: dir, model: model) } catch {
        refused.append(error.localizedDescription)
      }
    }
    do { _ = try enrol("beta", audio, in: "/dev/null/x", model: model) } catch {
      refused.append("mkdir")
    }
    // an existing directory open to others is made 0700
    let open = dir + "/open"
    try FileManager.default.createDirectory(
      atPath: open, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o755])
    _ = try enrol("beta", audio, in: open, model: model)
    let opened = try FileManager.default.attributesOfItem(atPath: open)
    #expect(opened[.posixPermissions] as? Int == 0o700)
    // one not this user's to change, a temp that cannot be made, a profile not to be renamed over
    try FileManager.default.createDirectory(
      atPath: open + "/.gamma.json.new", withIntermediateDirectories: false)
    try FileManager.default.createDirectory(
      atPath: open + "/delta.json/held", withIntermediateDirectories: true)
    for (label, into) in [("beta", "/usr"), ("gamma", open), ("delta", open)] {
      do { _ = try enrol(label, audio, in: into, model: model) } catch {
        refused.append(error.localizedDescription)
      }
    }
    #expect(!FileManager.default.fileExists(atPath: open + "/.delta.json.new"))
    try FileManager.default.removeItem(atPath: open)
    #expect(
      refused == [
        "1ada: a label is 1 to 64 letters, digits, - or _, starting with a letter",
        "no voice found in the audio", "under 1 s of audio", "mkdir",
        "/usr: Operation not permitted", "\(open)/gamma.json: File exists",
        "\(open)/delta.json: Is a directory",
      ])
    // a file that is no label's profile is said once, as is a directory that cannot be read
    for (name, text) in [("2x.json", "{}"), ("beta.json", "{}"), ("notes.txt", "")] {
      try text.write(toFile: dir + "/" + name, atomically: true, encoding: .utf8)
    }
    read = voices(dir) + voices(dir) + voices(dir + "/none") + voices(nil)
    // one another user owns, or others may write: nothing loaded, said once
    chmod(dir, 0o770)
    read += voices(dir) + voices(dir) + voices("/usr")
    chmod(dir, 0o700)
    #expect(read.map(\.label) == ["alpha", "alpha"])
    #expect(
      spoken == [
        "diarize: \(dir)/2x.json: not a profile; skipped",
        "diarize: \(dir)/beta.json: not a profile; skipped",
        "diarize: \(dir)/none: cannot be read; no voice is enrolled",
        "diarize: \(dir): not this user's, or writable by others; no voice is enrolled",
        "diarize: /usr: not this user's, or writable by others; no voice is enrolled",
      ])
    try forget("alpha", in: dir)
    #expect(!FileManager.default.fileExists(atPath: path))
    var forgot: [String] = []
    for label in ["alpha", "-x"] {
      do { try forget(label, in: dir) } catch { forgot.append(error.localizedDescription) }
    }
    #expect(
      forgot == [
        "no profile alpha in \(dir)",
        "-x: a label is 1 to 64 letters, digits, - or _, starting with a letter",
      ])
  }

  @Test(
    "a session from through: frames and turn lines acted on, masks heeded, unknown lines ignored, a refusal said once while it repeats; a voice not taken said once"
  )
  func sessions() throws {
    let daemon = Daemon([
      { daemon, conn in
        try conn.send(["ok": true, "seq": 1, "through": NSNull()])
        for line in voiced() { try conn.send(line) }
        daemon.heard += [try conn.line()!, try conn.line()!, try conn.line()!]
        for _ in 0..<2 { try conn.send(["code": "bad_request", "ok": false]) }
        try conn.send(["code": "other", "ok": false])
        shutdown(conn.fd, SHUT_RDWR)
      },
      { _, conn in
        try conn.send(["ok": true, "seq": 2, "through": NSNull()])
        _ = write(conn.fd, "nope\n", 5)
      },
      { _, conn in try conn.send(["code": "taken", "ok": false]) },
      // closed before its reply
      { _, _ in },
    ])
    listen()
    var ends: [String] = []
    let voices = [("alpha", [level(1 << 5)], [Float(1), 0]), ("beta", [Float(0)], [Float(1), 0])]
    for _ in 0..<4 {
      do {
        try session(path: daemon.path, track: "ear-desk", model: Stub(), voices: voices)
        ends.append("returned")
      } catch let failure as Failure {
        ends.append(failure.reason)
      }
    }
    #expect(
      json(daemon.requests[0])
        == #"{"from":"through","kind":"talkers","name":"diarize","op":"engine","track":"ear-desk"}"#
    )
    #expect(
      spans(daemon.heard) == ["0 500000 alpha", "500000 1100000", "1100000 1600000 alpha"])
    #expect(
      ends == [
        "returned", "a line of 4 bytes not a JSON object", #"{"code":"taken","ok":false}"#, ended,
      ])
    #expect(
      spoken == [
        "diarize: beta: its profile's audio took no voice; not enrolled",
        #"diarize: refused: {"code":"bad_request","ok":false}"#,
        #"diarize: refused: {"code":"other","ok":false}"#,
      ])
  }

  @Test("the daemon's read: its frames' samples in order, to its end")
  func reads() throws {
    let daemon = Daemon([
      { _, conn in
        try conn.send(["ok": true])
        try conn.send(frameLine(0, 0.5))
        try conn.send(["pcm": "0000", "t": 20_000])
      },
      { _, conn in try conn.send(["code": "gone", "ok": false]) },
    ])
    let audio = try read(path: daemon.path, track: "ear-desk", from: 0, to: 22_000)
    #expect(audio.count == 321 && audio[0] == 0.5 && audio[320] == 0)
    #expect(json(daemon.requests[0]) == #"{"from":0,"op":"read","to":22000,"track":"ear-desk"}"#)
    #expect(throws: Failure.self) { try read(path: daemon.path, track: "ear-desk", from: 0, to: 1) }
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
    #expect(spoken == ["diarize: down", "diarize: \(ended)", "diarize: down"])
    #expect(waits == 5 && escaped)
    written("diarize: a line on the suite's stderr, by the engine's own writer")
  }

  @Test(
    "the command line: usage and 2; the engine loaded, said ready, stood; enrol from a file or the daemon, forget; failures one line and 1"
  )
  func command() async throws {
    let load: (String) async throws -> any Model = { dir in
      guard dir == "m" else { throw Failure(reason: "no model here") }
      return Stub()
    }
    let dir = NSTemporaryDirectory() + "diarize-\(UUID().uuidString.prefix(8))"
    defer { try? FileManager.default.removeItem(atPath: dir) }
    try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
    // 1.25 s of the sample 32: the stub's slot 10
    let file = dir + "/voice.pcm"
    try Data([UInt8](repeating: 0, count: 40_000).indices.map { $0 % 2 == 0 ? 0x20 : 0 })
      .write(to: URL(fileURLWithPath: file))
    let profiles = dir + "/profiles"
    listen()
    var codes: [Int32] = []
    for args in [
      [], ["--model", "m"], ["ear-desk"], ["enrol"], ["enrol", "alpha", "--model", "m"],
      ["enrol", "alpha", "--model", "m", "--profiles", profiles],
      ["enrol", "alpha", "--model", "m", "--profiles", profiles, "--track", "t", "--from", "0"],
      ["forget"], ["forget", "alpha"],
      // a mistyped verb, by its second word or its flags
      ["enroll", "alpha", "--model", "m", "--profiles", profiles, "--file", file],
      ["enroll", "--model", "m", "--file", file],
    ] {
      codes.append(await run(args, env: [:], load: load, wait: { _ in true }))
    }
    #expect(codes == Array(repeating: 2, count: 11) && spoken == Array(repeating: usage, count: 11))
    listen()
    codes = []
    let enrolling = ["enrol", "alpha", "--model", "m", "--profiles", profiles]
    for args in [
      enrolling + ["--file", file],
      ["enrol", "9", "--model", "x", "--profiles", profiles, "--file", file],
      ["enrol", "alpha", "--model", "x", "--profiles", profiles, "--file", file],
      enrolling + ["--file", dir + "/none"],
      ["forget", "alpha", "--profiles", profiles], ["forget", "alpha", "--profiles", profiles],
      ["ear-desk", "--model", "x"],
    ] {
      codes.append(await run(args, env: [:], load: load, wait: { _ in true }))
    }
    #expect(codes == [0, 1, 1, 1, 0, 1, 1])
    #expect(
      spoken.dropLast(1) == [
        "diarize: enrolled alpha in \(profiles)/alpha.json",
        "diarize: 9: a label is 1 to 64 letters, digits, - or _, starting with a letter",
        "diarize: no model here",
        spoken[3], "diarize: forgot alpha", "diarize: no profile alpha in \(profiles)",
      ] && spoken.last == "diarize: no model here")
    // from the daemon's read of the track, then the engine over the same daemon, its poll's
    // default 1 s, and once handed $RIFT_POLL_MS
    let daemon = Daemon([
      { _, conn in
        try conn.send(["ok": true])
        for at in 0..<60 { try conn.send(frameLine(at * 20_000, Float(32) / 32_768)) }
      },
      { _, conn in try conn.send(["ok": true, "through": NSNull()]) },
    ])
    listen()
    let env = ["RIFT_ROOT": daemon.root]
    let span = ["--track", "ear-desk", "--from", "0", "--to", "1200000"]
    #expect(await run(enrolling + span, env: env, load: load, wait: { _ in true }) == 0)
    #expect(voices(profiles).first?.pcm.count == 19_200)
    let engine = ["ear-desk", "--model", "m", "--profiles", profiles]
    #expect(await run(engine, env: env, load: load, wait: { $0 != .seconds(1) }) == 0)
    let polled = ["RIFT_POLL_MS": "5", "RIFT_ROOT": "/nonexistent"]
    #expect(await run(engine, env: polled, load: load, wait: { $0 != .milliseconds(5) }) == 0)
    #expect(spoken[0] == "diarize: enrolled alpha in \(profiles)/alpha.json")
    #expect(
      spoken.dropFirst().map { $0.hasPrefix("diarize: ready in ") } == [true, false, true, false])
    #expect(spoken[2] == "diarize: \(ended)" && spoken[4] == "diarize: No such file or directory")
  }
}

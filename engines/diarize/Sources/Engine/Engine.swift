import Foundation

/// The engine's name on the wire.
let engineName = "diarize"
/// The usage line.
let usage =
  "usage: diarize <track> --model <dir> [--profiles <dir>] | diarize enrol <label> --model <dir>"
  + " --profiles <dir> (--file <pcm> | --track <track> --from <t> --to <t>)"
  + " | diarize forget <label> --profiles <dir>"
/// The most enrolment audio a profile keeps: 30 s.
let seedSamples = 480_000

/// A profile's file, `<label>.json`: the embedding of its voice and the enrolment audio it was
/// taken from, 16-bit little-endian PCM in base64, which LS-EEND is enrolled with as it connects.
struct Profile: Codable {
  let embedding: [Float]
  let pcm: String
}

/// The lines said once in the process's life: a profile's trouble, said at every connect.
nonisolated(unsafe) var told: Set<String> = []

/// Says `line` unless it was said before.
func once(_ line: String) {
  if told.insert(line).inserted { say(line) }
}

/// 16 kHz 16-bit little-endian PCM as floats in [-1, 1).
func floats(_ data: Data) -> [Float] {
  data.withUnsafeBytes {
    $0.bindMemory(to: Int16.self).map { Float(Int16(littleEndian: $0)) / 32_768 }
  }
}

/// `label` refused unless of a label's form.
func valid(_ label: String) throws {
  guard labelled(label) else {
    throw Failure(
      reason: "\(label): a label is 1 to 64 letters, digits, - or _, starting with a letter")
  }
}

/// The voices in `dir`'s profiles, in byte order of their labels; a file that is no label's
/// profile, or a directory that cannot be read, said once and skipped.
func voices(_ dir: String?) -> [(label: String, pcm: [Float], embedding: [Float])] {
  guard let dir else { return [] }
  guard let names = try? FileManager.default.contentsOfDirectory(atPath: dir) else {
    once("diarize: \(dir): cannot be read; no voice is enrolled")
    return []
  }
  return names.filter { $0.hasSuffix(".json") }.sorted().compactMap { name in
    let label = String(name.dropLast(5))
    guard labelled(label),
      let data = FileManager.default.contents(atPath: "\(dir)/\(name)"),
      let profile = try? JSONDecoder().decode(Profile.self, from: data),
      let pcm = Data(base64Encoded: profile.pcm)
    else {
      once("diarize: \(dir)/\(name): not a profile; skipped")
      return nil
    }
    return (label, floats(pcm), profile.embedding)
  }
}

/// `audio`'s voice enrolled as `label` in `dir` (made 0700 when absent): its first 30 s, which
/// LS-EEND must take as a voice, and the mean of WeSpeaker's embeddings of each 10 s of it, written
/// to `<dir>/<label>.json` 0600 by temp and rename; its path.
func enrol(_ label: String, _ audio: [Float], in dir: String, model: any Model) throws -> String {
  try valid(label)
  let seed = Array(audio.prefix(seedSamples))
  guard seed.count >= rate else { throw Failure(reason: "under 1 s of audio") }
  guard try model.start([(label, seed)]).values.contains(label) else {
    throw Failure(reason: "no voice found in the audio")
  }
  let parts = try stride(from: 0, to: seed.count, by: window).map {
    try model.embed(Array(seed[$0..<min(seed.count, $0 + window)]))
  }
  let embedding = parts[0].indices.map { at in
    parts.map { $0[at] }.reduce(0, +) / Float(parts.count)
  }
  let pcm = seed.flatMap {
    withUnsafeBytes(of: Int16(max(-32_768, min(32_767, $0 * 32_768))).littleEndian, Array.init)
  }
  let data = try JSONEncoder().encode(
    Profile(embedding: embedding, pcm: Data(pcm).base64EncodedString()))
  try FileManager.default.createDirectory(
    atPath: dir, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
  let (path, temp) = ("\(dir)/\(label).json", "\(dir)/.\(label).json.new")
  // created 0600, never wider for a moment
  let fd = open(temp, O_CREAT | O_WRONLY | O_TRUNC, 0o600)
  let wrote =
    fd >= 0 && fchmod(fd, 0o600) == 0
    && data.withUnsafeBytes { write(fd, $0.baseAddress, data.count) } == data.count
  if fd >= 0 { close(fd) }
  guard wrote, rename(temp, path) == 0 else {
    let reason = String(cString: strerror(errno))
    unlink(temp)
    throw Failure(reason: "\(path): \(reason)")
  }
  return path
}

/// `label`'s profile removed from `dir`.
func forget(_ label: String, in dir: String) throws {
  try valid(label)
  guard unlink("\(dir)/\(label).json") == 0 else {
    throw Failure(reason: "no profile \(label) in \(dir)")
  }
}

/// The samples of `track` from `from` to `to`, read from the daemon as any client reads.
func read(path: String, track: String, from: Int, to: Int) throws -> [Float] {
  let conn = try Conn(path: path, request: ["from": from, "op": "read", "to": to, "track": track])
  var audio: [Float] = []
  while let pcm = try conn.line()?["pcm"] as? String {
    audio += samples(pcm)
  }
  return audio
}

/// One connection as the talkers engine of `track`, from its kind's `through` on so a start or a
/// reconnect resumes, `voices` enrolled afresh: each frame and turn line acted on, a refusal
/// said once while it repeats, any other line ignored, until the connection ends.
func session(
  path: String, track: String, model: any Model,
  voices: [(label: String, pcm: [Float], embedding: [Float])]
) throws {
  // its reads block; on the main thread they would starve the stop signals' handlers
  dispatchPrecondition(condition: .notOnQueue(.main))
  let request: [String: Any] = [
    "from": "through", "kind": "talkers", "name": engineName, "op": "engine", "track": track,
  ]
  let conn = try Conn(path: path, request: request)
  let talkers = try Talkers(model: model, voices: voices, floor: conn.reply["through"] as? Int)
  for voice in voices where !talkers.labels.values.contains(voice.label) {
    once("diarize: \(voice.label): its profile's audio took no voice; not enrolled")
  }
  var refused = ""
  while let line = try conn.line() {
    if line["ok"] as? Bool == false, json(line) != refused {
      refused = json(line)
      say("diarize: refused: \(refused)")
    }
    var reports: [[String: Any]] = []
    if let end = (line["turn"] as? [String: Any])?["end"] as? Int {
      reports = try talkers.turn(end)
    }
    if let pcm = line["pcm"] as? String, let t = line["t"] as? Int {
      reports = try talkers.frame(t, samples(pcm), masked: line["masked"] as? Bool == true)
    }
    for report in reports { try conn.send(report) }
  }
}

/// The command line. `enrol` and `forget`: usage and 2 without their arguments, else done (one
/// line, 0) or refused (one line, 1). Else the engine: usage and 2 without a track or a model;
/// else the model loaded by `load` from `--model` before any connection, its readiness said once,
/// then stood on the socket `env` names, `--profiles` read at each connect, `wait` handed
/// $RIFT_POLL_MS (default 1000) between connections, until a stop signal (or `wait` says to
/// stop: 0); anything else that escapes is one stderr line and 1, for launchd to restart it.
public func run(
  _ args: [String], env: [String: String], load: (String) async throws -> any Model,
  wait: (Duration) async throws -> Bool
) async -> Int32 {
  let (verb, label) = (args.first ?? "", args.dropFirst().first ?? "-")
  let (dir, profiles) = (flag(args, "model"), flag(args, "profiles"))
  let span = (
    flag(args, "track"), flag(args, "from").flatMap { Int($0) },
    flag(args, "to").flatMap { Int($0) }
  )
  do {
    switch verb {
    case "enrol":
      guard !label.hasPrefix("-"), let dir, let profiles,
        flag(args, "file") != nil || (span.0 != nil && span.1 != nil && span.2 != nil)
      else { break }
      try valid(label)
      let model = try await load(dir)
      let audio =
        try flag(args, "file").map { floats(try Data(contentsOf: URL(fileURLWithPath: $0))) }
        ?? read(path: socketPath(env), track: span.0!, from: span.1!, to: span.2!)
      say("diarize: enrolled \(label) in \(try enrol(label, audio, in: profiles, model: model))")
      return 0
    case "forget":
      guard !label.hasPrefix("-"), let profiles else { break }
      try forget(label, in: profiles)
      say("diarize: forgot \(label)")
      return 0
    default:
      guard !verb.isEmpty, !verb.hasPrefix("-"), let dir else { break }
      let clock = ContinuousClock()
      let started = clock.now
      let model = try await load(dir)
      say("diarize: ready in \(Int((clock.now - started) / .milliseconds(1))) ms")
      let poll = Duration.milliseconds(Int(env["RIFT_POLL_MS", default: ""]) ?? 1000)
      try await stand(
        {
          try session(path: socketPath(env), track: verb, model: model, voices: voices(profiles))
        }, wait: { try await wait(poll) })
      return 0
    }
  } catch {
    say("diarize: \(error.localizedDescription)")
    return 1
  }
  say(usage)
  return 2
}

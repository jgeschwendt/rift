import Foundation

/// The µs an ear frame spans.
let frameSpan = 20_000
/// Ear frames to a model step: 100 ms.
let stepFrames = 5
/// The ear frames kept for scoring: 30 s.
let keptFrames = 1_500
/// The longest span held before the engine cuts it itself: 20 s, in µs.
let capSpan = 20_000_000
/// The shortest span scored, 0.5 s in µs; a shorter one's score is null.
let shortest = 500_000
/// The most talkers a span names.
let most = 16
/// The probability over which a slot is speaking in a step.
let bar: Float = 0.5
/// Steps either side of a step whose majority decides each slot in it (a median filter), so a
/// voice or a pause under 300 ms is smoothed away and a step settles this many steps after the
/// model gives it.
let radius = 2
/// The most steps a model's output may trail what it was fed: 2 s, over LS-EEND's 0.9 s.
let lag = 20
/// Samples a second.
let rate = 16_000
/// The most samples one embedding reads: WeSpeaker's 10 s window.
let window = 160_000

/// A streaming diarizer and a voice embedder.
public protocol Model {
  /// Starts a stream afresh with `voices` enrolled, each label's audio; the slot each one it took
  /// holds.
  func start(_ voices: [(label: String, pcm: [Float])]) throws -> [Int: String]
  /// The steps `pcm`, appended to the stream, completes: each 100 ms step's probability per slot,
  /// in order, never revised, the same however the stream is cut; at most `lag` steps behind it.
  func feed(_ pcm: [Float]) throws -> [[Float]]
  /// The voice in 16 kHz samples `pcm` as an embedding.
  func embed(_ pcm: [Float]) throws -> [Float]
}

/// A label's form: a track name's (1 to 64 ASCII letters, digits, `-` and `_`) starting with a
/// letter, so none reads as a number.
func labelled(_ label: String) -> Bool {
  let bytes = Array(label.utf8)
  let letter = { (byte: UInt8) in (65...90).contains(byte) || (97...122).contains(byte) }
  return (1...64).contains(bytes.count) && letter(bytes[0])
    && bytes.allSatisfy { letter($0) || (48...57).contains($0) || $0 == 45 || $0 == 95 }
}

/// Two embeddings' cosine similarity, 0 when either is all zeros.
func cosine(_ a: [Float], _ b: [Float]) -> Float {
  let (dot, aa, bb) = zip(a, b).reduce((Float(0), Float(0), Float(0))) {
    ($0.0 + $1.0 * $1.1, $0.1 + $1.0 * $1.0, $0.2 + $1.1 * $1.1)
  }
  return aa * bb > 0 ? dot / (aa * bb).squareRoot() : 0
}

/// A cosine similarity as a score: linear from -0.3 (0) to 1 (1), clamped, to 3 places, so the
/// 0.35 at which WeSpeaker parted two voices on 1 s windows reads 0.5.
func score(_ cosine: Float) -> Double {
  (min(1, max(0, (Double(cosine) + 0.3) / 1.3)) * 1_000).rounded() / 1_000
}

/// One connection's spans, every time on the daemon's clock in µs.
final class Talkers {
  let model: any Model
  /// Each enrolled slot's label.
  let labels: [Int: String]
  /// Each label's embedding.
  let profiles: [String: [Float]]
  /// The last span's end: nothing reported starts before it.
  var floor: Int
  /// Each slot reported with no label, its integer id.
  var ids: [Int: Int] = [:]
  /// The frames heard, the newest last.
  var kept: [(t: Int, pcm: [Float])] = []
  /// The frames of the step being gathered, each with whether it was masked.
  var step: [(t: Int, masked: Bool)] = []
  /// The steps whole whose probabilities the model has yet to give.
  var fed: [[(t: Int, masked: Bool)]] = []
  /// The steps given, each with its slots over the bar: those yet to settle from `next` on, up to
  /// `radius` settled before it.
  var given: [(frames: [(t: Int, masked: Bool)], on: Set<Int>)] = []
  var next = 0
  /// The settled span not yet reported, its talkers' slots rising.
  var open: (start: Int, end: Int, slots: [Int])?
  /// The turn ends read and not yet cut at, rising.
  var cuts: [Int] = []
  /// Whether a span past the cap was said.
  var crowded = false

  /// Spans from `floor`, the last span's end, on, with `voices` enrolled, each label's audio and
  /// embedding.
  init(model: any Model, voices: [(label: String, pcm: [Float], embedding: [Float])], floor: Int?)
    throws
  {
    self.model = model
    labels = try model.start(voices.map { ($0.label, $0.pcm) })
    // one profile per file, so one per label
    profiles = Dictionary(uniqueKeysWithValues: voices.map { ($0.label, $0.embedding) })
    self.floor = floor ?? 0
  }

  /// The frame at `t` heard and fed as it comes, a step a hole ends short first filled with
  /// silence, and the spans that settles.
  func frame(_ t: Int, _ samples: [Float], masked: Bool) throws -> [[String: Any]] {
    kept.append((t, samples))
    kept.removeFirst(max(0, kept.count - keptFrames))
    var said: [[String: Any]] = []
    if let last = step.last, last.t + frameSpan != t {
      let missing = stepFrames - step.count
      (fed, step) = (fed + [step], [])
      for _ in 0..<missing { said += try push([Float](repeating: 0, count: 320)) }
    }
    step.append((t, masked))
    if step.count == stepFrames { (fed, step) = (fed + [step], []) }
    return try said + push(samples + [Float](repeating: 0, count: max(0, 320 - samples.count)))
  }

  /// A frame's `samples` fed, and what settles; a model giving more steps than were fed, or
  /// falling more than `lag` behind, fails the connection rather than pair a step with the wrong
  /// frames.
  func push(_ samples: [Float]) throws -> [[String: Any]] {
    let rows = try model.feed(samples)
    guard rows.count <= fed.count, fed.count - rows.count <= lag else {
      throw Failure(reason: "the model gave \(rows.count) steps with \(fed.count) fed")
    }
    for slots in rows {
      given.append((fed.removeFirst(), Set(slots.indices.filter { slots[$0] > bar })))
    }
    var said: [[String: Any]] = []
    while given.count - next > radius {
      let around = given[max(0, next - radius)...next + radius].map(\.on)
      let slots = Set(around.joined()).filter { slot in
        around.filter { $0.contains(slot) }.count * 2 > around.count
      }.sorted()
      for frame in given[next].frames { said += try heard(frame.t, frame.masked ? [] : slots) }
      next += 1
      given.removeFirst(max(0, next - radius))
      next = min(next, radius)
    }
    return said
  }

  /// The settled frame at `t` spoken by `slots`: the open span carried on, or reported and
  /// another opened at a change, a hole, the cap, or a turn's end it reaches.
  func heard(_ t: Int, _ slots: [Int]) throws -> [[String: Any]] {
    var said = try cut(to: t)
    if let open, open.end == t, open.slots == slots, t + frameSpan - open.start <= capSpan {
      self.open!.end = t + frameSpan
      return said
    }
    if let open { said += try report(open.start, open.end, open.slots) }
    open = (t, t + frameSpan, slots)
    return said
  }

  /// A turn line: the span reaching its `end` cut there once settled that far.
  func turn(_ end: Int) throws -> [[String: Any]] {
    guard end > floor else { return [] }
    cuts = (cuts + [end]).sorted()
    return try cut(to: open?.end ?? floor)
  }

  /// The open span cut at each turn end up to `reach`: reported to it, the rest open, or reported
  /// whole when the end is at or past its own.
  func cut(to reach: Int) throws -> [[String: Any]] {
    var said: [[String: Any]] = []
    while let end = cuts.first, end <= reach {
      cuts.removeFirst()
      guard let open, end > open.start else { continue }
      said += try report(open.start, min(end, open.end), open.slots)
      self.open = end < open.end ? (end, open.end, open.slots) : nil
    }
    return said
  }

  /// The span from `start`, at or after the floor, to `end` as a report: each enrolled slot's
  /// label scored, then each other slot's integer, numbered as first reported, at most 16.
  func report(_ start: Int, _ end: Int, _ slots: [Int]) throws -> [[String: Any]] {
    let start = max(start, floor)
    guard end > start else { return [] }
    floor = end
    let named = slots.compactMap { labels[$0] }.sorted {
      Array($0.utf8).lexicographicallyPrecedes($1.utf8)
    }
    let others = slots.filter { labels[$0] == nil }.sorted {
      (ids[$0] ?? .max, $0) < (ids[$1] ?? .max, $1)
    }
    let kept = named.prefix(most).count
    let room = others.prefix(most - kept)
    for slot in room where ids[slot] == nil { ids[slot] = ids.count + 1 }
    if named.count + others.count > most, !crowded {
      say("diarize: a span named more than 16 talkers; the rest were left out")
      crowded = true
    }
    var talkers: [[String: Any]] = []
    for label in named.prefix(most) {
      talkers.append(["id": label, "score": try scored(label, start, end)])
    }
    talkers += room.map { ["id": ids[$0]!, "score": NSNull()] }
    return [["end": end, "op": "talkers", "start": start, "talkers": talkers]]
  }

  /// How sure the span from `start` to `end` is `label`'s voice: its kept audio's embedding
  /// against the profile's, null under 0.5 s.
  func scored(_ label: String, _ start: Int, _ end: Int) throws -> Any {
    guard end - start >= shortest, let profile = profiles[label] else { return NSNull() }
    var audio: [Float] = []
    for (t, frame) in kept where t + frameSpan > start && t < end {
      let low = max(0, (start - t) * rate / 1_000_000)
      let high = min(frame.count, (end - t) * rate / 1_000_000)
      if low < high { audio += frame[low..<high] }
    }
    return score(cosine(try model.embed(Array(audio.prefix(window))), profile))
  }
}

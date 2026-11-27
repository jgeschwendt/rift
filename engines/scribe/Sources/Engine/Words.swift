import Foundation

/// The span a voice opens may reach before the engine finishes it itself: 20 s, in µs.
let capSpan = 20_000_000
/// The µs a frame spans.
let frameSpan = 20_000
/// The frames kept: 30 s.
let keptFrames = 1_500
/// The µs the cap waits behind the newest frame: past a voice's close and its line, as
/// transcribe's feed does.
let lag = 300_000
/// The µs of audio before a voice's start decoded with it.
let preroll = 250_000
/// Samples a second.
let rate = 16_000

/// A word as a model heard it: its text, start and end in seconds into the audio it was given.
public struct Heard: Equatable, Sendable {
  public let text: String
  public let start: Double
  public let end: Double

  /// A word of `text` from `start` to `end`.
  public init(text: String, start: Double, end: Double) {
    (self.text, self.start, self.end) = (text, start, end)
  }
}

/// A speech-to-text model that decodes a finished span.
public protocol Model {
  /// The words in 16 kHz samples `pcm`, rising.
  func decode(_ pcm: [Float]) async throws -> [Heard]
}

/// The sample `us` µs in, rounded up.
func index(_ us: Int) -> Int { (us * rate + 999_999) / 1_000_000 }

/// One connection's spans, every time on the daemon's clock in µs.
final class Words {
  let model: any Model
  /// The last final's end: nothing at or before it is decoded again.
  var floor: Int
  /// The frames heard, the newest last.
  var kept: [(t: Int, pcm: [Float])] = []
  /// The open span's first time.
  var begin: Int?
  /// Whether a voice is on in the open span.
  var voice = false
  /// Whether the open span was carried on from a final's end (a cap's, or a turn's under a
  /// voice), not opened by a voice.
  var carried = false
  /// Whether the cap was said.
  var capped = false

  /// Words from `floor`, the last final's end, on.
  init(model: any Model, floor: Int?) {
    (self.model, self.floor) = (model, floor ?? 0)
  }

  /// The frame at `t` kept; a span past the cap, `lag` behind it, with its voice still on,
  /// finished there by the engine's own rule.
  func frame(_ t: Int, _ pcm: [Float]) async throws -> [[String: Any]] {
    kept.append((t, pcm))
    kept.removeFirst(max(0, kept.count - keptFrames))
    let end = t + frameSpan - lag
    guard voice, let begin, end - begin >= capSpan else { return [] }
    // said once: a voice that never ends is a stuck turn engine, not news each 20 s
    if !capped { say("scribe: a span reached 20 s and was finished there") }
    capped = true
    return try await finish(begin, end)
  }

  /// A voice line: an on opens a span from `start` less the pre-roll unless one is open (a turn
  /// held across voices); an off leaves the span open for its turn, but closes a carried span
  /// it ends at or before, which then holds none of that voice.
  func voiced(_ on: Bool, _ start: Int) {
    if !on, carried, let begin, start <= begin { self.begin = nil }
    // a voice at or before the last final's end, a replay's, opens nothing
    if on, begin == nil, start > floor { (begin, carried) = (max(floor, start - preroll), false) }
    voice = on && begin != nil
  }

  /// The final words of the open span to a turn's `end`, decoded once over its audio alone; a
  /// turn ending at or before the span begins closes a carried span and leaves one a voice
  /// opened (a replay's turn, already finished, under a later voice).
  func turn(_ end: Int) async throws -> [[String: Any]] {
    guard let begin, end > begin else {
      begin = carried ? nil : begin
      return []
    }
    return try await finish(begin, end)
  }

  /// The span's final from `start` to `end`, none when it holds no words; the next span from
  /// `end` when a voice is on.
  func finish(_ start: Int, _ end: Int) async throws -> [[String: Any]] {
    // the kept audio inside the span, and where each frame's piece of it was heard
    var pcm: [Float] = []
    var heard: [(first: Int, at: Int)] = []
    for (t, frame) in kept where t + frameSpan > start && t < end {
      let low = max(0, index(start - t))
      heard.append((pcm.count, t + low * 1_000_000 / rate))
      pcm += frame[low..<min(frame.count, index(end - t))]
    }
    (floor, begin, carried) = (end, voice ? end : nil, voice)
    let words = timed(try await model.decode(pcm), heard, start, end)
    return words.isEmpty ? [] : [["end": end, "op": "words", "start": start, "words": words]]
  }

  /// Each word mapped to when its audio was heard, holes included, rising, inside `start` to
  /// `end`.
  func timed(_ said: [Heard], _ heard: [(first: Int, at: Int)], _ start: Int, _ end: Int)
    -> [[String: Any]]
  {
    // the time a second into the audio was heard; an end, exclusive, by the piece it ends
    func at(_ second: Double, end: Bool = false) -> Int {
      let sample = Int((second * Double(rate)).rounded())
      let (first, at) = heard.last { end ? $0.first < sample : $0.first <= sample } ?? (0, start)
      return at + (sample - first) * 1_000_000 / rate
    }
    var floor = start
    var words: [[String: Any]] = []
    for word in said {
      let starts = at(word.start)
      guard starts < end else { break }
      floor = max(floor, starts)
      let stops = min(max(at(word.end, end: true), floor), end)
      words.append(["end": stops, "start": floor, "text": word.text])
      floor = stops
    }
    return words
  }
}

/// `heard`'s times carried to `text`, the same words rewritten (a rescoring's): each word both
/// share in order keeps its times, and each run of `text`'s words between two of them shares the
/// span between those two in equal parts.
public func retime(_ heard: [Heard], as text: String) -> [Heard] {
  let said = text.split(separator: " ").map(String.init)
  // longest[i][j]: the most words heard[i...] and said[j...] share in order
  var longest = Array(repeating: Array(repeating: 0, count: said.count + 1), count: heard.count + 1)
  for i in heard.indices.reversed() {
    for j in said.indices.reversed() {
      longest[i][j] =
        heard[i].text == said[j]
        ? longest[i + 1][j + 1] + 1 : max(longest[i + 1][j], longest[i][j + 1])
    }
  }
  var (out, run, from, i, j) = ([Heard](), [String](), heard.first?.start ?? 0, 0, 0)
  func spread(to: Double) {
    let share = (to - from) / Double(max(1, run.count))
    out += run.indices.map {
      Heard(text: run[$0], start: from + share * Double($0), end: from + share * Double($0 + 1))
    }
    run = []
  }
  while j < said.count {
    if i < heard.count, heard[i].text == said[j] {
      spread(to: heard[i].start)
      out.append(heard[i])
      (from, i, j) = (heard[i].end, i + 1, j + 1)
    } else if i < heard.count, longest[i + 1][j] >= longest[i][j + 1] {
      i += 1
    } else {
      run.append(said[j])
      j += 1
    }
  }
  spread(to: heard.last?.end ?? from)
  return out
}

/// The terms in a vocabulary file's text: each line (any newline, CRLF included) `Term` or
/// `Term: heard, heard`, blank lines and `#` comments skipped.
public func vocabulary(_ text: String) -> [(term: String, heard: [String])] {
  text.split(whereSeparator: \.isNewline).compactMap { line in
    let parts = line.split(separator: ":", maxSplits: 1).map {
      $0.trimmingCharacters(in: .whitespaces)
    }
    guard let term = parts.first, !term.isEmpty, !term.hasPrefix("#") else { return nil }
    let heard = parts.dropFirst().flatMap { $0.split(separator: ",") }
    return (term, heard.map { $0.trimmingCharacters(in: .whitespaces) }.filter { !$0.isEmpty })
  }
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

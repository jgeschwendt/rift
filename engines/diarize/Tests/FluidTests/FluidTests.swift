import Fluid
import Foundation
import Testing

@testable import Engine

/// The `--model` directory under $DIARIZE_MODEL, or none.
let model = ProcessInfo.processInfo.environment["DIARIZE_MODEL", default: ""]

@Test(
  "the models over the two-voice fixture, its first voice enrolled from its first phrase: that voice's label over 0.5 on its later phrase, the other's never over 0.5 as it",
  .enabled(
    if: FileManager.default.fileExists(
      atPath: model + "/speaker-diarization/wespeaker_v2.mlmodelc"),
    "skipped: set DIARIZE_MODEL to the --model directory (engines/README.md)"))
func fixture() async throws {
  // Pocket TTS: alba from 0.8 s to 3.0 s and 8.0 s to 10.1 s, marius from 3.8 s to 6.7 s and
  // 10.9 s to 13.0 s
  let fixture = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    .appending(path: "../../../fixtures/voices.pcm")
  let pcm = floats(try Data(contentsOf: fixture))
  let fluid = try await Fluid(model: model)
  let dir = NSTemporaryDirectory() + "diarize-fluid-\(getpid())"
  defer { try? FileManager.default.removeItem(atPath: dir) }
  _ = try enrol("first", Array(pcm[8_000..<52_800]), in: dir, model: fluid)
  let engine = try Talkers(model: fluid, voices: voices(dir), floor: nil)
  var said: [[String: Any]] = []
  for at in 0..<pcm.count / 320 {
    said += try engine.frame(at * 20_000, Array(pcm[at * 320..<at * 320 + 320]), masked: false)
  }
  // who each span names, by its middle, in each voice's phrases
  func named(_ from: Int, _ to: Int) -> Set<String> {
    Set(
      said.filter {
        (($0["start"] as! Int) + ($0["end"] as! Int)) / 2 > from
          && (($0["start"] as! Int) + ($0["end"] as! Int)) / 2 < to
      }
      .flatMap { ($0["talkers"] as! [[String: Any]]).map { "\($0["id"]!)" } })
  }
  // each span's score for `first`, by its middle, in each voice's phrases
  func scores(_ from: Int, _ to: Int) -> [Any] {
    said.filter {
      (($0["start"] as! Int) + ($0["end"] as! Int)) / 2 > from
        && (($0["start"] as! Int) + ($0["end"] as! Int)) / 2 < to
    }
    .flatMap { ($0["talkers"] as! [[String: Any]]).filter { $0["id"] as? String == "first" } }
    .map { $0["score"]! }
  }
  #expect(named(8_000_000, 10_100_000) == ["first"], "\(said)")
  let own = scores(8_000_000, 10_100_000)
  #expect(!own.isEmpty && own.allSatisfy { ($0 as? Double ?? 0) > 0.5 }, "\(own)")
  // LS-EEND alone reads marius's first phrase as the enrolled voice; the score parts them: none
  // of his spans scores `first` over 0.5
  #expect(named(10_900_000, 13_000_000) == ["1"], "\(said)")
  let other = scores(3_800_000, 6_700_000) + scores(10_900_000, 13_000_000)
  #expect(other.allSatisfy { ($0 as? Double ?? 0) < 0.5 }, "\(other)")
}

@Test(
  "overlap: alba's and marius's later phrases summed at 0.7, alba enrolled: both named at once",
  .enabled(
    if: FileManager.default.fileExists(
      atPath: model + "/speaker-diarization/wespeaker_v2.mlmodelc"),
    "skipped: set DIARIZE_MODEL to the --model directory (engines/README.md)"))
func overlap() async throws {
  let fixture = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    .appending(path: "../../../fixtures/voices.pcm")
  let pcm = floats(try Data(contentsOf: fixture))
  // 0.5 s of silence, alba's 7.66 s to 10.22 s and marius's 10.92 s to 13.48 s summed at 0.7
  // each, 1.5 s of silence
  let both: [Float] = (0..<40_960).map { 0.7 * (pcm[122_560 + $0] + pcm[174_720 + $0]) }
  let mixed = [Float](repeating: 0, count: 8_000) + both + [Float](repeating: 0, count: 24_000)
  let fluid = try await Fluid(model: model)
  let dir = NSTemporaryDirectory() + "diarize-overlap-\(getpid())"
  defer { try? FileManager.default.removeItem(atPath: dir) }
  _ = try enrol("first", Array(pcm[8_000..<52_800]), in: dir, model: fluid)
  let engine = try Talkers(model: fluid, voices: voices(dir), floor: nil)
  var said: [[String: Any]] = []
  for at in 0..<mixed.count / 320 {
    said += try engine.frame(at * 20_000, Array(mixed[at * 320..<at * 320 + 320]), masked: false)
  }
  // 1.7 s of the 2.56 s named `first` and an integer together (measured 2026-11-27)
  let together = said.filter {
    let ids = ($0["talkers"] as! [[String: Any]]).map { "\($0["id"]!)" }
    return ids.contains("first") && ids.contains { Int($0) != nil }
  }
  .map { ($0["end"] as! Int) - ($0["start"] as! Int) }.reduce(0, +)
  #expect(together >= 1_500_000, "\(said)")
}

@Test(
  "the model's steps the same however the fixture is cut: 20 ms frames, 100 ms, 333 samples",
  .enabled(
    if: FileManager.default.fileExists(
      atPath: model + "/speaker-diarization/wespeaker_v2.mlmodelc"),
    "skipped: set DIARIZE_MODEL to the --model directory (engines/README.md)"))
func chunked() async throws {
  let fixture = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    .appending(path: "../../../fixtures/voices.pcm")
  let pcm = floats(try Data(contentsOf: fixture))
  let fluid = try await Fluid(model: model)
  var runs: [[[Float]]] = []
  for size in [320, 1_600, 333] {
    _ = try fluid.start([])
    runs.append(
      try stride(from: 0, to: pcm.count, by: size).flatMap {
        try fluid.feed(Array(pcm[$0..<min(pcm.count, $0 + size)]))
      })
  }
  let steps = pcm.count / 1_600
  for run in runs {
    // all but the steps still inside the model's right context
    #expect(run.count > steps - 12 && run.count <= steps, "\(run.count) of \(steps)")
    let pairs = zip(run, runs[0]).flatMap { zip($0, $1) }
    #expect(run.count == runs[0].count && pairs.allSatisfy { abs($0 - $1) < 1e-3 })
  }
}

import Fluid
import Foundation
import Testing

@testable import Engine

/// The `--model` directory under $DIARIZE_MODEL, or none.
let model = ProcessInfo.processInfo.environment["DIARIZE_MODEL", default: ""]

/// A fixture's samples, `engines/fixtures/<name>`.
func fixture(_ name: String) throws -> [Float] {
  floats(
    try Data(
      contentsOf: URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        .appending(path: "../../../fixtures/\(name)")))
}

/// What `engine` says over `pcm` fed a frame at a time.
func heard(_ engine: Talkers, _ pcm: [Float]) throws -> [[String: Any]] {
  try (0..<pcm.count / 320).flatMap {
    try engine.frame($0 * 20_000, Array(pcm[$0 * 320..<$0 * 320 + 320]), masked: false)
  }
}

/// The models one at a time: two diarizers run at once in one process labelled nothing as
/// enrolled (observed 2026-11-27 · two probes in parallel; each alone labelled as these tests do).
@Suite(
  .serialized,
  .enabled(
    if: FileManager.default.fileExists(
      atPath: model + "/speaker-diarization/wespeaker_v2.mlmodelc"),
    "skipped: set DIARIZE_MODEL to the --model directory (engines/README.md)"))
struct Models {
  @Test(
    "the two-voice fixture, alba enrolled from 14.5 s of other sentences: her label on both her phrases, scored, and every score of hers above every score of marius's as her"
  )
  func voices() async throws {
    // Pocket TTS: alba from 0.8 s to 3.0 s and 8.0 s to 10.1 s, marius from 3.8 s to 6.7 s and
    // 10.9 s to 13.0 s
    let pcm = try fixture("voices.pcm")
    let fluid = try await Fluid(model: model)
    let dir = NSTemporaryDirectory() + "diarize-fluid-\(getpid())"
    defer { try? FileManager.default.removeItem(atPath: dir) }
    // alba's 14.5 s of other sentences, so every phrase here is out of sample
    _ = try enrol("first", fixture("alba.pcm"), in: dir, model: fluid)
    let engine = try Talkers(model: fluid, voices: Engine.voices(dir), floor: nil)
    let said = try heard(engine, pcm)
    // each span's talkers with `first`'s score, by its middle, in a voice's phrase
    func within(_ from: Int, _ to: Int) -> [[String: Any]] {
      said.filter {
        (from..<to).contains((($0["start"] as! Int) + ($0["end"] as! Int)) / 2)
      }
      .flatMap { $0["talkers"] as! [[String: Any]] }
    }
    let albas = [(800_000, 3_000_000), (8_000_000, 10_100_000)]
    let mariuses = [(3_800_000, 6_700_000), (10_900_000, 13_000_000)]
    #expect(albas.allSatisfy { Set(within($0.0, $0.1).map { "\($0["id"]!)" }) == ["first"] })
    // each phrase scored whole by the engine's own rule too, whatever LS-EEND labelled it, so
    // the order is tested even where no span of his is labelled `first` and scored
    func scores(_ phrases: [(Int, Int)]) throws -> [Any] {
      try phrases.flatMap { phrase in
        try [engine.scored("first", phrase.0, phrase.1)]
          + within(phrase.0, phrase.1).filter { $0["id"] as? String == "first" }.map {
            $0["score"]!
          }
      }
    }
    let (hers, his) = (try scores(albas), try scores(mariuses).compactMap { $0 as? Double })
    #expect(hers.allSatisfy { $0 is Double } && his.count >= 2, "\(hers) \(his)")
    // order alone, as the protocol promises (measured 2026-11-27: hers 0.606 to 0.664, his
    // 0.292 to 0.423)
    #expect(hers.map { $0 as! Double }.min()! > his.max()!, "\(hers) \(his)")
  }

  @Test("overlap: alba's and marius's later phrases summed at 0.7, alba enrolled: both at once")
  func overlap() async throws {
    let pcm = try fixture("voices.pcm")
    // 0.5 s of silence, alba's 7.66 s to 10.22 s and marius's 10.92 s to 13.48 s summed at 0.7
    // each, 1.5 s of silence
    let both: [Float] = (0..<40_960).map { 0.7 * (pcm[122_560 + $0] + pcm[174_720 + $0]) }
    let mixed = [Float](repeating: 0, count: 8_000) + both + [Float](repeating: 0, count: 24_000)
    let fluid = try await Fluid(model: model)
    let dir = NSTemporaryDirectory() + "diarize-overlap-\(getpid())"
    defer { try? FileManager.default.removeItem(atPath: dir) }
    _ = try enrol("first", Array(pcm[8_000..<52_800]), in: dir, model: fluid)
    let said = try heard(Talkers(model: fluid, voices: Engine.voices(dir), floor: nil), mixed)
    let together = said.filter {
      let ids = ($0["talkers"] as! [[String: Any]]).map { "\($0["id"]!)" }
      return ids.contains("first") && ids.contains { Int($0) != nil }
    }
    .map { ($0["end"] as! Int) - ($0["start"] as! Int) }.reduce(0, +)
    // 1.7 s of the 2.56 s (measured 2026-11-27)
    #expect(together >= 1_500_000, "\(said)")
  }

  @Test("the steps the same however the fixture is cut: 20 ms frames, 100 ms, 333 samples")
  func chunked() async throws {
    let pcm = try fixture("voices.pcm")
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
}

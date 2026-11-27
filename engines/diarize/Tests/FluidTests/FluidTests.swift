import Fluid
import Foundation
import Testing

@testable import Engine

/// The `--model` directory under $DIARIZE_MODEL, or none.
let model = ProcessInfo.processInfo.environment["DIARIZE_MODEL", default: ""]

@Test(
  "the models over the two-voice fixture, its first voice enrolled from its first phrase: that voice's label on its later phrase, an integer on the other's",
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
  #expect(named(8_000_000, 10_100_000) == ["first"], "\(said)")
  #expect(named(3_800_000, 6_700_000) == ["1"], "\(said)")
  #expect(named(10_900_000, 13_000_000) == ["1"], "\(said)")
  let scores = said.flatMap {
    ($0["talkers"] as! [[String: Any]]).compactMap { $0["score"] as? Double }
  }
  #expect(!scores.isEmpty && scores.allSatisfy { $0 > 0.5 }, "\(scores)")
}

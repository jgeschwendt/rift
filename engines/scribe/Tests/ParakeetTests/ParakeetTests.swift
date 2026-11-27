import Foundation
import Parakeet
import Testing

@testable import Engine

/// The `--model` directory under $SCRIBE_MODEL, or none.
let model = ProcessInfo.processInfo.environment["SCRIBE_MODEL", default: ""]

@Test(
  "the model over the fixture, with and without a vocabulary: its phrase in one final",
  .enabled(
    if: FileManager.default.fileExists(atPath: model + "/parakeet-ultra/Encoder.mlmodelc"),
    "skipped: set SCRIBE_MODEL to the --model directory (engines/README.md)"))
func fixture() async throws {
  // Pocket TTS saying "Please turn on the kitchen lights.", with the endpoint's voice and turn
  // on it (0.832 s to 2.496 s) where they come live
  let fixture = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    .appending(path: "../../../fixtures/phrase.pcm")
  let pcm = try Data(contentsOf: fixture).withUnsafeBytes { Array($0.bindMemory(to: Int16.self)) }
  let terms = FileManager.default.temporaryDirectory.appending(path: "scribe-terms.txt")
  try "Kitchen\n".write(to: terms, atomically: true, encoding: .utf8)
  for vocabulary in [nil, terms.path] {
    let words = Words(model: try await Parakeet(model: model, terms: vocabulary), floor: nil)
    var said: [[String: Any]] = []
    for at in 0..<pcm.count / 320 {
      said += try await words.frame(
        at * 20_000, pcm[at * 320..<at * 320 + 320].map { Float($0) / 32_768 })
      if at == 48 { words.voiced(true, 832_000) }
      if at == 140 {
        words.voiced(false, 2_496_000)
        said += try await words.turn(2_496_000)
      }
    }
    let text = (said.first?["words"] as? [[String: Any]] ?? []).map { "\($0["text"]!)" }
    #expect(said.count == 1)
    #expect(
      text.joined(separator: " ").lowercased().contains("turn on the kitchen lights"), "\(text)")
  }
}

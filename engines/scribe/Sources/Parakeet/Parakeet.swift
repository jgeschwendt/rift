import Engine
import FluidAudio
import Foundation

/// Parakeet Ultra through FluidAudio, each span decoded whole, its words rescored toward a
/// vocabulary by the CTC 110M spotter when one is given.
public final class Parakeet: Model {
  let manager = AsrManager()
  var boost: VocabularyBoostingSession?

  /// Loads `parakeet-ultra/` under `model` and, with `terms`, `parakeet-ctc-110m-coreml/` beside
  /// it and the terms' file (engines/README.md); nothing is fetched. Then runs each once, so
  /// the first turn heard is not the one that pays for the load.
  public init(model: String, terms: String?) async throws {
    // FluidAudio's own lines to the unified log alone, so stderr holds the engine's, and from
    // warning up: under it, its debug lines carry transcript text
    AppLogger.mirrorsToConsole = false
    AppLogger.minimumLevel = .warning
    let dir = URL(fileURLWithPath: model)
    let ultra = try AsrModels.loadLocal(
      from: dir.appending(path: "parakeet-ultra"), version: .ultra)
    try await manager.loadModels(ultra)
    let silence = [Float](repeating: 0, count: 16_000)
    if let terms {
      let ctc = try await CtcModels.loadDirect(
        from: dir.appending(path: "parakeet-ctc-110m-coreml"))
      let listed = vocabulary(try String(contentsOfFile: terms, encoding: .utf8)).map {
        CustomVocabularyTerm(text: $0.term, aliases: $0.heard.isEmpty ? nil : $0.heard)
      }
      boost = try await VocabularyBoostingSession(
        vocabulary: CustomVocabularyContext(terms: listed), ctcModels: ctc)
      // the spotter runs only over a transcript with tokens
      let token = TokenTiming(token: "▁warm", tokenId: 0, startTime: 0, endTime: 1, confidence: 1)
      _ = await boost?.rescore(text: "warm", tokenTimings: [token], audioSamples: silence)
    }
    _ = try await decode(silence)
  }

  /// The words in `pcm`, rescored when a vocabulary is loaded, each timed by its tokens.
  public func decode(_ pcm: [Float]) async throws -> [Heard] {
    // FluidAudio refuses audio under 0.3 s; silence after the span hears nothing
    let padded = pcm + [Float](repeating: 0, count: max(0, 4_800 - pcm.count))
    var state = TdtDecoderState.make(decoderLayers: await manager.decoderLayerCount)
    let result = try await manager.transcribe(padded, decoderState: &state)
    let timings = result.tokenTimings ?? []
    let words = buildWordTimings(from: timings).map {
      Heard(text: $0.word, start: $0.startTime, end: $0.endTime)
    }
    guard
      let rescored = await boost?.rescore(
        text: result.text, tokenTimings: timings, audioSamples: padded),
      rescored.wasModified
    else { return words }
    return retime(words, as: rescored.text)
  }
}

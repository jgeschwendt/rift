import Engine
import FluidAudio
import Foundation

/// LS-EEND (DIHARD III, 100 ms steps) for who speaks when and WeSpeaker v2 for a voice's
/// embedding, both through FluidAudio.
public final class Fluid: Model {
  let diarizer: LSEENDDiarizer
  let speakers = DiarizerManager()
  /// The stream's 16 kHz to 8 kHz front end, its state kept across frames.
  var stream = Decimator()

  /// Loads `ls-eend/` and `speaker-diarization/` under `model` (engines/README.md); nothing is
  /// fetched. Then runs each once, so the first audio heard is not the one that pays for the load.
  public init(model: String) async throws {
    // FluidAudio's own lines to the unified log alone, so stderr holds the engine's
    AppLogger.mirrorsToConsole = false
    AppLogger.minimumLevel = .warning
    let dir = URL(fileURLWithPath: model)
    let eend = try LSEENDModel(
      modelURL: dir.appending(path: "ls-eend/optimized/dih3/100ms/ls_eend_dih3_100ms.mlmodelc"))
    // no segments kept and 10 s of predictions, so a stream of hours holds no more than that
    let timeline = DiarizerTimelineConfig(maxStoredFrames: 100, storeSegments: false)
    diarizer = try LSEENDDiarizer(model: eend, timelineConfig: timeline)
    guard diarizer.targetSampleRate == 8_000 else {
      throw Failure(reason: "LS-EEND wants \(diarizer.targetSampleRate ?? 0) Hz, not 8000")
    }
    let embedding = dir.appending(path: "speaker-diarization")
    speakers.initialize(
      models: try DiarizerModels.load(
        localSegmentationModel: embedding.appending(path: "pyannote_segmentation.mlmodelc"),
        localEmbeddingModel: embedding.appending(path: "wespeaker_v2.mlmodelc")))
    let noise = (0..<16_000).map { Float(($0 * 7_919) % 201 - 100) / 10_000 }
    _ = try embed(noise)
    _ = try start([])
    _ = try feed(noise)
  }

  public func start(_ voices: [(label: String, pcm: [Float])]) throws -> [Int: String] {
    diarizer.reset()
    stream = Decimator()
    var slots: [Int: String] = [:]
    for voice in voices {
      // the stream's front end, so a voice is enrolled from what the live audio becomes
      var enrolment = Decimator()
      let held = try diarizer.enrollSpeaker(
        withAudio: enrolment(voice.pcm), sourceSampleRate: nil, named: voice.label,
        overwritingAssignedSpeakerName: false)
      if let held { slots[held.index] = voice.label }
    }
    return slots
  }

  public func feed(_ pcm: [Float]) throws -> [[Float]] {
    let given = try diarizer.process(samples: stream(pcm), sourceSampleRate: nil)
    let (probs, width) = (given?.chunkResult.finalizedPredictions ?? [], diarizer.numSpeakers ?? 1)
    return stride(from: 0, to: probs.count, by: width).map { Array(probs[$0..<$0 + width]) }
  }

  public func embed(_ pcm: [Float]) throws -> [Float] {
    try speakers.extractSpeakerEmbedding(from: pcm)
  }
}

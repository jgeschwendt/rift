// swift-tools-version: 6.2
// scribe: rift's macOS words engine, Parakeet Ultra through FluidAudio, decoding each turn whole.
import PackageDescription

let package = Package(
  name: "scribe",
  platforms: [.macOS(.v15)],
  dependencies: [
    // the release the 2026-11-26 baseline measured; ASR alone, so no TTS, diarizer or
    // text-normalization binary is built or fetched
    .package(url: "https://github.com/FluidInference/FluidAudio.git", exact: "0.18.4", traits: [])
  ],
  targets: [
    // the protocol logic, apart from any model so its tests need no weights
    .target(name: "Engine"),
    .testTarget(name: "EngineTests", dependencies: ["Engine"]),
    .target(
      name: "Parakeet",
      dependencies: ["Engine", .product(name: "FluidAudio", package: "FluidAudio")]
    ),
    .testTarget(name: "ParakeetTests", dependencies: ["Engine", "Parakeet"]),
    .executableTarget(name: "scribe", dependencies: ["Engine", "Parakeet"]),
  ]
)

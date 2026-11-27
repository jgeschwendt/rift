// swift-tools-version: 6.2
// diarize: rift's macOS talkers engine, LS-EEND and WeSpeaker through FluidAudio.
import PackageDescription

let package = Package(
  name: "diarize",
  platforms: [.macOS(.v15)],
  dependencies: [
    // the release the 2026-11-26 baseline measured; the diarizer alone, so no TTS or
    // text-normalization binary is built or fetched
    .package(
      url: "https://github.com/FluidInference/FluidAudio.git", exact: "0.18.4",
      traits: ["Diarizer"])
  ],
  targets: [
    // the protocol logic, apart from any model so its tests need no weights
    .target(name: "Engine"),
    .testTarget(name: "EngineTests", dependencies: ["Engine"]),
    .target(
      name: "Fluid",
      dependencies: ["Engine", .product(name: "FluidAudio", package: "FluidAudio")]
    ),
    .testTarget(name: "FluidTests", dependencies: ["Engine", "Fluid"]),
    .executableTarget(name: "diarize", dependencies: ["Engine", "Fluid"]),
  ]
)

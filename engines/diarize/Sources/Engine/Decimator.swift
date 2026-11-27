import Foundation

/// 16 kHz to 8 kHz, LS-EEND's rate: a 65-tap Blackman-windowed sinc low-pass at 3.8 kHz, every
/// second sample kept, the last 64 inputs carried from one call to the next, so the output is the
/// same however the input is cut (FluidAudio's own resampler starts afresh at each call). It lags
/// its input by 32 samples, 2 ms.
public struct Decimator {
  static let taps: [Float] = {
    let (count, cutoff) = (65, 3_800.0 / 16_000)
    let raw = (0..<count).map { n -> Double in
      let x = Double(n - count / 2)
      let sinc = x == 0 ? 2 * cutoff : sin(2 * .pi * cutoff * x) / (.pi * x)
      let phase = 2 * Double.pi * Double(n) / Double(count - 1)
      return sinc * (0.42 - 0.5 * cos(phase) + 0.08 * cos(2 * phase))
    }
    let sum = raw.reduce(0, +)
    return raw.map { Float($0 / sum) }
  }()
  /// The inputs the next output reaches back over, the oldest first.
  var held = [Float](repeating: 0, count: taps.count - 1)

  public init() {}

  /// `pcm`, appended to what came before, as the 8 kHz samples it completes.
  public mutating func callAsFunction(_ pcm: [Float]) -> [Float] {
    held += pcm
    let reach = Self.taps.count - 1
    var (out, at) = ([Float](), reach)
    while at < held.count {
      out.append(zip(Self.taps, held[(at - reach)...at]).reduce(0) { $0 + $1.0 * $1.1 })
      at += 2
    }
    held.removeFirst(at - reach)
    return out
  }
}

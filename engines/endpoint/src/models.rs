//! The models behind [`Prob`] and [`Score`]: Silero VAD and Smart Turn v3.2, through
//! onnxruntime; Smart Turn's input, the Whisper log-mel of the turn's last 8 s, computed here.
#![expect(
    clippy::arithmetic_side_effects,
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    reason = "index and size math over fixed-size buffers: the FFT's bins, frames and padding"
)]

use std::f64::consts::PI;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::path::Path;
use std::sync::Arc;

use ort::session::Session;
use ort::value::{Tensor, TensorRef};
use rustfft::num_complex::Complex;
use rustfft::{Fft, FftPlanner};

use crate::turns::{CONTEXT, Prob, Score, WINDOW};

/// Samples of the last window Silero reads before each: its streaming context.
const LEAD: usize = 64;
/// The STFT's window and size (Whisper's `n_fft`).
const FFT: usize = 400;
/// The STFT's hop, 10 ms.
const HOP: usize = 160;
/// Mel bands.
const MELS: usize = 80;
/// Positive-frequency bins of the FFT.
const BINS: usize = FFT / 2 + 1;
/// Frames of features: one per hop over the context, the trailing one dropped.
const FRAMES: usize = CONTEXT / HOP;

/// Silero VAD v6.2, its LSTM state and context carried window to window.
#[derive(Debug)]
pub struct Silero {
    /// The last [`LEAD`] samples of the window before.
    lead: [f32; LEAD],
    /// The graph.
    session: Session,
    /// The recurrent state, `[2, 1, 128]`.
    state: Vec<f32>,
}

impl Silero {
    /// Loads the graph at `path`.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            clippy::single_call_fn,
            reason = "the model's loader, named for its type; the tests call it as often"
        )
    )]
    pub fn load(path: &Path) -> io::Result<Self> {
        Ok(Self {
            lead: [0.0f32; LEAD],
            session: open(path)?,
            state: vec![0.0f32; 256],
        })
    }
}

impl Prob for Silero {
    fn prob(&mut self, window: &[f32]) -> io::Result<f32> {
        let input = [self.lead.as_slice(), window].concat();
        let shape = ([1usize, LEAD + WINDOW], input.as_slice());
        let outputs = self
            .session
            .run(ort::inputs![
                "input" => TensorRef::from_array_view(shape).map_err(io::Error::other)?,
                "state" => TensorRef::from_array_view(([2usize, 1, 128], self.state.as_slice())).map_err(io::Error::other)?,
                "sr" => Tensor::from_array(([1usize], vec![16_000i64])).map_err(io::Error::other)?,
            ])
            .map_err(io::Error::other)?;
        let (_, next) = outputs["stateN"]
            .try_extract_tensor::<f32>()
            .map_err(io::Error::other)?;
        self.state = next.to_vec();
        let (_, speech) = outputs["output"]
            .try_extract_tensor::<f32>()
            .map_err(io::Error::other)?;
        self.lead.copy_from_slice(&window[WINDOW - LEAD..]);
        Ok(speech.first().copied().unwrap_or_default())
    }

    fn reset(&mut self) {
        self.lead = [0.0f32; LEAD];
        self.state.fill(0.0f32);
    }
}

/// Smart Turn v3.2 (CPU, int8): a Whisper-Tiny encoder whose one output is already a probability.
pub struct SmartTurn {
    /// The FFT plan.
    fft: Arc<dyn Fft<f32>>,
    /// The Slaney mel filterbank, `[MELS][BINS]`.
    filters: Vec<[f32; BINS]>,
    /// The graph.
    session: Session,
    /// The periodic Hann window.
    window: Vec<f32>,
}

impl Debug for SmartTurn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SmartTurn").finish_non_exhaustive()
    }
}

impl SmartTurn {
    /// `WhisperFeatureExtractor(chunk_length=8, do_normalize=True)`'s features, `[MELS][FRAMES]`:
    /// the last 8 s normalized over the real samples and zero-padded after them, reflect-padded,
    /// the power STFT, the filterbank, `log10` clamped to 8 under the peak, then `(x + 4) / 4`.
    fn features(&self, tail: &[f32]) -> Vec<f32> {
        let real = &tail[tail.len().saturating_sub(CONTEXT)..];
        // in f64, as numpy's pairwise float32 sums land, near enough to move no score
        let count = real.len().max(1) as f64;
        let mean = real.iter().map(|&sample| f64::from(sample)).sum::<f64>() / count;
        let var = real
            .iter()
            .map(|&sample| (f64::from(sample) - mean).powi(2))
            .sum::<f64>()
            / count;
        let scale = 1.0f64 / (var + 1e-7f64).sqrt();
        let mut wave = vec![0.0f32; CONTEXT];
        for (cell, &sample) in wave.iter_mut().zip(real) {
            *cell = ((f64::from(sample) - mean) * scale) as f32;
        }
        let pad = FFT / 2;
        // numpy's reflect: mirrored about each edge sample, the edge itself not repeated
        let padded: Vec<f32> = (0..CONTEXT + FFT)
            .map(|at| wave[at.abs_diff(pad).min(2 * (CONTEXT - 1) + pad - at.max(pad))])
            .collect();
        let mut logs = vec![0.0f32; MELS * FRAMES];
        let mut spectrum = vec![Complex::new(0.0f32, 0.0f32); FFT];
        for frame in 0..FRAMES {
            for (cell, (&sample, &weight)) in spectrum
                .iter_mut()
                .zip(padded[frame * HOP..].iter().zip(&self.window))
            {
                *cell = Complex::new(sample * weight, 0.0f32);
            }
            self.fft.process(&mut spectrum);
            for (band, filter) in self.filters.iter().enumerate() {
                let energy: f32 = filter
                    .iter()
                    .zip(&spectrum)
                    .map(|(weight, bin)| weight * bin.norm_sqr())
                    .sum();
                logs[band * FRAMES + frame] = energy.max(1e-10f32).log10();
            }
        }
        let floor = logs
            .iter()
            .fold(f32::NEG_INFINITY, |held, &value| held.max(value))
            - 8.0f32;
        logs.iter()
            .map(|&value| (value.max(floor) + 4.0f32) / 4.0f32)
            .collect()
    }

    /// Loads the graph at `path` and lays out the front end.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            clippy::single_call_fn,
            reason = "the model's loader, named for its type; the tests call it as often"
        )
    )]
    pub fn load(path: &Path) -> io::Result<Self> {
        // the Slaney mel scale: linear to 1 kHz, logarithmic above
        let mel = |hz: f64| {
            if hz < 1000.0f64 {
                hz * 3.0f64 / 200.0f64
            } else {
                15.0f64 + (hz / 1000.0f64).ln() * 27.0f64 / 6.4f64.ln()
            }
        };
        let hz = |scale: f64| {
            if scale < 15.0f64 {
                scale * 200.0f64 / 3.0f64
            } else {
                1000.0f64 * ((scale - 15.0f64) * 6.4f64.ln() / 27.0f64).exp()
            }
        };
        let top = mel(8000.0f64);
        let edges: Vec<f64> = (0..MELS + 2)
            .map(|at| hz(top * at as f64 / (MELS + 1) as f64))
            .collect();
        let filters = edges
            .iter()
            .zip(edges.iter().skip(1))
            .zip(edges.iter().skip(2))
            .map(|((&low, &mid), &high)| {
                let mut row = [0.0f32; BINS];
                for (bin, cell) in row.iter_mut().enumerate() {
                    let freq = bin as f64 * 40.0f64;
                    let rise = (freq - low) / (mid - low);
                    let fall = (high - freq) / (high - mid);
                    *cell = (rise.min(fall).max(0.0f64) * 2.0f64 / (high - low)) as f32;
                }
                row
            })
            .collect();
        Ok(Self {
            fft: FftPlanner::new().plan_fft_forward(FFT),
            filters,
            session: open(path)?,
            window: (0..FFT)
                .map(|at| {
                    0.5f64.mul_add(-(2.0f64 * PI * at as f64 / FFT as f64).cos(), 0.5f64) as f32
                })
                .collect(),
        })
    }
}

impl Score for SmartTurn {
    fn score(&mut self, tail: &[f32]) -> io::Result<f32> {
        let features = self.features(tail);
        let input = TensorRef::from_array_view(([1usize, MELS, FRAMES], features.as_slice()))
            .map_err(io::Error::other)?;
        let outputs = self
            .session
            .run(ort::inputs!["input_features" => input])
            .map_err(io::Error::other)?;
        let (_, score) = outputs["logits"]
            .try_extract_tensor::<f32>()
            .map_err(io::Error::other)?;
        Ok(score
            .first()
            .copied()
            .unwrap_or_default()
            .clamp(0.0f32, 1.0f32))
    }
}

/// An onnxruntime session over the graph at `path`, its failure naming the path.
fn open(path: &Path) -> io::Result<Session> {
    let failed = |cause: &dyn Display| io::Error::other(format!("{}: {cause}", path.display()));
    Session::builder()
        .map_err(|cause| failed(&cause))?
        .commit_from_file(path)
        .map_err(|cause| failed(&cause))
}

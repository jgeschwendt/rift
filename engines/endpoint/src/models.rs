//! The models behind [`Prob`] and [`Score`]: Silero VAD and Smart Turn v3.2, through
//! onnxruntime; Smart Turn's input, the Whisper log-mel of the turn's last 8 s, computed here.

use std::f64::consts::PI;
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
    pub fn load(path: &Path) -> io::Result<Self> {
        Ok(Self {
            lead: [0.0; LEAD],
            session: open(path)?,
            state: vec![0.0; 256],
        })
    }
}

impl Prob for Silero {
    fn prob(&mut self, window: &[f32]) -> io::Result<f32> {
        let input = [&self.lead[..], window].concat();
        let shape = ([1_usize, LEAD + WINDOW], &input[..]);
        let outputs = self
            .session
            .run(ort::inputs![
                "input" => TensorRef::from_array_view(shape).map_err(io::Error::other)?,
                "state" => TensorRef::from_array_view(([2_usize, 1, 128], &self.state[..])).map_err(io::Error::other)?,
                "sr" => Tensor::from_array(([1_usize], vec![16_000_i64])).map_err(io::Error::other)?,
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
        self.lead = [0.0; LEAD];
        self.state.fill(0.0);
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

impl std::fmt::Debug for SmartTurn {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("SmartTurn").finish_non_exhaustive()
    }
}

impl SmartTurn {
    /// Loads the graph at `path` and lays out the front end.
    pub fn load(path: &Path) -> io::Result<Self> {
        // the Slaney mel scale: linear to 1 kHz, logarithmic above
        let mel = |hz: f64| {
            if hz < 1000.0 {
                hz * 3.0 / 200.0
            } else {
                15.0 + (hz / 1000.0).ln() * 27.0 / 6.4_f64.ln()
            }
        };
        let hz = |mel: f64| {
            if mel < 15.0 {
                mel * 200.0 / 3.0
            } else {
                1000.0 * ((mel - 15.0) * 6.4_f64.ln() / 27.0).exp()
            }
        };
        let top = mel(8000.0);
        let edges: Vec<f64> = (0..MELS + 2)
            .map(|at| hz(top * at as f64 / (MELS + 1) as f64))
            .collect();
        let filters = edges
            .windows(3)
            .map(|edge| {
                let mut row = [0.0; BINS];
                for (bin, cell) in row.iter_mut().enumerate() {
                    let freq = bin as f64 * 40.0;
                    let rise = (freq - edge[0]) / (edge[1] - edge[0]);
                    let fall = (edge[2] - freq) / (edge[2] - edge[1]);
                    *cell = (rise.min(fall).max(0.0) * 2.0 / (edge[2] - edge[0])) as f32;
                }
                row
            })
            .collect();
        Ok(Self {
            fft: FftPlanner::new().plan_fft_forward(FFT),
            filters,
            session: open(path)?,
            window: (0..FFT)
                .map(|at| 0.5f64.mul_add(-(2.0 * PI * at as f64 / FFT as f64).cos(), 0.5) as f32)
                .collect(),
        })
    }

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
        let scale = 1.0 / (var + 1e-7).sqrt();
        let mut wave = vec![0.0_f32; CONTEXT];
        for (cell, &sample) in wave.iter_mut().zip(real) {
            *cell = ((f64::from(sample) - mean) * scale) as f32;
        }
        let pad = FFT / 2;
        // numpy's reflect: mirrored about each edge sample, the edge itself not repeated
        let padded: Vec<f32> = (0..CONTEXT + FFT)
            .map(|at| wave[at.abs_diff(pad).min(2 * (CONTEXT - 1) + pad - at.max(pad))])
            .collect();
        let mut logs = vec![0.0_f32; MELS * FRAMES];
        let mut spectrum = vec![Complex::new(0.0, 0.0); FFT];
        for frame in 0..FRAMES {
            for (cell, (&sample, &weight)) in spectrum
                .iter_mut()
                .zip(padded[frame * HOP..].iter().zip(&self.window))
            {
                *cell = Complex::new(sample * weight, 0.0);
            }
            self.fft.process(&mut spectrum);
            for (band, filter) in self.filters.iter().enumerate() {
                let energy: f32 = filter
                    .iter()
                    .zip(&spectrum)
                    .map(|(weight, bin)| weight * bin.norm_sqr())
                    .sum();
                logs[band * FRAMES + frame] = energy.max(1e-10).log10();
            }
        }
        let floor = logs
            .iter()
            .fold(f32::NEG_INFINITY, |held, &value| held.max(value))
            - 8.0;
        logs.iter()
            .map(|&value| (value.max(floor) + 4.0) / 4.0)
            .collect()
    }
}

impl Score for SmartTurn {
    fn score(&mut self, tail: &[f32]) -> io::Result<f32> {
        let features = self.features(tail);
        let input = TensorRef::from_array_view(([1_usize, MELS, FRAMES], &features[..]))
            .map_err(io::Error::other)?;
        let outputs = self
            .session
            .run(ort::inputs!["input_features" => input])
            .map_err(io::Error::other)?;
        let (_, score) = outputs["logits"]
            .try_extract_tensor::<f32>()
            .map_err(io::Error::other)?;
        Ok(score.first().copied().unwrap_or_default().clamp(0.0, 1.0))
    }
}

/// An onnxruntime session over the graph at `path`, its failure naming the path.
fn open(path: &Path) -> io::Result<Session> {
    let failed =
        |cause: &dyn std::fmt::Display| io::Error::other(format!("{}: {cause}", path.display()));
    Session::builder()
        .map_err(|cause| failed(&cause))?
        .commit_from_file(path)
        .map_err(|cause| failed(&cause))
}

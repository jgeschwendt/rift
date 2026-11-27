//! Pocket TTS behind [`Voice`], through ptts on xn's CPU backend: the checkpoint without voice
//! cloning, one preset voice's embedding prompted once at load and its state reused per piece.

use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::path::Path;
use std::sync::Arc;

use ptts::Tokenizer;
use ptts::flow_lm::Rng;
use ptts::tts_model::{
    TTSConfig, TTSModel, TTSState, prepare_text_prompt, split_into_best_sentences,
};
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand_distr::{Distribution as _, Normal};
use xn::Tensor;
use xn::nn::VB;

use crate::speak::Voice;

/// The checkpoint's Hugging Face repo and revision, its snapshot the default `--pocket`.
pub const DIR: [&str; 2] = [
    "kyutai/pocket-tts-without-voice-cloning",
    "00eac05ed3d16bdc3f6b5d598874019c34a89214",
];
/// The checkpoint's file.
const WEIGHTS: &str = "tts_b6369a24.safetensors";
/// The sampling temperature, the reference CLI's.
const TEMPERATURE: f32 = 0.7;
/// The flow model's sequence budget per piece: the voice's prompt, the text and its frames.
const BUDGET: usize = 1024;
/// Tokens per piece at most, whole sentences joined up to it, a longer sentence alone: each
/// piece starts afresh, so its joins are the voice's own pauses. Moonshine heard two paragraphs
/// as written up to 118 tokens a piece, then repeated words at 167 and dropped them at 203
/// (observed 2026-11-27); ptts's default is 50.
const PIECE: usize = 120;
/// The audio decoder's context, the reference CLI's.
const CONTEXT: usize = 250;

/// The model on xn's CPU device, unquantized.
type Backend = xn::Unquantized<f32, xn::CpuDevice>;

/// The latents' noise: a seeded normal of variance the temperature.
struct Noise(StdRng, Normal<f32>);

impl Rng for Noise {
    fn sample(&mut self) -> f32 {
        self.1.sample(&mut self.0)
    }
}

/// Pocket TTS with one voice.
pub struct Pocket {
    /// The latent's width.
    latent: usize,
    /// The model.
    model: TTSModel<Backend>,
    /// The noise.
    noise: Noise,
    /// The budget the voice's prompt leaves a piece's text and frames.
    room: usize,
    /// The flow model's state with the voice prompted.
    voiced: TTSState<Backend>,
    /// The tokenizer, for cutting long text.
    words: Words,
}

impl Debug for Pocket {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pocket").finish_non_exhaustive()
    }
}

impl Pocket {
    /// Loads the checkpoint in `dir` with the preset `voice` (`embeddings/<voice>.safetensors`),
    /// then speaks one short sentence, so the first say pays no first-run stall.
    #[inline]
    pub fn load(dir: &Path, voice: &str) -> io::Result<Self> {
        let words = Words(Arc::new(
            tokenizers::Tokenizer::from_file(dir.join("tokenizer.json")).map_err(failed)?,
        ));
        let cfg = TTSConfig::v202601(TEMPERATURE);
        let weights =
            VB::load_with_key_map(&[dir.join(WEIGHTS)], xn::CPU, rename).map_err(failed)?;
        let model: TTSModel<Backend> =
            TTSModel::load(&weights.root(), Box::new(words.clone()), &cfg).map_err(failed)?;
        let embedding = VB::load(
            &[dir.join(format!("embeddings/{voice}.safetensors"))],
            xn::CPU,
        )
        .map_err(failed)?;
        let key = embedding
            .tensor_names()
            .first()
            .map(ToString::to_string)
            .unwrap_or_default();
        let dims = embedding
            .shape(&key)
            .map(|shape| shape.dims().to_vec())
            .unwrap_or_default();
        let raw: Tensor<f32, xn::CpuDevice> =
            embedding.tensor(&key, dims.clone()).map_err(failed)?;
        // a voice file holds `[T, dim]` or `[1, T, dim]`
        let prompt = match *dims.as_slice() {
            [frames, width] => raw.reshape((1, frames, width)).map_err(failed)?,
            _ => raw,
        };
        let mut voiced = model.init_flow_lm_state(1, BUDGET).map_err(failed)?;
        model.prompt_audio(&mut voiced, &prompt).map_err(failed)?;
        let noise = Noise(
            StdRng::seed_from_u64(4_242_424_242),
            Normal::new(0.0, TEMPERATURE.sqrt()).map_err(failed)?,
        );
        let mut pocket = Self {
            latent: cfg.flow_lm.ldim,
            model,
            noise,
            room: BUDGET.saturating_sub(dims.iter().rev().nth(1).copied().unwrap_or_default()),
            voiced,
            words,
        };
        pocket.speak("Ready.", &mut |_| Ok(true))?;
        Ok(pocket)
    }
}

impl Voice for Pocket {
    #[inline]
    fn rate(&self) -> u32 {
        self.model.sample_rate() as u32
    }

    #[inline]
    fn speak(
        &mut self,
        text: &str,
        give: &mut dyn FnMut(&[f32]) -> io::Result<bool>,
    ) -> io::Result<()> {
        for piece in split_into_best_sentences(&self.words, text, Some(PIECE)).map_err(failed)? {
            let (prompt, after) = prepare_text_prompt(&piece);
            let tokens = self
                .model
                .flow_lm
                .conditioner
                .tokenize(&prompt)
                .map_err(failed)?;
            let mut state = self.voiced.clone();
            let mut decoder = self.model.init_mimi_state(1, CONTEXT).map_err(failed)?;
            self.model
                .prompt_text(&mut state, &tokens)
                .map_err(failed)?;
            // the start of speech: a latent of NaNs
            let mut last =
                Tensor::from_vec(vec![f32::NAN; self.latent], (1, 1, self.latent), &xn::CPU)
                    .map_err(failed)?;
            let mut left: Option<usize> = None;
            // the reference CLI's cap on frames per piece, inside what the budget has left: a
            // sentence past it is one piece still, cut short rather than overflowing the state
            let cap = ((tokens.len() as f64 / 3.0 + 2.0) * 12.5).ceil() as usize;
            for _ in 0..cap.min(self.room.saturating_sub(tokens.len())) {
                let (latent, ended) = self
                    .model
                    .generate_step(&mut state, &last, &mut self.noise)
                    .map_err(failed)?;
                let audio = self
                    .model
                    .decode_latent(&latent, &mut decoder)
                    .map_err(failed)?;
                if !give(&audio.to_vec().map_err(failed)?)? {
                    return Ok(());
                }
                // past the end of speech, a few frames more let the voice fall away
                left = left
                    .map(|frames| frames.saturating_sub(1))
                    .or_else(|| ended.then_some(after));
                if left == Some(0) {
                    break;
                }
                last = latent;
            }
        }
        Ok(())
    }
}

/// The tokenizer, `tokenizer.json`, as ptts reads one.
#[derive(Clone)]
struct Words(Arc<tokenizers::Tokenizer>);

impl Tokenizer for Words {
    fn decode(&self, tokens: &[u32]) -> xn::Result<String> {
        self.0.decode(tokens, false).map_err(xn::Error::wrap)
    }

    fn encode(&self, text: &str) -> xn::Result<Vec<u32>> {
        Ok(self
            .0
            .encode(text, false)
            .map_err(xn::Error::wrap)?
            .get_ids()
            .to_vec())
    }
}

/// An xn error as an I/O one.
fn failed(cause: impl Display) -> io::Error {
    io::Error::other(cause.to_string())
}

/// The checkpoint's names as ptts's modules name them, the tensors it never reads dropped.
#[expect(
    clippy::single_call_fn,
    reason = "the key map the loader hands xn, named for what it renames"
)]
fn rename(name: &str) -> Option<String> {
    let unread = ["flow.w_s_t", "quantizer.vq", "quantizer.logvar_proj"];
    (!unread.iter().any(|part| name.contains(part))).then(|| {
        [
            (
                "flow_lm.condition_provider.conditioners.speaker_wavs.output_proj.weight",
                "flow_lm.speaker_proj_weight",
            ),
            (
                "flow_lm.condition_provider.conditioners.transcript_in_segment.",
                "flow_lm.conditioner.",
            ),
            ("flow_lm.backbone.", "flow_lm.transformer."),
            ("flow_lm.flow.", "flow_lm.flow_net."),
            ("mimi.model.", "mimi."),
        ]
        .iter()
        .fold(name.to_owned(), |held, pair| held.replace(pair.0, pair.1))
    })
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "a test asserts, and propagates its setup with ?"
)]
mod tests {
    use std::io;

    use ptts::tts_model::{prepare_text_prompt, split_into_best_sentences};

    use super::{DIR, PIECE, Pocket, failed};
    use crate::speak::Voice as _;

    /// A sentence of 60 words and 11 commas.
    const LONG: &str = "When the morning train finally pulled into the station, a little after \
        seven, the platform was crowded with commuters, students carrying heavy backpacks, and a \
        few tired travelers who had been waiting since midnight, so the conductor, who had seen \
        this many times before, simply smiled, opened every door at once, and let the whole \
        noisy crowd pour in together.";

    /// The model speaks a long sentence whole at 24 kHz in 80 ms chunks, in one piece and ended
    /// by the model before the frame cap, and stops when told. Skipped, saying so, without the
    /// weights.
    #[test]
    fn speaks() -> io::Result<()> {
        let dir = link::hub(DIR[0], DIR[1]);
        if !dir.join("tokenizer.json").is_file() {
            eprintln!("skipped: no {} (engines/README.md)", dir.display());
            return Ok(());
        }
        let mut pocket = Pocket::load(&dir, "alba")?;
        let mut chunks = Vec::new();
        pocket.speak(LONG, &mut |chunk| {
            chunks.push(chunk.len());
            Ok(true)
        })?;
        let pieces = split_into_best_sentences(&pocket.words, LONG, Some(PIECE)).map_err(failed)?;
        let tokens = (pocket.model.flow_lm.conditioner)
            .tokenize(&prepare_text_prompt(LONG).0)
            .map_err(failed)?
            .len();
        assert_eq!(pieces.len(), 1, "one piece");
        assert!(
            chunks.len() > 150
                && chunks.len() < ((tokens as f64 / 3.0f64 + 2.0f64) * 12.5f64) as usize,
            "over 12 s, ended before the cap: {} chunks, {tokens} tokens",
            chunks.len()
        );
        let mut stopped = 0u8;
        pocket.speak("Hello there, how are you?", &mut |_| {
            stopped = stopped.saturating_add(1);
            Ok(false)
        })?;
        assert_eq!(pocket.rate(), 24_000, "its rate");
        assert!(chunks.iter().all(|&len| len == 1920), "in 80 ms chunks");
        assert_eq!(stopped, 1, "stopped at once");
        Ok(())
    }
}

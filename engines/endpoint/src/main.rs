//! `endpoint <track> [--bar <score>] [--ceiling-ms <ms>] [--silero <file>] [--smart-turn <file>]`:
//! rift's reference turn engine on the ear (or clean) track `<track>`. Silero VAD opens a voice
//! and closes it after 256 ms under its offset; Smart Turn scores the turn at each close, over
//! `--bar` (default 0.5) a turn, else held for the next voice up to `--ceiling-ms` (default
//! 3000) of silence. Each model's file defaults to its pinned snapshot in the Hugging Face cache
//! (engines/README.md).

mod models;
mod turns;

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use link::{flag, hub};

use crate::models::{Silero, SmartTurn};
use crate::turns::Endpoint;

/// Silero VAD v6.2's repo, revision and file.
const SILERO: [&str; 3] = [
    "Tinysoft/silero-vad-6.2",
    "3034960e5d3beb943b860ec0ea1a857bdae4ae96",
    "silero_vad.onnx",
];
/// Smart Turn v3.2's, its CPU build.
const SMART_TURN: [&str; 3] = [
    "pipecat-ai/smart-turn-v3",
    "f766f81d3cfdf7737ac64aad813d91bbfd56bf93",
    "smart-turn-v3.2-cpu.onnx",
];

/// The model file `--<name>` names, else `pin`'s snapshot in the hub cache.
fn model(args: &[String], name: &str, [repo, revision, file]: [&str; 3]) -> PathBuf {
    flag(args, name).map_or_else(|| hub(repo, revision).join(file), PathBuf::from)
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let bar = flag(&args, "bar").unwrap_or("0.5").parse::<f32>();
    let ceiling = flag(&args, "ceiling-ms").unwrap_or("3000").parse::<u64>();
    let (Some(track), Ok(bar), Ok(ceiling)) = (
        args.first().filter(|held| !held.starts_with('-')),
        bar,
        ceiling,
    ) else {
        eprintln!(
            "usage: endpoint <track> [--bar <score>] [--ceiling-ms <ms>] [--silero <file>] [--smart-turn <file>]"
        );
        return ExitCode::from(2);
    };
    let loaded = Silero::load(&model(&args, "silero", SILERO)).and_then(|prob| {
        Ok((
            prob,
            SmartTurn::load(&model(&args, "smart-turn", SMART_TURN))?,
        ))
    });
    let (mut prob, mut score) = match loaded {
        Ok(held) => held,
        Err(cause) => {
            eprintln!("endpoint: {cause}; the models' download is in engines/README.md");
            return ExitCode::FAILURE;
        }
    };
    let mut through = None;
    link::stand("endpoint", || {
        let endpoint = Endpoint::new(&mut prob, &mut score, bar, ceiling * 1000);
        turns::session(&link::socket(), track, &mut through, endpoint)
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, io};

    use serde_json::Value;

    use super::{SILERO, SMART_TURN, model};
    use crate::models::{Silero, SmartTurn};
    use crate::turns::Endpoint;

    /// The models over the fixture (engines/fixtures/phrase.pcm, Pocket TTS's voice saying "So
    /// I was thinking that we..." then, after a pause, "could go for a walk."): voice on and off
    /// alternating from on, and one turn, from the first voice's start to the last one's end
    /// (Smart Turn holds both closes under the bar, observed 2026-11-26 · 0.008 and 0.034, so
    /// the ceiling closes it). Skipped, saying so, without the models.
    #[test]
    fn fixture() -> io::Result<()> {
        let (silero, smart) = (model(&[], "", SILERO), model(&[], "", SMART_TURN));
        if !silero.is_file() || !smart.is_file() {
            eprintln!(
                "skipped: no {} or {} (engines/README.md)",
                silero.display(),
                smart.display()
            );
            return Ok(());
        }
        let (mut prob, mut score) = (Silero::load(&silero)?, SmartTurn::load(&smart)?);
        let mut endpoint = Endpoint::new(&mut prob, &mut score, 0.5, 3_000_000);
        let pcm = fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../fixtures/phrase.pcm"
        ))?;
        let mut said = Vec::new();
        for (at, frame) in pcm.chunks_exact(640).enumerate() {
            let samples: Vec<f32> = frame
                .chunks_exact(2)
                .map(|pair| f32::from(i16::from_le_bytes([pair[0], pair[1]])) / 32768.0)
                .collect();
            said.extend(endpoint.frame(at as u64 * 20_000, &samples)?);
        }
        eprintln!(
            "{}",
            said.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        );
        let voices: Vec<&Value> = said
            .iter()
            .filter(|report| report["op"] == "voice")
            .collect();
        let turns: Vec<&Value> = said
            .iter()
            .filter(|report| report["op"] == "turn")
            .collect();
        assert!(
            voices.len() >= 2 && voices.len().is_multiple_of(2),
            "voice on and off in pairs"
        );
        assert!(
            voices
                .iter()
                .enumerate()
                .all(|(at, voice)| voice["on"] == at.is_multiple_of(2)),
            "from on"
        );
        assert_eq!(turns.len(), 1, "one turn over the phrase and its pause");
        assert_eq!(
            (&turns[0]["start"], &turns[0]["end"]),
            (&voices[0]["start"], &voices[voices.len() - 1]["start"]),
            "from the first voice to the last"
        );
        Ok(())
    }
}

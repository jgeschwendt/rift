//! `cargo run --release --example fixture -- <out.pcm> <text>…`: the engines' synthetic fixture,
//! Pocket TTS's alba saying each text, 0.5 s of silence first, 0.7 s between, 4 s last (past a
//! held turn's 3 s ceiling), as 16 kHz 16-bit mono little-endian PCM, the file a file-fed ear
//! plays. No recording of a person.
#![expect(
    unused_crate_dependencies,
    reason = "the library's, which the example reaches through it"
)]

use std::{env, fs, io};

use link::hub;
use read_aloud::pocket::{DIR, Pocket};
use read_aloud::speak::Voice;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut pocket = Pocket::load(&hub(DIR[0], DIR[1]), "alba")?;
    let mut said = vec![0.0; 12_000];
    for (at, text) in args.iter().skip(1).enumerate() {
        if at > 0 {
            said.resize(said.len() + 16_800, 0.0);
        }
        pocket.speak(text, &mut |chunk| {
            said.extend_from_slice(chunk);
            Ok(true)
        })?;
    }
    said.resize(said.len() + 96_000, 0.0);
    // 24 kHz to 16 kHz: each output sample interpolated between its two neighbours
    let pcm: Vec<u8> = (0..said.len() * 2 / 3)
        .map(|at| {
            let (whole, part) = (at * 3 / 2, if at % 2 == 1 { 0.5 } else { 0.0 });
            let next = said.get(whole + 1).copied().unwrap_or_default();
            said[whole].mul_add(1.0 - part, next * part)
        })
        .flat_map(|sample| ((sample.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes())
        .collect();
    fs::write(args.first().map_or("fixture.pcm", String::as_str), pcm)
}

//! `read-aloud <track> [--voice <name>] [--pocket <dir>]`: rift's reference speech engine on the
//! mouth `<track>`. Pocket TTS speaks each say as its text comes, all that has come up to its
//! last sentence end at once, in the preset voice `--voice` (default alba), its 24 kHz audio
//! given as it is made; `--pocket` defaults to the checkpoint's pinned snapshot in the Hugging
//! Face cache (engines/README.md).
#![expect(
    unused_crate_dependencies,
    reason = "the library's, which the binary reaches through it"
)]

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use link::{flag, hub};
use read_aloud::pocket::{DIR, Pocket};
use read_aloud::speak;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some(track) = args.first().filter(|held| !held.starts_with('-')) else {
        eprintln!("usage: read-aloud <track> [--voice <name>] [--pocket <dir>]");
        return ExitCode::from(2);
    };
    let dir = flag(&args, "pocket").map_or_else(|| hub(DIR[0], DIR[1]), PathBuf::from);
    let mut voice = match Pocket::load(&dir, flag(&args, "voice").unwrap_or("alba")) {
        Ok(voice) => voice,
        Err(cause) => {
            eprintln!(
                "read-aloud: {}: {cause}; the download is in engines/README.md",
                dir.display()
            );
            return ExitCode::FAILURE;
        }
    };
    link::stand("read-aloud", &mut || {
        speak::session(&link::socket(), track, &mut voice)
    })
}

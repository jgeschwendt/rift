//! `rift engine run tail <ear> [--tail-ms <ms>]`, the reference echo engine
//! (docs/index.html#engines): each mouth run masked from its start to its end plus the tail, the
//! ear ruled up to the last frame heard while no play is open and up to the earliest open play's
//! last run end while one is, so no frame a run may echo into passes before its mask.

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::net::UnixStream;

/// Reads what the daemon tells `reader`'s engine until the connection ends, masking `tail`
/// microseconds past each run; the replies, which carry nothing it needs, pass unread.
#[expect(
    clippy::single_call_fn,
    reason = "the engine's entry: main calls it once"
)]
pub fn run(mut reader: BufReader<UnixStream>, tail: u64) {
    // where the last frame heard ends, the last mask's end, the ruling, each open play's rate and
    // its last run's end
    let (mut heard, mut masked, mut ruled) = (0u64, 0u64, 0u64);
    let mut open: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap_or_default() > 0 {
        let told: Value = serde_json::from_str(&line).unwrap_or_default();
        line.clear();
        let at = |key: &str| told[key].as_u64().unwrap_or_default();
        let mut said = Vec::new();
        if told["pcm"].is_string() {
            heard = at("t").saturating_add(20_000);
        }
        if told["rate"].is_u64() {
            let _: Option<(u64, u64)> = open.insert(at("play"), (at("rate"), 0));
        }
        if let Some(entry) = open
            .get_mut(&at("play"))
            .filter(|_| told["samples"].is_u64())
        {
            let lasted = at("samples").saturating_mul(1_000_000).checked_div(entry.0);
            entry.1 = at("start").saturating_add(lasted.unwrap_or_default());
            let (from, to) = (
                at("start").max(masked).max(ruled),
                entry.1.saturating_add(tail),
            );
            if to > from {
                said.push(json!({"end": to, "op": "mask", "start": from}));
                masked = to;
            }
        }
        if told["played"].is_u64() {
            let _: Option<(u64, u64)> = open.remove(&at("played"));
        }
        let to = (open.values().map(|entry| entry.1).min()).map_or(heard, |end| end.min(heard));
        if to > ruled {
            said.push(json!({"op": "ruled", "to": to}));
            ruled = to;
        }
        // a write the connection's end beat is no matter: the next read ends the loop
        for report in said {
            drop(writeln!(reader.get_mut(), "{report}"));
        }
    }
}

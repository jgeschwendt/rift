//! `rift engine run tail <ear> [--tail-ms <ms>]`, the reference echo engine
//! (docs/index.html#engines). A mask goes on at a play's first run and off once the ear is heard
//! past the last run's end plus the tail, `GUARD` more while a play is open, so a play sounding
//! without a longer gap is two events. It rules up to the last frame heard while no play is open,
//! up to the mask's planned off while one is on (the frames past it held for a run reported
//! late), and `GUARD` behind it while a play is open unmasked.

use crate::FORESIGHT;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::net::UnixStream;

/// How far behind the last frame heard the ruling stays while a play is open and no mask is on.
/// A mouth reports a run within one 20 ms period of its output taking it, and its start lies the
/// output's latency after that, so its report reaches the daemon before its first sample sounds;
/// 200 ms more covers a report held up that long. A busier machine leaks only past that.
const GUARD: u64 = 200_000;

/// Reads what the daemon tells `reader`'s engine until the connection ends, masking `tail`
/// microseconds past each play's runs; the replies, which carry nothing it needs, pass unread.
#[expect(
    clippy::single_call_fn,
    reason = "the engine's entry: main calls it once"
)]
pub fn run(mut reader: BufReader<UnixStream>, tail: u64) {
    // where the last frame heard ends, the last mask's time, the ruling, the open mask's planned
    // off, and each open play's rate
    let (mut heard, mut masked, mut ruled) = (0u64, 0u64, 0u64);
    let (mut planned, mut open) = (None::<u64>, BTreeMap::<u64, u64>::new());
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
            let _: Option<u64> = open.insert(at("play"), at("rate"));
        }
        if let Some(&rate) = open.get(&at("play")).filter(|_| told["samples"].is_u64()) {
            let lasted = at("samples").saturating_mul(1_000_000).checked_div(rate);
            let end = at("start").saturating_add(lasted.unwrap_or_default());
            if planned.is_none() {
                masked = (at("start").max(masked).max(ruled)).min(heard.saturating_add(FORESIGHT));
                said.push(json!({"on": true, "op": "mask", "start": masked}));
            }
            planned = Some(planned.unwrap_or_default().max(end.saturating_add(tail)));
        }
        if told["played"].is_u64() {
            let _: Option<u64> = open.remove(&at("played"));
        }
        // the tail heard out, `GUARD` more while a play may still report, the mask goes off
        // where the tail ends
        let wait = if open.is_empty() { 0 } else { GUARD };
        if let Some(off) = planned.filter(|&off| heard >= off.saturating_add(wait)) {
            masked = off.max(masked);
            said.push(json!({"on": false, "op": "mask", "start": masked}));
            planned = None;
        }
        let to = planned.map_or_else(|| heard.saturating_sub(wait), |off| heard.min(off));
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

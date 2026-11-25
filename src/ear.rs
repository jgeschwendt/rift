//! `rift device ear <name> --mic [<microphone>]` (macOS): the system's default input, followed as
//! it changes, or one input by its exact name, waited for while absent; open only while the ears
//! are, its channel 0 fed onto the track as frames that name it. The cpal lines are `mic.rs`; this
//! file runs in the suite against `Fake`.

use crate::mic::Mic;
use crate::{BEHIND, FRAME, append, error, hex, now, setting};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::env;
use std::f64::consts::PI;
use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::iter;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant};

/// Where a microphone's grant is given, in each message about one withheld.
const GRANT: &str = "allow rift in System Settings \u{203a} Privacy & Security \u{203a} Microphone";

/// An open's outcome: the input's sample rate and channel count, else why it failed.
pub type Ready = Sender<Result<(u32, u16), String>>;

/// Where an open input's interleaved buffers go, called from its own thread.
pub type Sound = Box<dyn FnMut(&[f32]) + Send>;

/// Where the microphones come from: the system's (`mic.rs`), else the suite's `Fake`.
pub trait Source: Sync {
    /// Opens the input `name` and says so on `ready`, then hands each buffer to `sound` until
    /// `stop` is dropped, and closes it.
    fn hold(&self, name: &str, ready: Ready, sound: Sound, stop: Receiver<()>);
    /// The default input's name, if there is one, and every input's.
    fn inputs(&self) -> (Option<String>, Vec<String>);
}

/// The suite's microphones under `$RIFT_MIC`: `default` names the default input; each file under
/// `inputs/` is an input, holding `<rate> <channels> <level> <swing>` (channel 0 the level plus and
/// minus the swing at alternate samples, every other channel full scale), `block` (an open that
/// never returns) or `fail`; `log` gets an `open` and a `close` line per open. An input whose
/// file goes is unlisted and falls silent.
struct Fake(PathBuf);

impl Source for Fake {
    #[expect(
        clippy::float_arithmetic,
        reason = "a test signal: the level plus or minus the swing"
    )]
    fn hold(&self, name: &str, ready: Ready, mut sound: Sound, stop: Receiver<()>) {
        let input = self.0.join("inputs").join(name);
        let spec = fs::read_to_string(&input).unwrap_or_default();
        let words: Vec<&str> = spec.split_whitespace().collect();
        let word = |at: usize| words.get(at).copied().unwrap_or_default();
        let (rate, channels): (u32, u16) = (
            word(0).parse().unwrap_or_default(),
            word(1).parse().unwrap_or_default(),
        );
        let (level, mut swing): (f32, f32) = (
            word(2).parse().unwrap_or_default(),
            word(3).parse().unwrap_or_default(),
        );
        let log = self.0.join("log");
        match spec.trim() {
            "block" => stop.recv().unwrap_or_default(),
            "fail" => drop(ready.send(Err("refused".to_owned()))),
            _ => {
                drop(ready.send(Ok((rate, channels))));
                drop(append(&log, format!("open {name}\n").as_bytes()));
                // paced on the clock, as a device's own is
                let (begun, mut given) = (Instant::now(), 0u128);
                while matches!(
                    stop.recv_timeout(Duration::from_millis(10)),
                    Err(RecvTimeoutError::Timeout)
                ) {
                    let due = (begun.elapsed().as_micros().saturating_mul(rate.into()))
                        .checked_div(1_000_000)
                        .unwrap_or_default();
                    let count = due.saturating_sub(given);
                    given = due;
                    if input.exists() {
                        let chunk: Vec<f32> = (0..count)
                            .flat_map(|_| {
                                swing = -swing;
                                iter::once(level + swing).chain(iter::repeat_n(
                                    1.0,
                                    usize::from(channels).saturating_sub(1),
                                ))
                            })
                            .collect();
                        sound(&chunk);
                    }
                }
                drop(append(&log, format!("close {name}\n").as_bytes()));
            }
        }
    }

    fn inputs(&self) -> (Option<String>, Vec<String>) {
        let mut names: Vec<String> = fs::read_dir(self.0.join("inputs"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        names.sort();
        let default = fs::read_to_string(self.0.join("default")).ok();
        (default.map(|held| held.trim().to_owned()), names)
    }
}

/// 16 kHz from an input's rate: a windowed-sinc low-pass under 7 kHz (under 0.45 of a rate below
/// 16 kHz), then each output sample read between the two filtered samples around it.
struct Convert {
    /// Where the next output sample falls after the last filtered sample, in input samples.
    at: f64,
    /// The last filtered sample.
    last: f64,
    /// The input samples under the filter, oldest first.
    recent: VecDeque<f64>,
    /// Input samples per output sample.
    step: f64,
    /// The filter, its taps summing to 1.
    taps: Vec<f64>,
}

#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::float_arithmetic,
    clippy::while_float,
    reason = "signal math in floating point; `as` saturates a filtered sample into 16 bits"
)]
impl Convert {
    /// The input samples of `sound` filtered and resampled onto `pcm` as 16-bit little-endian.
    fn feed(&mut self, sound: impl Iterator<Item = f32>, pcm: &mut Vec<u8>) {
        for sample in sound {
            let _: Option<f64> = self.recent.pop_front();
            self.recent.push_back(f64::from(sample));
            let filtered: f64 = (self.taps.iter().zip(&self.recent))
                .map(|(tap, held)| tap * held)
                .sum();
            while self.at <= 1.0f64 {
                let value = (filtered - self.last).mul_add(self.at, self.last);
                pcm.extend(((value * 32767.0f64).round() as i16).to_le_bytes());
                self.at += self.step;
            }
            (self.at, self.last) = (self.at - 1.0f64, filtered);
        }
    }
}

#[expect(
    clippy::float_arithmetic,
    reason = "a Hann-windowed sinc, its taps summing to 1"
)]
impl From<u32> for Convert {
    /// The converter for an input of `hz` samples a second: its filter a millisecond each side.
    fn from(hz: u32) -> Self {
        let half = i32::try_from(hz.div_ceil(1000)).unwrap_or(i32::MAX);
        let (rate, span) = (f64::from(hz), f64::from(half.saturating_add(1)));
        let cut = (rate * 0.45f64).min(7000.0f64) / rate;
        let taps: Vec<f64> = (half.saturating_neg()..=half)
            .map(|tap| {
                let at = f64::from(tap);
                let sinc = if tap == 0i32 {
                    2.0f64 * cut
                } else {
                    (2.0f64 * PI * cut * at).sin() / (PI * at)
                };
                sinc * 0.5f64.mul_add((PI * at / span).cos(), 0.5f64)
            })
            .collect();
        let sum: f64 = taps.iter().sum();
        Self {
            at: 1.0f64,
            last: 0.0f64,
            recent: iter::repeat_n(0.0f64, taps.len()).collect(),
            step: rate / 16_000.0f64,
            taps: taps.iter().map(|tap| tap / sum).collect(),
        }
    }
}

/// What the device hears: a line from the daemon (`None` at its end), or a buffer from the open of
/// that number.
enum Heard {
    /// A line from the daemon.
    Line(Option<String>),
    /// A buffer of interleaved samples.
    Sound(u64, Vec<f32>),
}

/// The input open now.
struct Open {
    /// Dropped, it closes.
    _stop: Sender<()>,
    /// Where its frame 0 starts on this device's clock.
    base: u64,
    /// Its channels, of which channel 0 is read.
    channels: usize,
    /// Its converter.
    convert: Convert,
    /// Whether it has given a sample other than exact zero.
    heard: bool,
    /// When it last gave sound.
    last: Instant,
    /// Its name.
    name: String,
    /// Its open's number, which its buffers carry.
    number: u64,
    /// Its converted bytes short of a frame.
    pcm: Vec<u8>,
    /// Its frames sent.
    sent: u64,
    /// When it opened.
    since: Instant,
}

/// The device after its reply, until the daemon goes (`unknown`, exit 1) or a stated failure (one
/// stderr line, exit 1): every line from the daemon (the reply first) answered, printed unless ok
/// and obeyed for the ears; the inputs asked every `$RIFT_POLL_MS`, the wanted one opened while
/// the ears are on and closed when they go off, another is wanted or it falls silent; its sound
/// sent as frames.
#[expect(
    clippy::single_call_fn,
    reason = "the device's entry: main calls it once, on macOS"
)]
pub fn run(reader: BufReader<UnixStream>, reply: &Value, pinned: Option<&str>) -> ExitCode {
    let hear = || -> Option<String> {
        let source: &'static dyn Source =
            env::var_os("RIFT_MIC").map_or(&Mic, |dir| Box::leak(Box::new(Fake(dir.into()))));
        let [deadline, zeros, poll] = [
            ("RIFT_OPEN_MS", 10_000),
            ("RIFT_ZERO_MS", 2000),
            ("RIFT_POLL_MS", 1000),
        ]
        .map(|(key, default)| Duration::from_millis(setting(key, default)));
        let mut out = reader.get_ref().try_clone().ok()?;
        reader.get_ref().set_read_timeout(None).ok()?;
        let (heard, hearing) = mpsc::sync_channel(BEHIND);
        heard.send(Heard::Line(Some(reply.to_string()))).ok()?;
        let lines = heard.clone();
        drop(thread::spawn(move || {
            reader
                .lines()
                .map_while(Result::ok)
                .map(Some)
                .chain([None])
                .try_for_each(|line| lines.send(Heard::Line(line)))
        }));
        let (named, listed) = source.inputs();
        let wanted = pinned.map(str::to_owned).or(named);
        if !wanted.as_ref().is_some_and(|name| listed.contains(name)) {
            eprintln!(
                "rift device: waiting for {}; the inputs present: {}",
                wanted.map_or_else(|| "a default input".to_owned(), |name| format!("{name:?}")),
                listed.join(", ")
            );
        }
        let (mut ears, mut open, mut number, mut polled) =
            (false, None::<Open>, 0u64, None::<Instant>);
        loop {
            if polled.is_none_or(|at| at.elapsed() >= poll) {
                polled = Some(Instant::now());
                let (default, present) = source.inputs();
                let want = pinned
                    .map(str::to_owned)
                    .or(default)
                    .filter(|name| ears && present.contains(name));
                // another input wanted, or this one silent a poll long: closed
                if open.as_ref().is_some_and(|held| {
                    Some(&held.name) != want.as_ref() || held.last.elapsed() >= poll
                }) {
                    open = None;
                }
                if let Some(name) = want.filter(|_| open.is_none()) {
                    number = number.saturating_add(1);
                    let (ready, opened) = mpsc::channel();
                    let (stop, stopped) = mpsc::channel();
                    let (sounds, at, called) = (heard.clone(), number, name.clone());
                    drop(thread::spawn(move || {
                        let sound = move |data: &[f32]| {
                            drop(sounds.try_send(Heard::Sound(at, data.to_vec())));
                        };
                        source.hold(&called, ready, Box::new(sound), stopped);
                    }));
                    let (rate, channels) = match opened.recv_timeout(deadline) {
                        Ok(Ok(format)) => format,
                        Ok(Err(cause)) => return Some(format!("{name:?}: {cause}")),
                        Err(_) => {
                            return Some(format!(
                                "{name:?} did not open within {} ms: the microphone is not granted; {GRANT}",
                                deadline.as_millis()
                            ));
                        }
                    };
                    open = Some(Open {
                        _stop: stop,
                        base: 0,
                        channels: usize::from(channels).max(1),
                        convert: Convert::from(rate),
                        heard: false,
                        last: Instant::now(),
                        name,
                        number,
                        pcm: Vec::new(),
                        sent: 0,
                        since: Instant::now(),
                    });
                }
            }
            match hearing.recv_timeout(poll) {
                Ok(Heard::Line(Some(text))) => {
                    let line: Value = serde_json::from_str(&text).unwrap_or_default();
                    if line["ping"].is_u64() {
                        let pong = json!({"op": "pong", "ping": line["ping"], "t": now()});
                        writeln!(out, "{pong}").ok()?;
                    }
                    if line["ok"] != true {
                        println!("{line}");
                    }
                    // the next poll opens or closes the microphone at once
                    if let Some(on) = line["ears"].as_bool() {
                        (ears, polled) = (on, None);
                    }
                }
                Ok(Heard::Line(None)) => return None,
                Ok(Heard::Sound(at, data))
                    if open.as_ref().is_some_and(|held| held.number == at) =>
                {
                    let held = open.as_mut()?;
                    held.last = Instant::now();
                    let channel = data.iter().step_by(held.channels).copied();
                    held.heard |= channel.clone().any(|sample| sample != 0.0);
                    held.convert.feed(channel, &mut held.pcm);
                    // stamped as `rift device` stamps a file: from the clock, over 100 ms off it rebased
                    while held.pcm.len() >= FRAME {
                        let frame: Vec<u8> = held.pcm.drain(..FRAME).collect();
                        let (stamp, clock) = (
                            held.sent.saturating_mul(20_000),
                            now().saturating_sub(20_000),
                        );
                        if clock.abs_diff(held.base.saturating_add(stamp)) > 100_000 {
                            held.base = clock.saturating_sub(stamp);
                        }
                        let sent = json!({"mic": held.name, "op": "frame", "pcm": hex(&frame), "t": held.base.saturating_add(stamp)});
                        writeln!(out, "{sent}").ok()?;
                        held.sent = held.sent.saturating_add(1);
                    }
                    if !held.heard && held.since.elapsed() >= zeros {
                        return Some(format!(
                            "{:?} gave only exact zeros for {} ms after it opened: the microphone is not granted, or is muted; {GRANT}",
                            held.name,
                            zeros.as_millis()
                        ));
                    }
                }
                // an earlier open's last buffers, or a poll with nothing heard
                Ok(Heard::Sound(..)) | Err(_) => {}
            }
        }
    };
    match hear() {
        Some(why) => eprintln!("rift device: {why}"),
        None => println!("{}", error("unknown")),
    }
    ExitCode::FAILURE
}

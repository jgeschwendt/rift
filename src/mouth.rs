//! `rift device mouth <name> --speaker` (macOS): plays what the daemon gives its track on the
//! system's default output, followed as it changes, open while a play is and `$RIFT_LINGER_MS`
//! after the output last took one; reports what the output took as runs and, while the ears are
//! open, sends it as the track's frames, each once it has sounded (docs/protocol.html#plays). The
//! output's callback only copies samples the loop converted and records what it rendered, under
//! one mutex each side holds only for that copy; the loop converts, accounts, cuts and writes. The
//! cpal lines are `speaker.rs`; this file runs in the suite against `Fake`.

use crate::ear::Convert;
use crate::speaker::Speaker;
use crate::{BEHIND, FRAME, append, error, hex, now, setting};
use serde_json::{Value, json};
use std::collections::{BTreeSet, VecDeque};
use std::env;
use std::f64::consts::PI;
use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::iter;
use std::mem;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// Phases between two given samples the interpolator's window is precomputed at.
const PHASES: usize = 256;

/// Given samples read each side of an output sample's position.
const TAPS: usize = 16;

/// Fills each buffer of an open output, interleaved, given its rate, its channels and the
/// microseconds until its first sample sounds.
pub type Fill = Box<dyn FnMut(&mut [f32], u32, u16, u64) + Send>;

/// A frame of the track: its start on this device's clock, its 640 bytes and its output's name.
type Frame = (u64, Vec<u8>, String);

/// Called once an open returns: the output's rate, else why it failed.
pub type Ready = Box<dyn FnOnce(Result<u32, String>) + Send>;

/// What a callback rendered: the sample, its position among the play's given samples (none for
/// silence), and when it sounds on this device's clock.
type Rendered = (f32, Option<f64>, u64);

/// The outputs as one listing found them.
#[derive(Default)]
pub struct Outputs {
    /// The default output's name, if there is one.
    pub default: Option<String>,
    /// Every output's name.
    pub names: Vec<String>,
}

/// Where the outputs come from: the system's (`speaker.rs`), else the suite's `Fake`.
pub trait Sink: Sync {
    /// Opens the output `name` and says so on `ready`, then has `fill` fill each buffer until
    /// `stop` is dropped or the output fails, and closes it.
    fn hold(&self, name: &str, ready: Ready, fill: Fill, stop: Receiver<()>);
    /// The outputs now.
    fn outputs(&self) -> Outputs;
}

/// The suite's outputs under `$RIFT_SPEAKER`: `default` names the default output; each file
/// under `outputs/` is an output holding `<rate> <channels> <latency ms>`, or `fail` (an open
/// refused, or an open one failing as its file comes to say so), unlisted when its file goes;
/// with a `take` file it takes no more output samples in all than that names, so it stalls; every
/// sample taken (channel 0, 16-bit little-endian) appended to `heard/<output>`, an `open` and a
/// `close` line per open to `log`.
struct Fake {
    /// The directory.
    dir: PathBuf,
    /// The output samples taken in all.
    taken: Mutex<u64>,
}

impl Sink for Fake {
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::float_arithmetic,
        reason = "a test output: what it took kept as 16 bits"
    )]
    fn hold(&self, name: &str, ready: Ready, mut fill: Fill, stop: Receiver<()>) {
        let (output, log) = (self.dir.join("outputs").join(name), self.dir.join("log"));
        let spec = || fs::read_to_string(&output).unwrap_or_default();
        if spec().trim() == "fail" {
            return ready(Err("refused".to_owned()));
        }
        let words: Vec<u64> = (spec().split_whitespace())
            .map(|word| word.parse().unwrap_or_default())
            .collect();
        let [rate, channels, latency] =
            [0, 1, 2].map(|at| words.get(at).copied().unwrap_or_default());
        ready(Ok(u32::try_from(rate).unwrap_or_default()));
        drop(append(&log, format!("open {name}\n").as_bytes()));
        let heard = self.dir.join("heard");
        drop(fs::create_dir_all(&heard));
        // paced on the clock, as an output's own is, never past `take`, until stopped or failed
        let (begun, mut given) = (Instant::now(), 0u64);
        while matches!(
            stop.recv_timeout(Duration::from_millis(10)),
            Err(RecvTimeoutError::Timeout)
        ) && spec().trim() != "fail"
        {
            let due = u64::try_from(begun.elapsed().as_micros())
                .unwrap_or_default()
                .saturating_mul(rate)
                .checked_div(1_000_000)
                .unwrap_or_default();
            let mut taken = self.taken.lock().unwrap_or_else(PoisonError::into_inner);
            let allowed = fs::read_to_string(self.dir.join("take"))
                .ok()
                .and_then(|all| all.trim().parse::<u64>().ok())
                .map_or(u64::MAX, |all| all.saturating_sub(*taken));
            let count = due.saturating_sub(given).min(allowed);
            let wide = usize::try_from(channels).unwrap_or_default().max(1);
            let mut data = vec![
                0.0f32;
                usize::try_from(count)
                    .unwrap_or_default()
                    .saturating_mul(wide)
            ];
            fill(
                &mut data,
                u32::try_from(rate).unwrap_or_default(),
                u16::try_from(channels).unwrap_or_default(),
                latency.saturating_mul(1000),
            );
            let kept: Vec<u8> = (data.iter().step_by(wide))
                .flat_map(|sample| ((sample * 32767.0f32).round() as i16).to_le_bytes())
                .collect();
            drop(append(&heard.join(name), &kept));
            (given, *taken) = (given.saturating_add(count), taken.saturating_add(count));
        }
        drop(append(&log, format!("close {name}\n").as_bytes()));
    }

    fn outputs(&self) -> Outputs {
        let mut names: Vec<String> = fs::read_dir(self.dir.join("outputs"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        names.sort();
        Outputs {
            default: fs::read_to_string(self.dir.join("default"))
                .ok()
                .map(|held| held.trim().to_owned()),
            names,
        }
    }
}

/// The track cut from what an open output took: 16 kHz, 20 ms frames on one grid from the open,
/// stamped by their count and rebased past 100 ms off the output's clock, as an ear's.
struct Cut {
    /// Where frame 0 starts on this device's clock.
    base: u64,
    /// The converter from the output's rate.
    convert: Convert,
    /// When the frame being cut began sounding.
    first: Option<u64>,
    /// Whether the frame being cut holds a play's samples.
    has: bool,
    /// The output's name, which its frames carry.
    name: String,
    /// Converted bytes short of a frame.
    pcm: Vec<u8>,
    /// Frames cut.
    sent: u64,
    /// Converted bytes still to drop: the rest of a frame a play's end padded.
    skip: usize,
}

impl Cut {
    /// The next frame, `pcm`'s first `FRAME` bytes, queued with its start when it holds a play's
    /// samples.
    fn emit(&mut self, frames: &mut VecDeque<Frame>) {
        let stamp = self.base.saturating_add(self.sent.saturating_mul(20_000));
        let first = self.first.unwrap_or(stamp);
        if first.abs_diff(stamp) > 100_000 {
            self.base = first.saturating_sub(self.sent.saturating_mul(20_000));
        }
        let frame: Vec<u8> = self.pcm.drain(..FRAME.min(self.pcm.len())).collect();
        if self.has {
            frames.push_back((
                self.base.saturating_add(self.sent.saturating_mul(20_000)),
                frame,
                self.name.clone(),
            ));
        }
        (self.sent, self.has, self.first) = (self.sent.saturating_add(1), false, None);
    }

    /// One rendered sample, sounding at `when`, a play's when `played`, into the track.
    fn feed(&mut self, value: f32, played: bool, when: u64, frames: &mut VecDeque<Frame>) {
        let _: &mut u64 = self.first.get_or_insert(when);
        self.has |= played;
        self.convert.feed(iter::once(value), &mut self.pcm);
        let dropped = self.skip.min(self.pcm.len());
        drop(self.pcm.drain(..dropped));
        self.skip = self.skip.saturating_sub(dropped);
        while self.pcm.len() >= FRAME {
            self.emit(frames);
        }
    }

    /// The frame being cut, holding a play's last samples, filled with silence and queued now;
    /// the silence the output gives in its place is dropped as it comes.
    fn pad(&mut self, frames: &mut VecDeque<Frame>) {
        if self.has {
            self.skip = self
                .skip
                .saturating_add(FRAME.saturating_sub(self.pcm.len()));
            self.pcm.resize(FRAME, 0);
            self.emit(frames);
        }
    }
}

/// What the device hears, on one channel: a line from the daemon (`None` at its end), and each
/// open's outcome and its end, tagged with its number, and the outputs.
enum Heard {
    /// An open's output stopped or failed.
    Closed(u64),
    /// A line from the daemon, its ping answered already.
    Line(Option<Value>),
    /// An open returned: the output's rate, else why it failed.
    Opened(u64, Result<u32, String>),
    /// A listing of the outputs.
    Outputs(Outputs),
}

/// The output open now, or opening.
struct Open {
    /// Dropped, it closes.
    _stop: Sender<()>,
    /// Its name.
    name: String,
    /// Its rate, once it opened.
    rate: Option<u32>,
}

/// The play under way: what is held of it, how far it is converted and how far its output took it.
struct Play {
    /// The next output sample's position among the given samples.
    at: f64,
    /// Whether nothing more will be given: its `end`, or a hush.
    ended: bool,
    /// The index of `held`'s first sample.
    front: u64,
    /// Its samples given.
    given: u64,
    /// The given samples held, from `front`, as far back as the output has not taken.
    held: VecDeque<f32>,
    /// Its number.
    num: u64,
    /// Whether its last frame has been padded and queued.
    padded: bool,
    /// Its samples the output took.
    played: u64,
    /// The given audio's rate in Hz.
    rate: u32,
    /// The run under way: its first sample and when that sounds, on this device's clock.
    run: Option<(u64, u64)>,
    /// Output samples taken since its last report.
    since: u64,
}

#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_arithmetic,
    reason = "signal math in floating point over sample counts far below 2^52"
)]
impl Play {
    /// The next output sample at `rate`, and its position: the window's taps over the given
    /// samples around it, its phase the nearest precomputed; none once converted to the end or
    /// while what it needs is not yet given.
    fn convert(&mut self, rate: u32, window: &[f64]) -> Option<(f32, f64)> {
        let base = self.at.floor();
        let ahead = (base as u64).saturating_add(TAPS as u64);
        if self.at > self.given as f64 - 1.0f64 && self.ended || !self.ended && ahead >= self.given
        {
            return None;
        }
        let phase = ((self.at - base) * PHASES as f64).round() as usize;
        let taps = window
            .get(phase.saturating_mul(2 * TAPS)..)
            .unwrap_or_default();
        let first = (base as u64).saturating_add(1).saturating_sub(TAPS as u64);
        let skip = usize::try_from(first.saturating_sub(self.front)).unwrap_or_default();
        // taps before the first given sample read silence
        let before = (TAPS as u64).saturating_sub((base as u64).saturating_add(1));
        let sum: f64 = (iter::repeat_n(0.0f32, usize::try_from(before).unwrap_or_default()))
            .chain(
                self.held
                    .iter()
                    .skip(skip)
                    .copied()
                    .chain(iter::repeat(0.0f32)),
            )
            .zip(taps.iter().take(2 * TAPS))
            .map(|(held, tap)| f64::from(held) * tap)
            .sum();
        let at = self.at;
        self.at += f64::from(self.rate) / f64::from(rate);
        // the last sample of an ended play stands for the given samples past it, so that taken,
        // every given sample counts played
        let last = self.ended && self.at > self.given as f64 - 1.0f64;
        let counted = if last {
            at.max(self.given as f64 - 1.0f64)
        } else {
            at
        };
        Some((sum as f32, counted))
    }

    /// Whether the output has taken all of it that it will: converted to the end, with nothing
    /// converted left untaken.
    fn finished(&self, queued: bool) -> bool {
        self.ended && self.at > self.given as f64 - 1.0f64 && !queued
    }

    /// The run under way as a report, `closing` it; none with no run.
    fn report(&mut self, closing: bool) -> Option<Value> {
        let (from, start) = if closing { self.run.take() } else { self.run }?;
        self.since = 0;
        Some(
            json!({"from": from, "op": "sound", "play": self.num, "samples": self.played.saturating_sub(from), "t": start}),
        )
    }

    /// One rendered sample at `rate`, at `position` among the given samples (none for silence),
    /// sounding at `when`: counted played, its run begun or reported each 20 ms, or the run ended.
    fn taken(&mut self, position: Option<f64>, when: u64, rate: u32, reports: &mut Vec<Value>) {
        if let Some(at) = position {
            let _: &mut (u64, u64) = self.run.get_or_insert((self.played, when));
            self.played = self
                .played
                .max((at as u64).saturating_add(1))
                .min(self.given);
            self.since = self.since.saturating_add(1);
            // the given samples the output is past, but for the window's reach, let go
            while self.front.saturating_add(TAPS as u64) < at as u64
                && self.held.pop_front().is_some()
            {
                self.front = self.front.saturating_add(1);
            }
        }
        if position.is_none() || self.since >= u64::from(rate).checked_div(50).unwrap_or_default() {
            reports.extend(self.report(position.is_none()));
        }
    }
}

/// What the output's callback shares with the loop, under one mutex each holds only to copy: the
/// samples converted for the output, each with its position, and what the callback rendered.
struct Ring {
    /// The open whose callback renders: an earlier open's renders silence and records nothing.
    open: u64,
    /// Converted for the output, not yet taken.
    queued: VecDeque<(f32, f64)>,
    /// Rendered, not yet read by the loop, its capacity never grown.
    rendered: VecDeque<Rendered>,
}

/// What the output `rendered` since the last look, at `rate`, copied out of the ring: each sample
/// accounted to `play` and cut into the track; whether the output took any of a play's.
fn absorb(
    rendered: Vec<Rendered>,
    play: &mut Option<Play>,
    cut: &mut Option<Cut>,
    frames: &mut VecDeque<Frame>,
    reports: &mut Vec<Value>,
    rate: u32,
) -> bool {
    let mut took = false;
    for (value, position, when) in rendered {
        if let Some(given) = play.as_mut() {
            given.taken(position, when, rate, reports);
        }
        if let Some(track) = cut.as_mut() {
            track.feed(value, position.is_some(), when, frames);
        }
        took |= position.is_some();
    }
    took
}

/// The interpolator's window for a play at `given` Hz into an output at `rate`: for each of
/// `PHASES + 1` phases between two given samples, `2 × TAPS` Hann-windowed sinc taps cut at 0.45
/// of the lower rate, summing to 1.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::float_arithmetic,
    clippy::single_call_fn,
    reason = "the window computed once a play and output, in floating point, apart from the loop"
)]
fn window(given: u32, rate: u32) -> Vec<f64> {
    let cut = 0.45f64 * f64::from(given.min(rate)) / f64::from(given);
    (0..=PHASES)
        .flat_map(|phase| {
            let fraction = phase as f64 / PHASES as f64;
            let taps: Vec<f64> = (0..2 * TAPS)
                .map(|tap| {
                    let apart = fraction + TAPS as f64 - 1.0f64 - tap as f64;
                    let sinc = if apart.abs() < f64::EPSILON {
                        2.0f64 * cut
                    } else {
                        (2.0f64 * PI * cut * apart).sin() / (PI * apart)
                    };
                    sinc * 0.5f64.mul_add((PI * apart / (TAPS as f64 + 1.0f64)).cos(), 0.5f64)
                })
                .collect();
            let sum: f64 = taps.iter().sum();
            taps.into_iter().map(move |tap| tap / sum)
        })
        .collect()
}

/// The device after its reply, until the daemon goes (`unknown`, then main waits a poll and
/// connects again): every ping answered on the reader's thread as it arrives; every line printed
/// but the plays' own and the replies that are ok; a give queued, an `end` played out, a hush
/// dropped at once, converted or not, and a play's lines once it has ended ignored; the outputs
/// listed every `$RIFT_POLL_MS`, the default opened while a play is open and `$RIFT_LINGER_MS`
/// after, and reopened as it changes or fails; a failed open said once, retried a poll doubled
/// per failure in a row, 64 at most. Its connection gone, it drops what it holds.
#[expect(
    clippy::single_call_fn,
    reason = "the device's entry: main calls it once, on macOS"
)]
pub fn run(reader: BufReader<UnixStream>, reply: &Value) {
    #[expect(
        clippy::cognitive_complexity,
        reason = "the device is one loop over one channel of what it hears, as the ear's"
    )]
    let hear = || -> Option<()> {
        let sink: &'static dyn Sink = env::var_os("RIFT_SPEAKER").map_or(&Speaker, |dir| {
            Box::leak(Box::new(Fake {
                dir: dir.into(),
                taken: Mutex::new(0),
            }))
        });
        let [poll, linger] = [("RIFT_POLL_MS", 1000), ("RIFT_LINGER_MS", 10_000)]
            .map(|(key, default)| Duration::from_millis(setting(key, default)));
        let out = Arc::new(Mutex::new(reader.get_ref().try_clone().ok()?));
        reader.get_ref().set_read_timeout(None).ok()?;
        let (heard, hearing) = mpsc::sync_channel(BEHIND);
        let (lines, pongs, first) = (heard.clone(), Arc::clone(&out), reply.clone());
        drop(thread::spawn(move || {
            let read = reader.lines().map_while(Result::ok);
            iter::once(first)
                .chain(read.map(|text| serde_json::from_str(&text).unwrap_or_default()))
                .map(Some)
                .chain([None])
                .try_for_each(|line: Option<Value>| {
                    if let Some(ping) = line
                        .as_ref()
                        .map(|held| &held["ping"])
                        .filter(|ping| ping.is_u64())
                    {
                        let pong = json!({"op": "pong", "ping": ping, "t": now()});
                        drop(pongs.lock().map(|mut held| writeln!(held, "{pong}")));
                    }
                    lines.send(Heard::Line(line))
                })
        }));
        let listing = heard.clone();
        drop(thread::spawn(move || {
            iter::repeat_with(|| sink.outputs()).try_for_each(|listed| {
                let sent = listing.send(Heard::Outputs(listed));
                thread::sleep(poll);
                sent
            })
        }));
        // two seconds of rendering at 48 kHz, drained every 10 ms
        let ring = Arc::new(Mutex::new(Ring {
            open: 0,
            queued: VecDeque::new(),
            rendered: VecDeque::with_capacity(96_000),
        }));
        let shared = || ring.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut ears, mut failed, mut open, mut outputs, mut retry, mut said) = (
            false,
            0u32,
            None::<Open>,
            Outputs::default(),
            None::<Instant>,
            BTreeSet::new(),
        );
        let (mut cut, mut frames, mut last, mut play, mut reports) = (
            None::<Cut>,
            VecDeque::<Frame>::new(),
            None::<Instant>,
            None::<Play>,
            Vec::new(),
        );
        // the last play ended, whose late lines are no one's; the window for a play and rate;
        // whether the open output failed
        let (mut finished, mut windowed, mut broken) = (0u64, (0u32, 0u32, Vec::new()), false);
        // each line said once until an output opens
        let say = |seen: &mut BTreeSet<String>, line: String| {
            if seen.insert(line.clone()) {
                eprintln!("rift device: {line}");
            }
        };
        loop {
            let pause = if play.is_some() || !frames.is_empty() {
                Duration::from_millis(10)
            } else {
                poll
            };
            let rate = open.as_ref().and_then(|held| held.rate).unwrap_or(1);
            match hearing.recv_timeout(pause) {
                Ok(Heard::Line(Some(line))) => {
                    let num = line["play"].as_u64();
                    if let Some(on) = line["ears"].as_bool() {
                        ears = on;
                    }
                    if line["ok"] != true
                        && num.is_none()
                        && line["end"].is_null()
                        && line["hush"].is_null()
                    {
                        println!("{line}");
                    }
                    // a play's lines once it has ended are no one's
                    let ours = play.as_ref().map(|held| held.num);
                    if let Some(text) = line["pcm"]
                        .as_str()
                        .filter(|_| num.is_some_and(|at| at > finished))
                    {
                        if num != ours {
                            play = Some(Play {
                                at: 0.0,
                                ended: false,
                                front: 0,
                                given: 0,
                                held: VecDeque::new(),
                                num: num.unwrap_or_default(),
                                padded: false,
                                played: 0,
                                rate: u32::try_from(line["rate"].as_u64().unwrap_or_default())
                                    .unwrap_or_default()
                                    .max(1),
                                run: None,
                                since: 0,
                            });
                        }
                        // four lowercase hex digits a 16-bit sample, little-endian
                        #[expect(
                            clippy::float_arithmetic,
                            reason = "16-bit samples to the output's floating point"
                        )]
                        let samples = text.as_bytes().chunks_exact(4).map(|digits| {
                            let byte = |at: usize| {
                                str::from_utf8(
                                    digits.get(at..at.saturating_add(2)).unwrap_or_default(),
                                )
                                .ok()
                                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                                .unwrap_or_default()
                            };
                            f32::from(i16::from_le_bytes([byte(0), byte(2)])) / 32768.0f32
                        });
                        if let Some(given) = play.as_mut().filter(|held| !held.ended) {
                            let before = given.held.len();
                            given.held.extend(samples);
                            let count = given.held.len().saturating_sub(before);
                            given.given = given
                                .given
                                .saturating_add(u64::try_from(count).unwrap_or_default());
                        }
                    }
                    if let Some(given) = play.as_mut().filter(|held| {
                        line["end"].as_u64() == Some(held.num)
                            || line["hush"].as_u64() == Some(held.num)
                    }) {
                        given.ended = true;
                        // a hush drops what is queued at once, converted or not; what the output
                        // took still sounds and is counted
                        if line["hush"].is_u64() {
                            let rendered: Vec<Rendered> = {
                                let mut copying = shared();
                                copying.queued.clear();
                                copying.rendered.drain(..).collect()
                            };
                            let took = absorb(
                                rendered,
                                &mut play,
                                &mut cut,
                                &mut frames,
                                &mut reports,
                                rate,
                            );
                            last = took.then(Instant::now).or(last);
                            if let Some(hushed) = play.as_mut() {
                                hushed.held.clear();
                                (hushed.given, hushed.at) = (hushed.played, f64::MAX);
                                reports.extend(hushed.report(true));
                            }
                        }
                    }
                }
                Ok(Heard::Line(None)) => return None,
                Ok(Heard::Opened(at, opened)) if at == shared().open => match opened {
                    Ok(hz) => {
                        (failed, said) = (0, BTreeSet::new());
                        if let Some(held) = open.as_mut() {
                            held.rate = Some(hz);
                            cut = Some(Cut {
                                base: 0,
                                convert: Convert::from(hz),
                                first: None,
                                has: false,
                                name: held.name.clone(),
                                pcm: Vec::new(),
                                sent: 0,
                                skip: 0,
                            });
                        }
                    }
                    Err(cause) => {
                        let name = open.as_ref().map(|held| held.name.clone());
                        say(
                            &mut said,
                            format!("{:?}: {cause}", name.unwrap_or_default()),
                        );
                        let backoff = poll.saturating_mul(2u32.saturating_pow(failed.min(6)));
                        (failed, open, retry) = (
                            failed.saturating_add(1),
                            None,
                            Instant::now().checked_add(backoff),
                        );
                    }
                },
                // the open output stopped by itself: failed, so closed and opened again a poll on
                Ok(Heard::Closed(at)) if open.is_some() && at == shared().open => {
                    let name = open.as_ref().map(|held| held.name.clone());
                    say(
                        &mut said,
                        format!("{:?} failed; opening it again", name.unwrap_or_default()),
                    );
                    (broken, retry) = (true, Instant::now().checked_add(poll));
                }
                Ok(Heard::Outputs(listed)) => outputs = listed,
                Ok(Heard::Closed(..) | Heard::Opened(..)) | Err(_) => {}
            }
            // the output wanted while a play is open and as it lingers, the default if present
            let wanted = play.is_some() || last.is_some_and(|at| at.elapsed() < linger);
            let target =
                (outputs.default.clone()).filter(|name| wanted && outputs.names.contains(name));
            // under the ring's lock only to copy: what the output rendered out, and, for an
            // output closed, the open moved on and what it was given and had not taken taken
            // back, to be converted again for the next
            let closing = broken
                || open
                    .as_ref()
                    .is_some_and(|current| Some(&current.name) != target.as_ref());
            let (rendered, untaken, waiting) = {
                let mut copying = shared();
                if closing {
                    copying.open = copying.open.saturating_add(1);
                }
                let untaken = closing.then(|| copying.queued.drain(..).next()).flatten();
                (
                    copying.rendered.drain(..).collect::<Vec<_>>(),
                    untaken,
                    copying.queued.len(),
                )
            };
            if absorb(
                rendered,
                &mut play,
                &mut cut,
                &mut frames,
                &mut reports,
                rate,
            ) {
                last = Some(Instant::now());
            }
            if closing {
                // a closed output ends the run under way and starts the track's grid afresh
                (open, broken, cut) = (None, false, None);
                if let Some(given) = play.as_mut() {
                    given.at = untaken.map_or(given.at, |queued| queued.1);
                    reports.extend(given.report(true));
                }
            }
            // converted ahead of the output a quarter second, outside the lock, its window made
            // once a play and rate
            let mut converted = Vec::new();
            if let (Some(given), Some(hz)) = (
                play.as_mut(),
                open.as_ref().and_then(|current| current.rate),
            ) {
                if (windowed.0, windowed.1) != (given.rate, hz) {
                    windowed = (given.rate, hz, window(given.rate, hz));
                }
                let room = usize::try_from(hz)
                    .unwrap_or_default()
                    .checked_div(4)
                    .unwrap_or_default();
                while waiting.saturating_add(converted.len()) < room
                    && let Some(next) = given.convert(hz, &windowed.2)
                {
                    converted.push(next);
                }
            }
            let (queued, number) = {
                let mut copying = shared();
                copying.queued.extend(converted);
                (!copying.queued.is_empty(), copying.open)
            };
            // the reports, then the frames that have sounded (none while the ears are closed),
            // then a finished play's `done`, once its last frame is sent
            let mut writer = out.lock().ok()?;
            if let Some(given) = play
                .as_mut()
                .filter(|current| current.finished(queued) && !current.padded)
            {
                given.padded = true;
                reports.extend(given.report(true));
                if let Some(track) = cut.as_mut() {
                    track.pad(&mut frames);
                }
            }
            for report in mem::take(&mut reports) {
                writeln!(writer, "{report}").ok()?;
            }
            while let Some((start, frame, speaker)) =
                frames.pop_front_if(|frame| !ears || frame.0.saturating_add(20_000) <= now())
            {
                if ears {
                    let sent =
                        json!({"op": "frame", "pcm": hex(&frame), "speaker": speaker, "t": start});
                    writeln!(writer, "{sent}").ok()?;
                }
            }
            if let Some(num) = play
                .as_ref()
                .filter(|current| current.padded && frames.is_empty())
                .map(|current| current.num)
            {
                writeln!(writer, "{}", json!({"op": "done", "play": num})).ok()?;
                (play, finished) = (None, num);
            }
            drop(writer);
            if wanted && target.is_none() {
                say(
                    &mut said,
                    format!(
                        "waiting for a default output; the outputs present: {}",
                        outputs.names.join(", ")
                    ),
                );
            }
            if let Some(name) =
                target.filter(|_| open.is_none() && retry.is_none_or(|at| at <= Instant::now()))
            {
                let next = number.saturating_add(1);
                shared().open = next;
                let (stop, stopped) = mpsc::channel();
                let (opened, called, filled) = (heard.clone(), name.clone(), Arc::clone(&ring));
                drop(thread::spawn(move || {
                    let (ended, closed) = (opened.clone(), opened);
                    let ready = move |outcome| drop(ended.send(Heard::Opened(next, outcome)));
                    // copies converted samples out and records what it rendered, nothing more
                    let fill = move |data: &mut [f32], hz: u32, channels: u16, latency: u64| {
                        let clock = now().saturating_add(latency);
                        let mut copy = filled.lock().unwrap_or_else(PoisonError::into_inner);
                        let current = copy.open == next;
                        for (index, frame) in
                            data.chunks_mut(usize::from(channels).max(1)).enumerate()
                        {
                            let sample = current.then(|| copy.queued.pop_front()).flatten();
                            let value = sample.map_or(0.0f32, |taken| taken.0);
                            frame.fill(value);
                            let when = clock.saturating_add(
                                u64::try_from(index)
                                    .unwrap_or_default()
                                    .saturating_mul(1_000_000)
                                    .checked_div(u64::from(hz))
                                    .unwrap_or_default(),
                            );
                            if current && copy.rendered.len() < copy.rendered.capacity() {
                                copy.rendered
                                    .push_back((value, sample.map(|taken| taken.1), when));
                            }
                        }
                    };
                    sink.hold(&called, Box::new(ready), Box::new(fill), stopped);
                    drop(closed.send(Heard::Closed(next)));
                }));
                open = Some(Open {
                    _stop: stop,
                    name,
                    rate: None,
                });
            }
        }
    };
    let _: Option<()> = hear();
    println!("{}", error("unknown"));
}

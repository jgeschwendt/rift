//! `rift device mouth <name> --speaker` (macOS): plays what the daemon gives its track on the
//! system's default output, followed as it changes, open while a play is and `$RIFT_LINGER_MS`
//! after the output last took one; reports what the output took as runs and, while the ears are
//! open, sends it as the track's frames, each once it has sounded (docs/protocol.html#plays). The
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
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// Given samples read each side of an output sample's position.
const TAPS: i64 = 16;

/// A frame of the track: its start on this device's clock, its 640 bytes and its output's name.
type Frame = (u64, Vec<u8>, String);

/// Fills each buffer of an open output, interleaved, given its rate, its channels and the
/// microseconds until its first sample sounds.
pub type Fill = Box<dyn FnMut(&mut [f32], u32, u16, u64) + Send>;

/// Called once an open returns: nothing, else why it failed.
pub type Ready = Box<dyn FnOnce(Result<(), String>) + Send>;

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
    /// `stop` is dropped, and closes it.
    fn hold(&self, name: &str, ready: Ready, fill: Fill, stop: Receiver<()>);
    /// The outputs now.
    fn outputs(&self) -> Outputs;
}

/// The suite's outputs under `$RIFT_SPEAKER`: `default` names the default output; each file
/// under `outputs/` is an output holding `<rate> <channels> <latency ms>`, or `fail`, gone when
/// its file goes; `take`, when present, the output samples it may take in all; every sample
/// taken (channel 0, 16-bit little-endian) appended to `heard/<output>`, an `open` and a `close`
/// line per open to `log`.
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
        let spec = fs::read_to_string(&output).unwrap_or_default();
        if spec.trim() == "fail" {
            return ready(Err("refused".to_owned()));
        }
        let words: Vec<u64> = (spec.split_whitespace())
            .map(|word| word.parse().unwrap_or_default())
            .collect();
        let [rate, channels, latency] =
            [0, 1, 2].map(|at| words.get(at).copied().unwrap_or_default());
        ready(Ok(()));
        drop(append(&log, format!("open {name}\n").as_bytes()));
        let heard = self.dir.join("heard");
        drop(fs::create_dir_all(&heard));
        // paced on the clock, as an output's own is, and never past `take`
        let (begun, mut given) = (Instant::now(), 0u64);
        while matches!(
            stop.recv_timeout(Duration::from_millis(10)),
            Err(RecvTimeoutError::Timeout)
        ) {
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

/// What the device hears, on one channel: a line from the daemon (`None` at its end), each
/// open's outcome tagged with its number, and the outputs.
enum Heard {
    /// A line from the daemon, its ping answered already.
    Line(Option<Value>),
    /// An open returned.
    Opened(u64, Result<(), String>),
    /// A listing of the outputs.
    Outputs(Outputs),
}

/// The output open now, or opening.
struct Open {
    /// Dropped, it closes.
    _stop: Sender<()>,
    /// Its name.
    name: String,
}

/// What the loop and the output's callback share.
#[derive(Default)]
struct Shared {
    /// The track being cut from the open output.
    cut: Option<Cut>,
    /// Frames cut, until each has sounded.
    frames: VecDeque<Frame>,
    /// When the output last took a play's sample.
    last: Option<Instant>,
    /// The open whose callback fills: an earlier open's is ignored.
    open: u64,
    /// Reports to send, in order.
    reports: Vec<Value>,
    /// The output open now.
    speaker: String,
    /// The play under way.
    speech: Option<Speech>,
}

impl Shared {
    /// Fills open `number`'s buffer of `data` at `rate` with `channels`, its first sample
    /// sounding `latency` microseconds from now: the play's samples, else silence, each taken one
    /// counted, run and reported, and the track cut from all of it.
    fn fill(&mut self, number: u64, data: &mut [f32], rate: u32, channels: u16, latency: u64) {
        let (clock, current) = (now().saturating_add(latency), number == self.open);
        for (index, frame) in
            (data.chunks_mut(usize::from(channels).max(1)).enumerate()).take_while(|_| current)
        {
            let cut = self.cut.get_or_insert_with(|| Cut {
                base: 0,
                convert: Convert::from(rate),
                first: None,
                has: false,
                name: self.speaker.clone(),
                pcm: Vec::new(),
                sent: 0,
                skip: 0,
            });
            let when = clock.saturating_add(
                u64::try_from(index)
                    .unwrap_or_default()
                    .saturating_mul(1_000_000)
                    .checked_div(u64::from(rate))
                    .unwrap_or_default(),
            );
            let sample = (self.speech.as_mut()).and_then(|held| held.take(rate, when));
            frame.fill(sample.unwrap_or_default());
            // a run reported each 20 ms of output, and as it ends
            if let Some(held) = self.speech.as_mut() {
                let closing = sample.is_none();
                if closing || held.since >= u64::from(rate).checked_div(50).unwrap_or_default() {
                    self.reports.extend(held.report(closing));
                }
            }
            if sample.is_some() {
                self.last = Some(Instant::now());
            }
            let _: &mut u64 = cut.first.get_or_insert(when);
            cut.has |= sample.is_some();
            cut.convert
                .feed(iter::once(sample.unwrap_or_default()), &mut cut.pcm);
            let dropped = cut.skip.min(cut.pcm.len());
            drop(cut.pcm.drain(..dropped));
            cut.skip = cut.skip.saturating_sub(dropped);
            while cut.pcm.len() >= FRAME {
                cut.emit(&mut self.frames);
            }
        }
    }
}

/// The play under way: what is queued of it and how far its output has taken it.
struct Speech {
    /// The next output sample's position among the given samples.
    at: f64,
    /// Whether nothing more will be given: its `end`, or a hush.
    ended: bool,
    /// The index of `held`'s first sample.
    front: u64,
    /// Its samples given.
    given: u64,
    /// The given samples held, from `front`.
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
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_arithmetic,
    reason = "signal math in floating point over sample counts far below 2^52"
)]
impl Speech {
    /// Whether the output has taken all of it that it will.
    fn finished(&self) -> bool {
        self.ended && self.at > self.given as f64 - 1.0f64
    }

    /// The run under way as a report, `closing` it; none with no run.
    fn report(&mut self, closing: bool) -> Option<Value> {
        let (from, start) = if closing { self.run.take() } else { self.run }?;
        self.since = 0;
        Some(
            json!({"from": from, "op": "sound", "play": self.num, "samples": self.played.saturating_sub(from), "t": start}),
        )
    }

    /// The next output sample at `rate`, sounding at `when`: a Hann-windowed sinc interpolation
    /// of the given samples around its position, `TAPS` each side, cut at 0.45 of the lower rate;
    /// none once finished or while what it needs is not yet given.
    fn take(&mut self, rate: u32, when: u64) -> Option<f32> {
        let base = self.at.floor() as i64;
        // finished, every given sample has gone into the output; else it has run dry
        if self.finished() {
            self.played = self.given;
        }
        if self.finished() || !self.ended && base.saturating_add(TAPS) >= self.given as i64 {
            return None;
        }
        let (given, output) = (f64::from(self.rate), f64::from(rate));
        let cut = 0.45f64 * given.min(output) / given;
        let (mut sum, mut weight) = (0.0f64, 0.0f64);
        for index in base.saturating_sub(TAPS).saturating_add(1)..=base.saturating_add(TAPS) {
            let apart = self.at - index as f64;
            let sinc = if apart.abs() < f64::EPSILON {
                2.0f64 * cut
            } else {
                (2.0f64 * PI * cut * apart).sin() / (PI * apart)
            };
            let tap = sinc * 0.5f64.mul_add((PI * apart / (TAPS + 1) as f64).cos(), 0.5f64);
            let held = u64::try_from(index)
                .ok()
                .and_then(|at| at.checked_sub(self.front))
                .and_then(|at| self.held.get(usize::try_from(at).ok()?));
            (sum, weight) = (
                tap.mul_add(f64::from(held.copied().unwrap_or_default()), sum),
                weight + tap,
            );
        }
        let _: &mut (u64, u64) = self.run.get_or_insert((self.played, when));
        self.played = self
            .played
            .max((base as u64).saturating_add(1))
            .min(self.given);
        self.since = self.since.saturating_add(1);
        self.at += given / output;
        while self.front.saturating_add(TAPS as u64) < self.at as u64
            && self.held.pop_front().is_some()
        {
            self.front = self.front.saturating_add(1);
        }
        Some((sum / weight) as f32)
    }
}

/// The device after its reply, until the daemon goes (`unknown`, then main waits a poll and
/// connects again): every ping answered on the reader's thread as it arrives; every line printed
/// but the plays' own and the replies that are ok; a give queued, an `end` played out, a hush
/// dropped at once; the outputs listed every `$RIFT_POLL_MS`, the default opened while a play is
/// open and `$RIFT_LINGER_MS` after, and reopened as it changes; a failed open said once, retried
/// a poll doubled per failure in a row, 64 at most. Its connection gone, it drops what it holds.
#[expect(
    clippy::single_call_fn,
    reason = "the device's entry: main calls it once, on macOS"
)]
pub fn run(reader: BufReader<UnixStream>, reply: &Value) {
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
        let shared = Arc::new(Mutex::new(Shared::default()));
        let lock = || shared.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut ears, mut failed, mut open, mut outputs, mut retry, mut said) = (
            false,
            0u32,
            None::<Open>,
            Outputs::default(),
            None::<Instant>,
            BTreeSet::new(),
        );
        // each line said once until an output opens
        let say = |seen: &mut BTreeSet<String>, line: String| {
            if seen.insert(line.clone()) {
                eprintln!("rift device: {line}");
            }
        };
        loop {
            let busy = {
                let held: MutexGuard<'_, Shared> = lock();
                held.speech.is_some() || !held.frames.is_empty()
            };
            let pause = if busy {
                Duration::from_millis(10)
            } else {
                poll
            };
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
                    let state = &mut *lock();
                    let ours = state.speech.as_ref().map(|speech| speech.num);
                    if let Some(text) = line["pcm"].as_str() {
                        if num != ours {
                            state.speech = Some(Speech {
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
                        let given = text.as_bytes().chunks_exact(4).map(|digits| {
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
                        if let Some(speech) = state.speech.as_mut() {
                            let before = speech.held.len();
                            speech.held.extend(given);
                            let count = speech.held.len().saturating_sub(before);
                            speech.given = speech
                                .given
                                .saturating_add(u64::try_from(count).unwrap_or_default());
                        }
                    }
                    if let Some(speech) = state.speech.as_mut().filter(|speech| {
                        line["end"].as_u64() == Some(speech.num)
                            || line["hush"].as_u64() == Some(speech.num)
                    }) {
                        speech.ended = true;
                        // a hush drops what is queued at once; what the output took still sounds
                        if line["hush"].is_u64() {
                            speech.held.clear();
                            speech.given = speech.played;
                            state.reports.extend(speech.report(true));
                        }
                    }
                }
                Ok(Heard::Line(None)) => return None,
                Ok(Heard::Opened(at, opened)) if at == lock().open => match opened {
                    Ok(()) => (failed, said) = (0, BTreeSet::new()),
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
                Ok(Heard::Outputs(listed)) => outputs = listed,
                Ok(Heard::Opened(..)) | Err(_) => {}
            }
            let opening = {
                let state = &mut *lock();
                // the reports, then the frames that have sounded (none while the ears are closed),
                // then a finished play's `done`, once its last frame is sent
                let mut writer = out.lock().ok()?;
                for report in state.reports.drain(..) {
                    writeln!(writer, "{report}").ok()?;
                }
                if let Some(speech) = state
                    .speech
                    .as_mut()
                    .filter(|speech| speech.finished() && !speech.padded)
                {
                    speech.padded = true;
                    if let Some(cut) = state.cut.as_mut() {
                        cut.pad(&mut state.frames);
                    }
                }
                while let Some((start, frame, speaker)) = state
                    .frames
                    .pop_front_if(|frame| !ears || frame.0.saturating_add(20_000) <= now())
                {
                    if ears {
                        let sent = json!({"op": "frame", "pcm": hex(&frame), "speaker": speaker, "t": start});
                        writeln!(writer, "{sent}").ok()?;
                    }
                }
                if let Some(num) = state
                    .speech
                    .as_ref()
                    .filter(|speech| speech.padded && state.frames.is_empty())
                    .map(|speech| speech.num)
                {
                    writeln!(writer, "{}", json!({"op": "done", "play": num})).ok()?;
                    state.speech = None;
                }
                drop(writer);
                // the output wanted while a play is open and as it lingers, the default if present
                let wanted = state.speech.is_some()
                    || state.last.is_some_and(|last| last.elapsed() < linger);
                let target =
                    (outputs.default.clone()).filter(|name| wanted && outputs.names.contains(name));
                if open
                    .as_ref()
                    .is_some_and(|current| Some(&current.name) != target.as_ref())
                {
                    // a closed output ends the run under way and starts the track's grid afresh
                    open = None;
                    state.cut = None;
                    state.open = state.open.saturating_add(1);
                    if let Some(speech) = state.speech.as_mut() {
                        state.reports.extend(speech.report(true));
                    }
                }
                if wanted && target.is_none() {
                    say(
                        &mut said,
                        format!(
                            "waiting for a default output; the outputs present: {}",
                            outputs.names.join(", ")
                        ),
                    );
                }
                let next = target
                    .filter(|_| open.is_none() && retry.is_none_or(|at| at <= Instant::now()));
                next.map(|name| {
                    (state.open, state.speaker) = (state.open.saturating_add(1), name.clone());
                    (name, state.open)
                })
            };
            if let Some((name, number)) = opening {
                let (stop, stopped) = mpsc::channel();
                let (opened, called, filled) = (heard.clone(), name.clone(), Arc::clone(&shared));
                drop(thread::spawn(move || {
                    let ready = move |outcome| drop(opened.send(Heard::Opened(number, outcome)));
                    let fill = move |data: &mut [f32], rate, channels, latency| {
                        let mut guard = filled.lock().unwrap_or_else(PoisonError::into_inner);
                        guard.fill(number, data, rate, channels, latency);
                    };
                    sink.hold(&called, Box::new(ready), Box::new(fill), stopped);
                }));
                open = Some(Open { _stop: stop, name });
            }
        }
    };
    let _: Option<()> = hear();
    println!("{}", error("unknown"));
}

//! `rift device ear <name> --mic [<microphone>]` (macOS): the system's default input, followed as
//! it changes, or one input by its exact name, waited for while absent; open only while the ears
//! are, its channel 0 fed onto the track as frames that name it. The cpal lines are `mic.rs`; this
//! file runs in the suite against `Fake`.

use crate::mic::Mic;
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
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Where a microphone's grant is given, in each message about one withheld.
const GRANT: &str = "allow rift in System Settings \u{203a} Privacy & Security \u{203a} Microphone";

/// Called once an open returns: the input's sample rate and channel count, else why it failed.
pub type Ready = Box<dyn FnOnce(Result<(u32, u16), String>) + Send>;

/// Where an open input's interleaved buffers go, called from its own thread.
pub type Sound = Box<dyn FnMut(&[f32]) + Send>;

/// The inputs as one listing found them.
#[derive(Default)]
pub struct Inputs {
    /// The built-in input's name, if the Mac has one.
    pub built: Option<String>,
    /// The default input's name, if there is one.
    pub default: Option<String>,
    /// Every input's name.
    pub names: Vec<String>,
    /// Whether the default input is Bluetooth (or Bluetooth LE).
    pub wireless: bool,
}

/// Where the microphones come from: the system's (`mic.rs`), else the suite's `Fake`.
pub trait Source: Sync {
    /// Opens the input `name` and says so on `ready`, then hands each buffer to `sound` until
    /// `stop` is dropped, and closes it.
    fn hold(&self, name: &str, ready: Ready, sound: Sound, stop: Receiver<()>);
    /// The inputs now.
    fn inputs(&self) -> Inputs;
}

/// The suite's microphones under `$RIFT_MIC`: `default` names the default input; each file under
/// `inputs/` is an input, holding `<rate> <channels> <level> <swing> [bluetooth | builtin]` (channel
/// 0 the level plus and minus the swing at alternate samples, every other channel full scale; the
/// last word its transport), `block` (an open that
/// waits, as for a grant's prompt, until the file says otherwise) or `fail`; `log` gets an `ask`
/// line per open asked for, then an `open` and a `close` line per open that returned; `slow`, the
/// milliseconds a listing takes. An input whose file goes is unlisted and falls silent.
struct Fake(PathBuf);

impl Source for Fake {
    #[expect(
        clippy::float_arithmetic,
        reason = "a test signal: the level plus or minus the swing"
    )]
    fn hold(&self, name: &str, ready: Ready, mut sound: Sound, stop: Receiver<()>) {
        let (input, log) = (self.0.join("inputs").join(name), self.0.join("log"));
        let tick = || {
            matches!(
                stop.recv_timeout(Duration::from_millis(10)),
                Err(RecvTimeoutError::Timeout)
            )
        };
        drop(append(&log, format!("ask {name}\n").as_bytes()));
        let mut spec = fs::read_to_string(&input).unwrap_or_default();
        while spec.trim() == "block" {
            if !tick() {
                return;
            }
            spec = fs::read_to_string(&input).unwrap_or_default();
        }
        if spec.trim() == "fail" {
            return ready(Err("refused".to_owned()));
        }
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
        ready(Ok((rate, channels)));
        drop(append(&log, format!("open {name}\n").as_bytes()));
        // paced on the clock, as a device's own is
        let (begun, mut given) = (Instant::now(), 0u128);
        while tick() {
            let due = (begun.elapsed().as_micros().saturating_mul(rate.into()))
                .checked_div(1_000_000)
                .unwrap_or_default();
            let count = due.saturating_sub(given);
            given = due;
            if input.exists() {
                let chunk: Vec<f32> = (0..count)
                    .flat_map(|_| {
                        swing = -swing;
                        iter::once(level + swing)
                            .chain(iter::repeat_n(1.0, usize::from(channels).saturating_sub(1)))
                    })
                    .collect();
                sound(&chunk);
            }
        }
        drop(append(&log, format!("close {name}\n").as_bytes()));
    }

    fn inputs(&self) -> Inputs {
        let slow = fs::read_to_string(self.0.join("slow")).unwrap_or_default();
        thread::sleep(Duration::from_millis(
            slow.trim().parse().unwrap_or_default(),
        ));
        let mut names: Vec<String> = fs::read_dir(self.0.join("inputs"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        names.sort();
        let default = fs::read_to_string(self.0.join("default"))
            .ok()
            .map(|held| held.trim().to_owned());
        let transport = |name: &String| {
            let spec = fs::read_to_string(self.0.join("inputs").join(name)).unwrap_or_default();
            spec.split_whitespace()
                .nth(4)
                .unwrap_or_default()
                .to_owned()
        };
        Inputs {
            built: names
                .iter()
                .find(|name| transport(name) == "builtin")
                .cloned(),
            wireless: default
                .as_ref()
                .is_some_and(|name| transport(name) == "bluetooth"),
            default,
            names,
        }
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

/// What the device hears, on one channel: the inputs, a line from the daemon (`None` at its end),
/// and each open's outcome and buffers, tagged with its number.
enum Heard {
    /// A listing of the inputs.
    Inputs(Inputs),
    /// A line from the daemon, its ping answered already.
    Line(Option<Value>),
    /// An open returned.
    Opened(u64, Result<(u32, u16), String>),
    /// A buffer of interleaved samples, and this device's clock as it was handed over.
    Sound(u64, u64, Vec<f32>),
}

/// The input open now, or opening.
struct Open {
    /// Dropped, it closes.
    _stop: Sender<()>,
    /// Where its frame 0 starts on this device's clock.
    base: u64,
    /// Once it opened: its channels, of which channel 0 is read, and its converter.
    format: Option<(usize, Convert)>,
    /// Whether it has given a sample other than exact zero.
    heard: bool,
    /// When its last sound was sent, or it opened.
    last: Instant,
    /// Its name.
    name: String,
    /// Its open's number, which its buffers carry.
    number: u64,
    /// Its converted bytes short of a frame.
    pcm: Vec<u8>,
    /// Its frames sent.
    sent: u64,
    /// When it was asked to open, then when it opened.
    since: Instant,
}

/// The device after its reply, until the daemon goes (`unknown`, then main waits a poll and
/// connects again): every ping answered on
/// the reader's thread as it arrives; every line printed unless ok and obeyed for the ears; the
/// inputs listed on a thread of their own every `$RIFT_POLL_MS`; the wanted one opened while the
/// ears are on and closed when they go off or another is wanted; its sound sent as frames from its
/// first sample other than exact zero. A failure (the open refused, no signal `$RIFT_ZERO_MS` after
/// it, a poll's silence) closes it, and the next open waits a poll doubled per failure in a row, 64
/// at most; each line is said once until the input is heard. Nothing here waits on the system.
#[expect(
    clippy::single_call_fn,
    reason = "the device's entry: main calls it once, on macOS"
)]
pub fn run(reader: BufReader<UnixStream>, reply: &Value, pinned: Option<&str>) {
    let hear = || -> Option<()> {
        let source: &'static dyn Source =
            env::var_os("RIFT_MIC").map_or(&Mic, |dir| Box::leak(Box::new(Fake(dir.into()))));
        let [deadline, zeros, poll] = [
            ("RIFT_OPEN_MS", 10_000),
            ("RIFT_ZERO_MS", 2000),
            ("RIFT_POLL_MS", 1000),
        ]
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
        // the input to hear and, when the Bluetooth rule decided it, why: the pinned one, else
        // the default, but for a Bluetooth default the built-in, since a headset's microphone open
        // puts it in call mode
        let pick = |listed: &Inputs| match (
            pinned,
            listed.wireless,
            listed.built.as_ref(),
            listed.default.as_ref(),
        ) {
            (Some(name), ..) => (Some(name.to_owned()), None),
            (None, true, Some(built), Some(blue)) => (
                Some(built.clone()),
                Some(format!(
                    "{blue:?} is Bluetooth, which an open microphone puts in call mode; hearing {built:?} instead"
                )),
            ),
            (None, true, None, Some(blue)) => (
                Some(blue.clone()),
                Some(format!(
                    "{blue:?} is Bluetooth and this Mac has no built-in input; hearing it, in call mode while the ears are on"
                )),
            ),
            (None, _, _, default) => (default.cloned(), None),
        };
        let mut inputs = source.inputs();
        let wanted = pick(&inputs).0;
        if !wanted
            .as_ref()
            .is_some_and(|name| inputs.names.contains(name))
        {
            eprintln!(
                "rift device: waiting for {}; the inputs present: {}",
                wanted.map_or_else(|| "a default input".to_owned(), |name| format!("{name:?}")),
                inputs.names.join(", ")
            );
        }
        let listing = heard.clone();
        drop(thread::spawn(move || {
            iter::repeat_with(|| {
                thread::sleep(poll);
                source.inputs()
            })
            .try_for_each(|listed| listing.send(Heard::Inputs(listed)))
        }));
        let (mut ears, mut failed, mut number, mut open, mut retry, mut said, mut told) = (
            false,
            0u32,
            0u64,
            None::<Open>,
            None::<Instant>,
            BTreeSet::new(),
            None::<String>,
        );
        loop {
            // a line to say once, and whether the open failed
            let mut noted = None::<(Option<String>, bool)>;
            match hearing.recv_timeout(poll) {
                Ok(Heard::Inputs(listed)) => inputs = listed,
                Ok(Heard::Line(Some(line))) => {
                    if line["ok"] != true {
                        println!("{line}");
                    }
                    if let Some(on) = line["ears"].as_bool() {
                        (ears, retry) = (on, None);
                    }
                }
                Ok(Heard::Line(None)) => return None,
                Ok(Heard::Opened(at, opened))
                    if open.as_ref().is_some_and(|held| held.number == at) =>
                {
                    let held = open.as_mut()?;
                    match opened {
                        Ok((rate, channels)) => {
                            let format = (usize::from(channels).max(1), Convert::from(rate));
                            (held.format, held.last, held.since) =
                                (Some(format), Instant::now(), Instant::now());
                        }
                        Err(cause) => {
                            noted = Some((Some(format!("{:?}: {cause}", held.name)), true));
                        }
                    }
                }
                Ok(Heard::Sound(at, when, data))
                    if open
                        .as_ref()
                        .is_some_and(|held| held.number == at && held.format.is_some()) =>
                {
                    let held = open.as_mut()?;
                    let &mut (channels, ref mut convert) = held.format.as_mut()?;
                    let channel = data.iter().step_by(channels).copied();
                    held.heard |= channel.clone().any(|sample| sample != 0.0);
                    // exact zeros before it are no signal, never sent as audio
                    if held.heard {
                        (failed, said) = (0, BTreeSet::new());
                        convert.feed(channel, &mut held.pcm);
                    }
                    // stamped by the sample count from the first; over 100 ms off the time the
                    // frame was captured (audio really missing) rebased. That time is the
                    // buffer's handover less every sample still pending, this frame's included,
                    // so a buffer handed over late in one piece misses nothing; never this loop's
                    // clock, which a write held up by the daemon would turn into holes
                    while held.pcm.len() >= FRAME {
                        let pending = u64::try_from(held.pcm.len()).unwrap_or_default();
                        let frame: Vec<u8> = held.pcm.drain(..FRAME).collect();
                        // bytes to microseconds: two a sample, 62.5 microseconds each
                        let (stamp, clock) = (
                            held.sent.saturating_mul(20_000),
                            when.saturating_sub(
                                pending
                                    .saturating_mul(125)
                                    .checked_div(4)
                                    .unwrap_or_default(),
                            ),
                        );
                        if clock.abs_diff(held.base.saturating_add(stamp)) > 100_000 {
                            held.base = clock.saturating_sub(stamp);
                        }
                        let sent = json!({"mic": held.name, "op": "frame", "pcm": hex(&frame), "t": held.base.saturating_add(stamp)});
                        writeln!(out.lock().ok()?, "{sent}").ok()?;
                        held.sent = held.sent.saturating_add(1);
                    }
                    // after the writes: a write held up is not the input fallen silent
                    held.last = Instant::now();
                }
                // an earlier open's, or a poll with nothing heard
                Ok(Heard::Opened(..) | Heard::Sound(..)) | Err(_) => {}
            }
            noted = noted.or_else(|| match open.as_ref() {
                // the open goes on: a grant's prompt holds it until answered
                Some(held) if held.format.is_none() && held.since.elapsed() >= deadline => Some((
                    Some(format!(
                        "{:?} has not opened in {} ms: the microphone is not granted yet; {GRANT}",
                        held.name,
                        deadline.as_millis()
                    )),
                    false,
                )),
                Some(held)
                    if held.format.is_some()
                        && (held.last.elapsed() >= poll
                            || !held.heard && held.since.elapsed() >= zeros) =>
                {
                    let why = format!(
                        "{:?} gave no signal (nothing, or only exact zeros) after it opened: the microphone is not granted, is muted, or is a closed lid's; {GRANT}",
                        held.name
                    );
                    // a heard input fallen silent: closed, nothing said
                    Some(((!held.heard).then_some(why), true))
                }
                _ => None,
            });
            if let Some((line, closes)) = noted {
                if let Some(why) = line
                    && said.insert(why.clone())
                {
                    eprintln!("rift device: {why}");
                }
                if closes {
                    let wait = poll.saturating_mul(2u32.saturating_pow(failed.min(6)));
                    (failed, open, retry) = (
                        failed.saturating_add(1),
                        None,
                        Instant::now().checked_add(wait),
                    );
                }
            }
            // the rule's reason said once each time it changes
            let (chosen, why) = pick(&inputs);
            if why != told {
                if let Some(line) = why.as_ref() {
                    eprintln!("rift device: {line}");
                }
                told = why;
            }
            let want = chosen.filter(|name| ears && inputs.names.contains(name));
            if open
                .as_ref()
                .is_some_and(|held| Some(&held.name) != want.as_ref())
            {
                open = None;
            }
            if let Some(name) =
                want.filter(|_| open.is_none() && retry.is_none_or(|at| at <= Instant::now()))
            {
                number = number.saturating_add(1);
                let (stop, stopped) = mpsc::channel();
                let (sounds, at, called) = (heard.clone(), number, name.clone());
                drop(thread::spawn(move || {
                    let opened = sounds.clone();
                    let ready = move |outcome| drop(opened.send(Heard::Opened(at, outcome)));
                    let sound = move |data: &[f32]| {
                        drop(sounds.try_send(Heard::Sound(at, now(), data.to_vec())));
                    };
                    source.hold(&called, Box::new(ready), Box::new(sound), stopped);
                }));
                open = Some(Open {
                    _stop: stop,
                    base: 0,
                    format: None,
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
    };
    let _: Option<()> = hear();
    println!("{}", error("unknown"));
}

//! `rift`. `serve` is the daemon, an append-only event log served over a unix socket
//! (docs/protocol.html, protocol 1) and an audio stream its devices feed onto one clock; `status`,
//! `quiet`, `ears`, `keep`, `feed`, `listen` and `read` are thin clients of that socket, `device`
//! one that plays a raw PCM file as frames or, on macOS, hears a microphone (`ear.rs`); `setup`
//! installs the release, signs it and loads its launchd agent, `upgrade` swaps in a newer release;
//! `version` prints the build.

#[cfg(target_os = "macos")]
mod ear;
#[cfg(target_os = "macos")]
mod mic;

use serde_json::{Value, json};
use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::signal_name;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs::{self, DirBuilder, OpenOptions, Permissions, TryLockError};
use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::iter;
use std::net::Shutdown;
use std::os::unix::fs::{
    DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::os::unix::net::{UnixListener, UnixStream};
use std::panic;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output, Stdio, id};
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A feed client this many events behind is dropped (docs/protocol.html#feed).
const BEHIND: usize = 1024;

/// A frame's bytes: 20 ms of 16 kHz mono 16-bit little-endian PCM.
const FRAME: usize = 640;

/// The longest request line, its `\n` not counted (docs/protocol.html#framing).
const LINE: u64 = 1 << 16;

/// The wire protocol this build speaks.
const PROTO: u64 = 1;

/// The `stopping` state, the one the request handler tests for.
const STOPPING: &str = "stopping";

/// The release (a checkout's build: the crate version), then `build.rs`'s short commit: hello's
/// and health's `version`.
const VERSION: &str = concat!(env!("RIFT_RELEASE"), "-", env!("RIFT_BUILD"));

/// Why the daemon stops (`signal TERM`, `shutdown request`), set once before `accept` is woken.
static STOP: OnceLock<String> = OnceLock::new();

/// What a connection does after its reply is written.
enum Next {
    /// Close it: a refused hello or a line past the limit.
    Close,
    /// Become a device: its replies and notices leave through this queue, by a writer of its own.
    Device(Receiver<String>),
    /// Become a feed: the lines it starts with, then every live one until the daemon drops it.
    Feed(Box<dyn Iterator<Item = String> + Send>, Receiver<String>),
    /// Read the next request.
    Go,
    /// The hello was accepted: read requests.
    Greet,
    /// The shutdown request: wake `accept` into the drain, then read on.
    Stop,
}

/// The daemon's state behind one mutex: an append and a feed's subscription each hold it, so a
/// replay and the live events after it are one sequence.
struct Live {
    /// The directory a segment leaving the window moves into, else null: it is deleted.
    archive: Value,
    /// This boot's number, derived from the log.
    boot: u64,
    /// Every open connection by key, for the drain's forced close and a slow feed's drop.
    conns: BTreeMap<u64, UnixStream>,
    /// Whether the ears are open: whether devices' frames are taken.
    ears: bool,
    /// Each feed's queue, its connection's key and its topic: `""` the events, `"ears"` a
    /// device's replies and notices, else the track a listener follows.
    feeds: Vec<(SyncSender<String>, u64, String)>,
    /// The closed segments still under `audio/` and not handed to the sweep, as their events, in
    /// log order.
    held: Vec<Value>,
    /// The last connection key handed out.
    keys: u64,
    /// The quiet setting.
    quiet: bool,
    /// The state root; `events.ndjson` and the segments under `audio/` are its durable state.
    root: PathBuf,
    /// A segment's length in samples, whole frames.
    segment: u64,
    /// The last event's `seq` in this boot.
    seq: u64,
    /// When this boot started, RFC 3339.
    since: String,
    /// The socket, which the shutdown request connects to so `accept` wakes.
    sock: PathBuf,
    /// `ready`, `degraded` (the last write to the log or a segment failed) or `stopping`.
    state: &'static str,
    /// The sweep's queue: each batch of segments leaving the window and the archive they move
    /// into, moved by a thread of its own, so no request waits on the archive's disk.
    sweeps: Sender<(Value, Vec<Value>)>,
    /// Every track of this boot, by name.
    tracks: BTreeMap<String, Track>,
    /// The window in seconds: a closed segment that ended longer ago leaves `audio/`.
    window: u64,
}

/// A track, one device's sound on the daemon's clock, in this boot.
#[derive(Default)]
struct Track {
    /// The key of the connection streaming it, while one does.
    conn: Option<u64>,
    /// Where its next frame goes on the timeline, once it has had one.
    end: Option<u64>,
    /// The microphone its last frame named, else null.
    mic: Value,
    /// The next segment's number.
    next: u64,
    /// The open segment's number, start and samples written.
    open: Option<(u64, u64, u64)>,
}

impl Live {
    /// Closes the open segment of `only`, else of every track, each with its event, then sweeps.
    /// A segment whose event fails to log stays open, as does every one after it.
    fn close(&mut self, only: Option<&str>) -> io::Result<()> {
        let boot = self.boot;
        let closing: Vec<(String, Value)> = self
            .tracks
            .iter()
            .filter(|entry| only.is_none_or(|one| one == entry.0.as_str()))
            .filter_map(|(name, track)| {
                let (num, start, samples) = track.open?;
                Some((name.clone(), bare(json!({"kind": "segment", "mic": track.mic, "n": num, "path": format!("audio/{boot}/{name}/{num}.pcm"), "samples": samples, "start": start, "track": name}))))
            })
            .collect();
        for (name, event) in closing {
            let logged = self.emit(&event)?;
            self.held.push(logged);
            self.tracks.entry(name).or_default().open = None;
        }
        self.sweep();
        Ok(())
    }

    /// Appends `event` to the log as this boot's next `seq` (the file opened, written whole and
    /// closed), then queues it to every feed of the events; the line as logged. A failed append
    /// is cut back to the log's length before it, so nothing changed, and the state is
    /// `degraded` until an append succeeds.
    fn emit(&mut self, event: &Value) -> io::Result<Value> {
        let seq = self.seq.saturating_add(1);
        let logged =
            json!({"boot": self.boot, "e": event, "seq": seq, "t": utc(SystemTime::now())});
        let line = format!("{logged}\n");
        let appended = append(&self.root.join("events.ndjson"), line.as_bytes());
        self.mark(appended)?;
        self.seq = seq;
        self.fan("", &line);
        Ok(logged)
    }

    /// Queues `line` to every feed of `topic`, dropping one that is full or gone.
    fn fan(&mut self, topic: &str, line: &str) {
        let conns = &self.conns;
        self.feeds.retain(|feed| {
            let kept = feed.2 != topic || feed.0.try_send(line.to_owned()).is_ok();
            if !kept {
                drop(conns.get(&feed.1).map(|conn| conn.shutdown(Shutdown::Both)));
            }
            kept
        });
    }

    /// The health reply: hello's fields, then the stream's and the switches'.
    fn health(&self) -> Value {
        let mut reply = self.hello();
        let devices: Vec<&String> = self
            .tracks
            .iter()
            .filter(|&(_, track)| track.conn.is_some())
            .map(|(name, _)| name)
            .collect();
        (
            reply["archive"],
            reply["devices"],
            reply["ears"],
            reply["quiet"],
            reply["seq"],
            reply["window"],
        ) = (
            self.archive.clone(),
            json!(devices),
            self.ears.into(),
            self.quiet.into(),
            self.seq.into(),
            self.window.into(),
        );
        reply
    }

    /// The hello reply.
    fn hello(&self) -> Value {
        json!({"boot": self.boot, "ok": true, "pid": id(), "proto": PROTO, "since": self.since, "state": self.state, "version": VERSION})
    }

    /// `wrote`, a write to the log or a segment, as it came; the state `ready` once one
    /// succeeds, `degraded` while the last failed.
    const fn mark<T>(&mut self, wrote: io::Result<T>) -> io::Result<T> {
        self.state = if wrote.is_ok() { "ready" } else { "degraded" };
        wrote
    }

    /// Hands every closed segment that ended more than the window ago to the sweep's thread,
    /// which moves it out of `audio/`.
    fn sweep(&mut self) {
        let cutoff = now().saturating_sub(self.window.saturating_mul(1_000_000));
        let due = self
            .held
            .extract_if(.., |event| end(&event["e"]) < cutoff)
            .collect();
        drop(self.sweeps.send((self.archive.clone(), due)));
    }
}

/// Appends `bytes` to `path`, created when absent, in one write: a failed one is cut back to the
/// length before it.
fn append(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    let len = file.metadata()?.len();
    file.write_all(bytes)
        .inspect_err(|_| drop(file.set_len(len)))
}

/// `event` without its null fields: a stream event names a microphone only when its frames did.
fn bare(mut event: Value) -> Value {
    (event.as_object_mut().into_iter())
        .for_each(|fields| fields.retain(|_, field| !field.is_null()));
    event
}

/// Where a stream event's span ends on the daemon's clock: its `start` plus its `samples` (a
/// `track` event has none) at 16 kHz.
#[expect(
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    reason = "a sample is 62.5 microseconds: the timeline's whole microseconds round down"
)]
fn end(event: &Value) -> u64 {
    let samples = event["samples"].as_u64().unwrap_or_default();
    event["start"]
        .as_u64()
        .unwrap_or_default()
        .saturating_add(samples.saturating_mul(125) / 2)
}

/// The error reply with `code`.
fn error(code: &str) -> Value {
    json!({"code": code, "ok": false})
}

/// The events in `bytes`, a log's contents: every whole line that parses, a torn last one not.
fn events(bytes: &[u8]) -> Vec<Value> {
    bytes
        .split_inclusive(|&byte| byte == b'\n')
        .filter(|line| line.ends_with(b"\n"))
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect()
}

/// `bytes` as lowercase hex, a frame's encoding on the wire.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02x}").unwrap_or_default();
        text
    })
}

/// The guard on `live`, a poisoned lock taken as it stands.
fn lock(live: &Mutex<Live>) -> MutexGuard<'_, Live> {
    live.lock().unwrap_or_else(PoisonError::into_inner)
}

/// rift's hotswap manifest (gigantic.dev/hotswap/): releases from jgeschwendt/rift under its
/// packslip signer and `rift setup` after each swap. No agent to kickstart and no drain step:
/// setup itself swaps the daemon with one `kickstart -k`, whose SIGTERM is the daemon's own drain,
/// and returns once the new one answers ready. The smoke is the new binary's `version`, then
/// `status` until the daemon answers with that same version, 30 s at most.
fn manifest() -> hotswap::Manifest {
    let owned = |words: &[&str]| words.iter().map(|&word| word.to_owned()).collect();
    hotswap::Manifest {
        bin: "rift".into(),
        repo: "jgeschwendt/rift".into(),
        setup: owned(&["rift", "setup"]),
        // packslip's fingerprint of the repository, printed by `packslip pin` on its first release
        signer: "ps1_fby5zwdz6ddvk4frxkhs4mmpny".into(),
        smoke: vec![
            owned(&["rift", "version"]),
            owned(&[
                "/bin/sh",
                "-c",
                r#"for _ in $(seq 30); do rift status | grep -qF "\"version\":\"$(rift version)\"" && exit 0; sleep 1; done; exit 1"#,
            ]),
        ],
        ..hotswap::Manifest::default()
    }
}

/// This host's clock in microseconds since the epoch: the daemon's, the stream's one timeline;
/// a device's own.
fn now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(u64::MAX)
}

/// The samples at 16 kHz in `micros`: a time's index on the timeline's sample grid.
#[expect(
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    reason = "a sample is 62.5 microseconds: a time between two samples takes the earlier"
)]
const fn samples(micros: u64) -> u64 {
    micros.saturating_mul(2) / 125
}

/// `$key` parsed, else `default`.
fn setting<T: FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `time` as RFC 3339 UTC with milliseconds.
#[expect(
    clippy::arithmetic_side_effects,
    clippy::integer_division,
    clippy::integer_division_remainder_used,
    reason = "calendar math on unsigned seconds since the epoch: nothing goes negative or overflows"
)]
fn utc(time: SystemTime) -> String {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let days = secs / 86_400 + 719_468;
    let doe = days % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    format!(
        "{:04}-{month:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        days / 146_097 * 400 + yoe + u64::from(month <= 2),
        doy - (153 * mp + 2) / 5 + 1,
        secs / 3600 % 24,
        secs / 60 % 60,
        secs % 60,
        since.subsec_millis()
    )
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let words: Vec<&str> = args.iter().skip(1).map(String::as_str).collect();
    let home = PathBuf::from(env::var_os("HOME").unwrap_or_default());
    // absolute: launchd runs the agent from /, not from where setup ran
    let root = env::var_os("RIFT_ROOT").map_or_else(
        || home.join(".local/state/rift"),
        |held| env::current_dir().unwrap_or_default().join(held),
    );
    let (rundir, sock) = (root.join("run"), root.join("run/rift.sock"));
    let asked = match *words.as_slice() {
        ["device", "ear", name, "--mic"] | ["device", "ear", name, "--mic", _] => {
            Some(json!({"kind": "ear", "name": name, "op": "device"}))
        }
        ["device", kind, name, _] | ["device", kind, name, _, "--fast"] => {
            Some(json!({"kind": kind, "name": name, "op": "device"}))
        }
        [op @ ("ears" | "quiet"), on @ ("on" | "off")] => Some(json!({"on": on == "on", "op": op})),
        ["feed"] => Some(json!({"after": null, "op": "feed"})),
        ["feed", "--after", at] => at.split_once(':').and_then(|(boot, seq)| {
            Some(json!({"after": {"boot": boot.parse::<u64>().ok()?, "seq": seq.parse::<u64>().ok()?}, "op": "feed"}))
        }),
        ["keep", secs] | ["keep", secs, _] => secs.parse::<u64>().ok().map(|window| {
            let archive = words
                .get(2)
                .map(|dir| env::current_dir().unwrap_or_default().join(dir));
            json!({"archive": archive, "op": "keep", "window": window})
        }),
        ["listen", track] => Some(json!({"op": "listen", "track": track})),
        ["read", track, from, to] => from.parse::<u64>().ok().zip(to.parse::<u64>().ok()).map(
            |(start, stop)| json!({"from": start, "op": "read", "to": stop, "track": track}),
        ),
        ["status"] => Some(json!({"op": "health"})),
        _ => None,
    };
    match (words.as_slice(), asked) {
        (&[verb, ..], Some(wanted)) => {
            let mic = verb == "device" && words.get(3) == Some(&"--mic");
            #[cfg(not(target_os = "macos"))]
            if mic {
                eprintln!("rift device: --mic is macOS only; a file-fed device plays anywhere");
                return ExitCode::from(2);
            }
            // a device's file, read before it connects
            let pcm = match words
                .get(3)
                .filter(|_| verb == "device" && !mic)
                .map(fs::read)
                .transpose()
            {
                Ok(pcm) => pcm.unwrap_or_default(),
                Err(cause) => {
                    eprintln!("rift device: {cause}");
                    return ExitCode::FAILURE;
                }
            };
            let answer = |reader: &mut BufReader<UnixStream>| -> io::Result<Value> {
                let mut line = String::new();
                let _: usize = reader.read_line(&mut line)?;
                serde_json::from_str(&line).map_err(io::Error::other)
            };
            // set once the request may have left: a failure after it is an unknown outcome
            let sent = Cell::new(false);
            let talk = || -> io::Result<(Value, Value, BufReader<UnixStream>)> {
                let mut stream = UnixStream::connect(&sock)?;
                stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                let mut reader = BufReader::new(stream.try_clone()?);
                let hello =
                    json!({"client": format!("rift/{VERSION}"), "op": "hello", "proto": PROTO});
                writeln!(stream, "{hello}")?;
                let greeting = answer(&mut reader)?;
                let greeted = greeting["ok"] == true;
                if greeted {
                    sent.set(true);
                    writeln!(stream, "{wanted}")?;
                }
                let reply = if greeted {
                    answer(&mut reader)?
                } else {
                    Value::Null
                };
                Ok((greeting, reply, reader))
            };
            match talk() {
                Ok((hello, _, _)) if hello["code"] == "proto" => {
                    eprintln!(
                        "rift {verb}: the daemon speaks protocol {} ({}), this client protocol {PROTO} ({VERSION}); fix: rift setup, so both are one release",
                        hello["proto"],
                        hello["version"].as_str().unwrap_or_default()
                    );
                    ExitCode::FAILURE
                }
                Ok((hello, reply, mut reader)) if hello["ok"] == true => {
                    if hello["version"] != VERSION {
                        eprintln!(
                            "rift {verb}: the daemon is {}, this client {VERSION}; fix: rift setup",
                            hello["version"].as_str().unwrap_or_default()
                        );
                    }
                    if verb == "device" && reply["ok"] == true {
                        println!("{reply}");
                        #[cfg(target_os = "macos")]
                        if mic {
                            return ear::run(reader, &reply, words.get(4).copied());
                        }
                        let fast = words.len() == 5;
                        // a reply carrying `ping` is answered with this device's clock
                        let pong = |conn: &mut BufReader<UnixStream>, ping: &Value| {
                            writeln!(
                                conn.get_mut(),
                                "{}",
                                json!({"op": "pong", "ping": ping, "t": now()})
                            )
                        };
                        let frames = pcm.chunks_exact(FRAME);
                        // stamped from now in real time, or, sent as fast as accepted, ending now
                        let mut base = now().saturating_sub(
                            u64::try_from(frames.len())
                                .unwrap_or_default()
                                .saturating_mul(20_000)
                                .saturating_mul(u64::from(fast)),
                        );
                        let played = pong(&mut reader, &reply["ping"]).and_then(|()| {
                            (0u64..).step_by(20_000).zip(frames).try_for_each(|(at, frame)| {
                                // paced on the clock its pongs read; over 100 ms off it (a pause,
                                // a sleep, a clock step) plays on from now, the gap a hole
                                if !fast {
                                    let clock = now();
                                    if clock.abs_diff(base.saturating_add(at)) > 100_000 {
                                        base = clock.saturating_sub(at);
                                    }
                                    thread::sleep(Duration::from_micros(
                                        base.saturating_add(at).saturating_sub(clock),
                                    ));
                                }
                                writeln!(
                                    reader.get_mut(),
                                    "{}",
                                    json!({"op": "frame", "pcm": hex(frame), "t": base.saturating_add(at)})
                                )?;
                                // notices and refusals printed, pings answered, until its reply
                                loop {
                                    let line = answer(&mut reader)?;
                                    if line["ping"].is_u64() {
                                        pong(&mut reader, &line["ping"])?;
                                    }
                                    if line["ok"] != true {
                                        println!("{line}");
                                    }
                                    if line.get("ok").is_some() {
                                        break Ok(());
                                    }
                                }
                            })
                        });
                        return played.map_or_else(
                            |_| {
                                println!("{}", error("unknown"));
                                ExitCode::FAILURE
                            },
                            |()| ExitCode::SUCCESS,
                        );
                    }
                    if matches!(verb, "feed" | "listen" | "read") && reply["ok"] == true {
                        if reply["reset"] == true {
                            eprintln!(
                                "rift feed: the log does not hold that position; from this boot's first event"
                            );
                        }
                        drop(reader.get_ref().set_read_timeout(None));
                        drop(
                            reader
                                .lines()
                                .map_while(Result::ok)
                                .try_for_each(|line| writeln!(io::stdout(), "{line}")),
                        );
                        return ExitCode::SUCCESS;
                    }
                    println!("{reply}");
                    ExitCode::from(u8::from(reply["ok"] != true))
                }
                Ok((refused, _, _)) => {
                    println!("{refused}");
                    ExitCode::FAILURE
                }
                Err(_) => {
                    println!("{}", error(if sent.get() { "unknown" } else { "down" }));
                    ExitCode::FAILURE
                }
            }
        }
        (&["serve"], None) => {
            let (log, tmp, pid) = (
                rundir.join("serve.log"),
                rundir.join("serve.new"),
                rundir.join("serve.pid"),
            );
            let (cap, err) = (setting("RIFT_LOG_CAP", 1u64 << 20), root.join("serve.err"));
            // serve.log, and launchd's serve.err beside it, each rotate to `.1` at the cap
            let write = move |mut line: Value| {
                line["at"] = utc(SystemTime::now()).into();
                for file in [&log, &err] {
                    if fs::metadata(file).is_ok_and(|meta| meta.len() >= cap) {
                        drop(fs::rename(file, file.with_added_extension("1")));
                    }
                }
                drop(
                    OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&log)
                        .and_then(|mut file| file.write_all(format!("{line}\n").as_bytes())),
                );
            };
            let hook = write.clone();
            panic::set_hook(Box::new(move |info| {
                hook(json!({"event": "panic", "reason": info.to_string()}));
            }));
            let started = || -> io::Result<_> {
                // first: a stop signal during the boot waits for the drain, never kills
                let signals = Signals::new([SIGHUP, SIGINT, SIGTERM])?;
                if sock.as_os_str().len() > 103 {
                    return Err(io::Error::other(format!(
                        "{} is over a unix socket's 103 bytes; shorten RIFT_ROOT",
                        sock.display()
                    )));
                }
                DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&root)?;
                drop(DirBuilder::new().mode(0o700).create(&rundir));
                // append, not create: a second instance must not even truncate the lock file
                let held = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(rundir.join("serve.lock"))?;
                match held.try_lock() {
                    Err(TryLockError::WouldBlock) => {
                        return Err(io::Error::other(format!(
                            "already running (pid {}, by its pid file)",
                            fs::read_to_string(&pid).unwrap_or_default().trim()
                        )));
                    }
                    other => other?,
                }
                drop(fs::remove_file(&tmp));
                let listener = UnixListener::bind(&tmp)?;
                let dir = fs::metadata(&rundir)?;
                if dir.uid() != fs::metadata(&tmp)?.uid() || dir.mode() & 0o077 != 0 {
                    fs::remove_file(&tmp)?;
                    return Err(io::Error::other(format!(
                        "{} must be this user's, mode 0700",
                        rundir.display()
                    )));
                }
                // the log: a torn last line cut off, then the boot and quiet it holds
                let events_path = root.join("events.ndjson");
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&events_path)?;
                let bytes = fs::read(&events_path)?;
                let whole = bytes
                    .iter()
                    .rposition(|&byte| byte == b'\n')
                    .map_or(0, |at| at.saturating_add(1));
                file.set_len(u64::try_from(whole).map_err(io::Error::other)?)?;
                let held_events = events(&bytes);
                let last = |kind: &str| {
                    held_events
                        .iter()
                        .rev()
                        .find(|event| event["e"]["kind"] == kind)
                        .map(|event| &event["e"])
                };
                let kept = last("keep");
                let closed: Vec<&Value> = held_events
                    .iter()
                    .filter(|event| event["e"]["kind"] == "segment")
                    .collect();
                let (sweeps, swept) = mpsc::channel::<(Value, Vec<Value>)>();
                let live: &'static Mutex<Live> = Box::leak(Box::new(Mutex::new(Live {
                    archive: kept.map_or(Value::Null, |keep| keep["archive"].clone()),
                    boot: held_events
                        .last()
                        .and_then(|event| event["boot"].as_u64())
                        .unwrap_or(0)
                        .saturating_add(1),
                    conns: BTreeMap::new(),
                    ears: last("ears").is_some_and(|ears| ears["on"] == true),
                    feeds: Vec::new(),
                    held: closed
                        .iter()
                        .filter(|event| {
                            root.join(event["e"]["path"].as_str().unwrap_or_default())
                                .exists()
                        })
                        .map(|&event| event.clone())
                        .collect(),
                    keys: 0,
                    quiet: last("quiet").is_some_and(|quiet| quiet["on"] == true),
                    root: root.clone(),
                    segment: setting("RIFT_SEGMENT_MS", 10_000u64)
                        .checked_div(20)
                        .unwrap_or_default()
                        .max(1)
                        .saturating_mul(320),
                    seq: 0,
                    since: utc(SystemTime::now()),
                    sock: sock.clone(),
                    state: "ready",
                    sweeps,
                    tracks: BTreeMap::new(),
                    window: kept.map_or(600, |keep| keep["window"].as_u64().unwrap_or_default()),
                })));
                // the sweep's thread, off the lock: into the archive, its event appended to the
                // archive's index, else deleted; one that fails goes back for the next sweep
                let moving = root.clone();
                drop(thread::spawn(move || {
                    for (archive, due) in swept {
                        let failed: Vec<Value> = due
                            .into_iter()
                            .filter(|event| {
                                let path = event["e"]["path"].as_str().unwrap_or_default();
                                let from = moving.join(path);
                                archive
                                    .as_str()
                                    .map(Path::new)
                                    .map_or(Ok(()), |under| {
                                        let into = under.join(path);
                                        into.parent().map_or(Ok(()), fs::create_dir_all)?;
                                        let _: u64 = fs::copy(&from, &into)?;
                                        append(
                                            &under.join("index.ndjson"),
                                            format!("{event}\n").as_bytes(),
                                        )
                                    })
                                    .and_then(|()| fs::remove_file(&from))
                                    .is_err()
                            })
                            .collect();
                        drop(lock(live).held.splice(0..0, failed));
                    }
                }));
                // the window holds while nothing closes a segment: ears off, no device, idle
                drop(thread::spawn(move || -> ! {
                    loop {
                        thread::sleep(Duration::from_secs(1));
                        lock(live).sweep();
                    }
                }));
                // the pid file first: past the boot event only a failed append or rename refuses
                fs::write(&pid, id().to_string())?;
                let mut guard = lock(live);
                drop(guard.emit(&json!({"kind": "boot", "pid": id(), "version": VERSION}))?);
                // a segment a kill left open: cut to whole frames and closed with its event, its
                // start where its track's last event of that boot ended
                let mut torn: Vec<String> = fs::read_dir(root.join("audio"))
                    .into_iter()
                    .flatten()
                    .flatten()
                    .flat_map(|boot| fs::read_dir(boot.path()).into_iter().flatten().flatten())
                    .flat_map(|track| fs::read_dir(track.path()).into_iter().flatten().flatten())
                    .filter_map(|entry| {
                        Some(entry.path().strip_prefix(&root).ok()?.to_str()?.to_owned())
                    })
                    .filter(|path| {
                        Path::new(path).extension().is_some_and(|ext| ext == "pcm")
                            && !closed
                                .iter()
                                .any(|event| event["e"]["path"] == path.as_str())
                    })
                    .collect();
                torn.sort();
                for path in torn {
                    let (folder, name) = path.rsplit_once('/').unwrap_or_default();
                    let (top, track) = folder.rsplit_once('/').unwrap_or_default();
                    let boot: u64 = top.trim_start_matches("audio/").parse().unwrap_or_default();
                    // one that will not open or cut (a directory, a read-only file) stays as it
                    // is: refusing would put the daemon in a boot loop under KeepAlive
                    let Ok(frames) = OpenOptions::new()
                        .write(true)
                        .open(root.join(&path))
                        .and_then(|handle| {
                            let frames = handle
                                .metadata()?
                                .len()
                                .checked_div(640)
                                .unwrap_or_default();
                            handle.set_len(frames.saturating_mul(640))?;
                            Ok(frames)
                        })
                    else {
                        continue;
                    };
                    // and its microphone that event's: a microphone's change opens with a hole
                    let (start, mic) = held_events
                        .iter()
                        .rev()
                        .find(|event| {
                            event["boot"] == boot
                                && event["e"]["track"] == track
                                && event["e"]["start"].is_u64()
                        })
                        .map_or((0, Value::Null), |event| {
                            (end(&event["e"]), event["e"]["mic"].clone())
                        });
                    let num: u64 = name.trim_end_matches(".pcm").parse().unwrap_or_default();
                    let logged = guard.emit(&bare(json!({"kind": "segment", "mic": mic, "n": num, "path": path, "samples": frames.saturating_mul(320), "start": start, "track": track})))?;
                    guard.held.push(logged);
                }
                guard.sweep();
                drop(guard);
                fs::rename(&tmp, &sock)?;
                Ok((held, listener, live, signals))
            };
            let (_held, listener, live, mut signals) = match started() {
                Ok(held) => held,
                Err(cause) => {
                    write(json!({"event": "refused", "reason": cause.to_string()}));
                    eprintln!("rift serve: {cause}");
                    return ExitCode::FAILURE;
                }
            };
            let wake = sock.clone();
            drop(thread::spawn(move || {
                for signal in signals.forever() {
                    drop(STOP.set(format!(
                        "signal {}",
                        signal_name(signal)
                            .unwrap_or_default()
                            .trim_start_matches("SIG")
                    )));
                    drop(UnixStream::connect(&wake));
                }
            }));
            write(json!({"event": "start", "pid": id(), "version": VERSION}));
            // the ready line: whoever spawned the daemon reads it, so nothing polls
            println!("{}", lock(live).health());
            let reason = loop {
                let accepted = listener.accept();
                if let Some(why) = STOP.get() {
                    break why.clone();
                }
                // a failed accept (EMFILE, ECONNABORTED) skips one connection, never stops; the
                // backoff first, since opening serve.log takes the descriptor EMFILE just denied
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(why) => {
                        thread::sleep(Duration::from_millis(100));
                        write(json!({"event": "accept", "reason": why.to_string()}));
                        continue;
                    }
                };
                let note = write.clone();
                #[expect(
                    clippy::cognitive_complexity,
                    reason = "a connection is one read loop over one match of its requests: split, each arm would be a function called once"
                )]
                let connection = move || -> io::Result<()> {
                    let (read, listed) = (stream.try_clone()?, stream.try_clone()?);
                    let key = {
                        let mut guard = lock(live);
                        guard.keys = guard.keys.saturating_add(1);
                        let key = guard.keys;
                        drop(guard.conns.insert(key, listed));
                        key
                    };
                    let (mut reader, mut writer, mut greeted) =
                        (BufReader::new(read), stream, false);
                    // a device's track, its clock and the daemon's at one instant (by its last
                    // pong; until one, no frame is placed), its last ping, and the queue its
                    // writer drains
                    let (mut device, mut clock, mut pinged, mut queue) = (
                        None::<String>,
                        None::<(u64, u64)>,
                        Instant::now(),
                        None::<SyncSender<String>>,
                    );
                    let every = Duration::from_millis(setting("RIFT_PING_MS", 10_000));
                    loop {
                        let mut raw = Vec::new();
                        drop((&mut reader).take(LINE + 1).read_until(b'\n', &mut raw));
                        let whole = raw.ends_with(b"\n");
                        let over = !whole && u64::try_from(raw.len()).unwrap_or(u64::MAX) > LINE;
                        // end of stream, or a read error, mid-line or between lines
                        if !whole && !over {
                            break;
                        }
                        let request: Value = serde_json::from_slice(&raw).unwrap_or_default();
                        let id_ok = request["id"].is_null()
                            || request["id"].is_string()
                            || request["id"].is_number();
                        // a track is `<kind>-<name>`
                        let well = |kind: &str, name: &str| {
                            matches!(kind, "ear" | "mouth")
                                && (1..=64).contains(&name.len())
                                && name.bytes().all(|byte| {
                                    byte.is_ascii_alphanumeric() || b"-_".contains(&byte)
                                })
                        };
                        let track = request["track"].as_str().unwrap_or_default();
                        let tracked = track
                            .split_once('-')
                            .is_some_and(|(kind, name)| well(kind, name));
                        let named = request["kind"]
                            .as_str()
                            .zip(request["name"].as_str())
                            .filter(|&(kind, name)| well(kind, name))
                            .map(|(kind, name)| format!("{kind}-{name}"));
                        let pcm = request["pcm"]
                            .as_str()
                            // lowercase digits only: from_str_radix alone takes uppercase and a `+`
                            .filter(|text| {
                                (request["mic"].is_null() || request["mic"].is_string())
                                    && text.len() == FRAME * 2
                                    && text
                                        .bytes()
                                        .all(|digit| matches!(digit, b'0'..=b'9' | b'a'..=b'f'))
                            })
                            .and_then(|text| {
                                text.as_bytes()
                                    .chunks(2)
                                    .map(|pair| {
                                        u8::from_str_radix(str::from_utf8(pair).ok()?, 16).ok()
                                    })
                                    .collect::<Option<Vec<u8>>>()
                            });
                        let mut guard = lock(live);
                        let (result, next) = match (greeted, request["op"].as_str()) {
                            _ if over => (Ok(error("too_long")), Next::Close),
                            _ if !request.is_object() || !id_ok => (
                                Ok(error("bad_request")),
                                if greeted { Next::Go } else { Next::Close },
                            ),
                            (false, Some("hello"))
                                if request["client"].is_string() && request["proto"] == PROTO =>
                            {
                                (Ok(guard.hello()), Next::Greet)
                            }
                            (false, Some("hello"))
                                if request["client"].is_string() && request["proto"].is_u64() =>
                            {
                                (
                                    Ok(
                                        json!({"code": "proto", "ok": false, "proto": PROTO, "version": VERSION}),
                                    ),
                                    Next::Close,
                                )
                            }
                            (false, _) => (Ok(error("bad_request")), Next::Close),
                            (true, Some(op)) if guard.state == STOPPING && op != "health" => {
                                (Ok(error(STOPPING)), Next::Go)
                            }
                            // a device's connection carries its frames, never a stream of replies
                            (true, Some("device" | "feed" | "listen" | "read"))
                                if device.is_some() =>
                            {
                                (Ok(error("bad_request")), Next::Go)
                            }
                            (true, Some("device"))
                                if named.as_ref().is_some_and(|name| {
                                    guard
                                        .tracks
                                        .get(name)
                                        .is_some_and(|held| held.conn.is_some())
                                }) =>
                            {
                                (Ok(error("taken")), Next::Go)
                            }
                            (true, Some("device")) if named.is_some() => {
                                let name = named.unwrap_or_default();
                                let (sender, incoming) = mpsc::sync_channel(BEHIND);
                                guard.tracks.entry(name.clone()).or_default().conn = Some(key);
                                guard.feeds.push((sender.clone(), key, "ears".to_owned()));
                                queue = Some(sender);
                                let reply = json!({"ears": guard.ears, "ok": true, "ping": now(), "track": name});
                                device = Some(name);
                                (Ok(reply), Next::Device(incoming))
                            }
                            (true, Some(op @ ("ears" | "quiet"))) if request["on"].is_boolean() => {
                                let on = request["on"] == true;
                                let changed = on
                                    != if op == "ears" {
                                        guard.ears
                                    } else {
                                        guard.quiet
                                    };
                                let toggled = (|| {
                                    if changed {
                                        if op == "ears" && !on {
                                            guard.close(None)?;
                                        }
                                        drop(guard.emit(&json!({"kind": op, "on": on}))?);
                                        // devices hear of it only once it is logged
                                        if op == "ears" {
                                            guard
                                                .fan("ears", &format!("{}\n", json!({"ears": on})));
                                        }
                                        *(if op == "ears" {
                                            &mut guard.ears
                                        } else {
                                            &mut guard.quiet
                                        }) = on;
                                    }
                                    Ok(json!({"changed": changed, "ok": true, "seq": guard.seq}))
                                })();
                                (toggled, Next::Go)
                            }
                            (true, Some("frame")) if device.is_some() && request["t"].is_u64() => {
                                match (pcm, clock) {
                                    (None, _) => (Ok(error("bad_frame")), Next::Go),
                                    (_, None) => (Ok(error("unsynced")), Next::Go),
                                    _ if !guard.ears => (Ok(error("closed")), Next::Go),
                                    (Some(bytes), Some((theirs, ours))) => {
                                        let (name, state) =
                                            (device.clone().unwrap_or_default(), &mut *guard);
                                        // never after it arrived: a clock stepped ahead between
                                        // pongs leaves no hole into the future to stay ahead by
                                        let at = request["t"]
                                            .as_u64()
                                            .unwrap_or_default()
                                            .saturating_add(ours)
                                            .saturating_sub(theirs)
                                            .min(now());
                                        let placed = (|| {
                                            // logged only once the frame is written, so a refused one changes nothing
                                            let (known, mic) =
                                                (state.tracks.get(&name), &request["mic"]);
                                            let (start, event) = match known
                                                .and_then(|held| held.end)
                                            {
                                                None => (
                                                    at,
                                                    Some(bare(
                                                        json!({"kind": "track", "mic": mic, "start": at, "track": name}),
                                                    )),
                                                ),
                                                // half a frame or more past the track's end is a
                                                // hole, as is another microphone's first frame
                                                Some(last)
                                                    if at >= last.saturating_add(10_000)
                                                        || known.is_some_and(|held| {
                                                            held.mic != *mic
                                                        }) =>
                                                {
                                                    let gap = bare(
                                                        json!({"kind": "hole", "mic": mic, "samples": samples(at.saturating_sub(last)), "start": last, "track": name}),
                                                    );
                                                    (end(&gap), Some(gap))
                                                }
                                                // as far before it is a clock running fast, dropped
                                                // so the track never runs ahead of the device
                                                Some(last) if at.saturating_add(10_000) <= last => {
                                                    return Ok(error("ahead"));
                                                }
                                                Some(last) => (last, None),
                                            };
                                            // the open segment, unless a hole closes it, else the next, which counts only once written
                                            let (num, first, written) = known
                                                .and_then(|held| held.open)
                                                .filter(|_| event.is_none())
                                                .unwrap_or_else(|| {
                                                    (known.map_or(0, |held| held.next), start, 0)
                                                });
                                            let dir = state
                                                .root
                                                .join(format!("audio/{}/{name}", state.boot));
                                            let file = dir.join(format!("{num}.pcm"));
                                            state.mark(
                                                fs::create_dir_all(&dir)
                                                    .and_then(|()| append(&file, &bytes)),
                                            )?;
                                            if let Some(opening) = event {
                                                // a segment its event never logged is unwritten
                                                let logged = state
                                                    .close(Some(&name))
                                                    .and_then(|()| state.emit(&opening));
                                                drop(logged.inspect_err(|_| {
                                                    drop(fs::remove_file(&file));
                                                })?);
                                            }
                                            let (held, full) = (
                                                state.tracks.entry(name.clone()).or_default(),
                                                written.saturating_add(320),
                                            );
                                            (held.next, held.open, held.end, held.mic) = (
                                                num.saturating_add(1),
                                                Some((num, first, full)),
                                                Some(start.saturating_add(20_000)),
                                                request["mic"].clone(),
                                            );
                                            // the frame is placed: a close that fails to log
                                            // leaves the segment open for the next frame's close
                                            if full >= state.segment {
                                                drop(state.close(Some(&name)));
                                            }
                                            state.fan(
                                                &name,
                                                &format!(
                                                    "{}\n",
                                                    json!({"pcm": hex(&bytes), "t": start})
                                                ),
                                            );
                                            let mut reply = json!({"ok": true, "t": start});
                                            if pinged.elapsed() >= every {
                                                (reply["ping"], pinged) =
                                                    (now().into(), Instant::now());
                                            }
                                            Ok(reply)
                                        })();
                                        (placed, Next::Go)
                                    }
                                }
                            }
                            (true, Some("health")) => (Ok(guard.health()), Next::Go),
                            (true, Some("keep"))
                                if request["window"].is_u64()
                                    && (request["archive"].is_null()
                                        || request["archive"]
                                            .as_str()
                                            .is_some_and(|dir| Path::new(dir).is_absolute())) =>
                            {
                                let kept = guard
                                    .emit(&json!({"archive": request["archive"], "kind": "keep", "window": request["window"]}))
                                    .map(|_| {
                                        (guard.archive, guard.window) = (
                                            request["archive"].clone(),
                                            request["window"].as_u64().unwrap_or_default(),
                                        );
                                        guard.sweep();
                                        json!({"ok": true, "seq": guard.seq})
                                    });
                                (kept, Next::Go)
                            }
                            (true, Some("listen")) if tracked => {
                                let (sender, incoming) = mpsc::sync_channel(BEHIND);
                                guard.feeds.push((sender, key, track.to_owned()));
                                (
                                    Ok(json!({"ok": true})),
                                    Next::Feed(Box::new(iter::empty()), incoming),
                                )
                            }
                            // a pong is answered by nothing
                            (true, Some("pong"))
                                if device.is_some()
                                    && request["ping"].is_u64()
                                    && request["t"].is_u64() =>
                            {
                                clock = Some((
                                    request["t"].as_u64().unwrap_or_default(),
                                    request["ping"].as_u64().unwrap_or_default().midpoint(now()),
                                ));
                                (Ok(Value::Null), Next::Go)
                            }
                            (true, Some("read"))
                                if tracked
                                    && matches!(
                                        (request["from"].as_u64(), request["to"].as_u64()),
                                        (Some(from), Some(to)) if from < to
                                    ) =>
                            {
                                let (from, clock_now) =
                                    (request["from"].as_u64().unwrap_or_default(), now());
                                if from
                                    < clock_now
                                        .saturating_sub(guard.window.saturating_mul(1_000_000))
                                {
                                    (Ok(error("gone")), Next::Go)
                                } else {
                                    // on the sample grid: the range, and each segment of the track
                                    // inside it by its start, samples and file, last first
                                    let (first, last) = (
                                        samples(from),
                                        samples(
                                            request["to"]
                                                .as_u64()
                                                .unwrap_or_default()
                                                .min(clock_now),
                                        ),
                                    );
                                    let open = guard.tracks.get(track).and_then(|held| held.open).map(|(num, start, written)| {
                                        json!({"path": format!("audio/{}/{track}/{num}.pcm", guard.boot), "samples": written, "start": start, "track": track})
                                    });
                                    let mut spans: Vec<(u64, PathBuf)> = guard
                                        .held
                                        .iter()
                                        .map(|event| &event["e"])
                                        .chain(open.as_ref())
                                        .filter(|held| held["track"] == track)
                                        .map(|held| {
                                            (
                                                samples(held["start"].as_u64().unwrap_or_default()),
                                                samples(end(held)),
                                                guard.root.join(
                                                    held["path"].as_str().unwrap_or_default(),
                                                ),
                                            )
                                        })
                                        .filter(|span| span.0 < last && span.1 > first)
                                        .map(|(start, _, path)| (start, path))
                                        .collect();
                                    spans.sort();
                                    spans.reverse();
                                    // a frame at a time, silence wherever nothing is held, each
                                    // segment read once as the range reaches it and let go as it
                                    // passes: memory is a segment or two, whatever the range
                                    let mut loaded: Vec<(u64, u64, Vec<u8>)> = Vec::new();
                                    let bytes = |count: u64| {
                                        usize::try_from(count.saturating_mul(2)).unwrap_or_default()
                                    };
                                    let frames = (first..last)
                                        .step_by(320)
                                        .zip((from..).step_by(20_000))
                                        .map(move |(at, micros)| {
                                            let stop = at.saturating_add(320).min(last);
                                            while let Some((start, path)) =
                                                spans.pop_if(|span| span.0 < stop)
                                            {
                                                let sound = fs::read(path).unwrap_or_default();
                                                let count =
                                                    u64::try_from(sound.chunks_exact(2).len())
                                                        .unwrap_or_default();
                                                loaded.push((
                                                    start,
                                                    start.saturating_add(count),
                                                    sound,
                                                ));
                                            }
                                            loaded.retain(|held| held.1 > at);
                                            let mut frame =
                                                vec![0u8; bytes(stop.saturating_sub(at))];
                                            for held_span in &loaded {
                                                let (start, finish, sound) =
                                                    (held_span.0, held_span.1, &held_span.2);
                                                let (low, high) = (at.max(start), stop.min(finish));
                                                if let (Some(into), Some(held)) = (
                                                    frame.get_mut(
                                                        bytes(low.saturating_sub(at))
                                                            ..bytes(high.saturating_sub(at)),
                                                    ),
                                                    sound.get(
                                                        bytes(low.saturating_sub(start))
                                                            ..bytes(high.saturating_sub(start)),
                                                    ),
                                                ) {
                                                    into.copy_from_slice(held);
                                                }
                                            }
                                            format!(
                                                "{}\n",
                                                json!({"pcm": hex(&frame), "t": micros})
                                            )
                                        });
                                    // the range, then the end of stream
                                    let (_, ended) = mpsc::sync_channel(0);
                                    (Ok(json!({"ok": true})), Next::Feed(Box::new(frames), ended))
                                }
                            }
                            (true, Some("feed"))
                                if request.get("after").is_some_and(|after| {
                                    after.is_null()
                                        || after["boot"].is_u64() && after["seq"].is_u64()
                                }) =>
                            {
                                let (after, boot, id) =
                                    (request["after"].clone(), guard.boot, request["id"].clone());
                                let path = guard.root.join("events.ndjson");
                                // the log as of the subscription, read and replied to off the lock:
                                // appends hold it, so every event past this length reaches the queue
                                let len = fs::metadata(&path).map_or(0, |meta| meta.len());
                                let backlog = iter::once(()).flat_map(move |()| {
                                    let mut bytes = Vec::new();
                                    drop(fs::File::open(&path).and_then(|file| {
                                        file.take(len).read_to_end(&mut bytes)
                                    }));
                                    let held_events = events(&bytes);
                                    let found = held_events.iter().position(|event| {
                                        event["boot"] == after["boot"]
                                            && event["seq"] == after["seq"]
                                    });
                                    let mut reply = json!({"ok": true, "reset": found.is_none() && !after.is_null()});
                                    if !id.is_null() {
                                        reply["id"] = id.clone();
                                    }
                                    iter::once(format!("{reply}\n")).chain(
                                        held_events
                                            .into_iter()
                                            .enumerate()
                                            .filter(move |entry| {
                                                found.map_or(entry.1["boot"] == boot, |start| {
                                                    entry.0 > start
                                                })
                                            })
                                            .map(|(_, event)| format!("{event}\n")),
                                    )
                                });
                                let (sender, incoming) = mpsc::sync_channel(BEHIND);
                                guard.feeds.push((sender, key, String::new()));
                                (Ok(Value::Null), Next::Feed(Box::new(backlog), incoming))
                            }
                            (true, Some("shutdown")) => (Ok(json!({"ok": true})), Next::Stop),
                            (
                                true,
                                Some(
                                    "device" | "ears" | "feed" | "frame" | "keep" | "listen"
                                    | "pong" | "quiet" | "read",
                                )
                                | None,
                            ) => (Ok(error("bad_request")), Next::Go),
                            (true, Some(_)) => (Ok(error("unknown_op")), Next::Go),
                        };
                        drop(guard);
                        let mut reply = result.unwrap_or_else(|cause| {
                            note(json!({"event": "degraded", "reason": cause.to_string()}));
                            error("io")
                        });
                        if !reply.is_null() {
                            if id_ok && !request["id"].is_null() {
                                reply["id"] = request["id"].clone();
                            }
                            // a vanished client: the next read ends the loop, a feed's first write the feed
                            let said = format!("{reply}\n");
                            match queue.as_ref() {
                                Some(sender) => drop(sender.send(said)),
                                None => drop(writer.write_all(said.as_bytes())),
                            }
                        }
                        match next {
                            Next::Close => break,
                            Next::Device(incoming) => drop(writer.try_clone().map(|mut out| {
                                thread::spawn(move || {
                                    incoming
                                        .into_iter()
                                        .try_for_each(|line| out.write_all(line.as_bytes()))
                                })
                            })),
                            Next::Feed(backlog, incoming) => {
                                // a vanished client drops its queue at once, so `incoming` ends
                                drop(thread::spawn(move || {
                                    drop(io::copy(&mut reader, &mut io::sink()));
                                    lock(live).feeds.retain(|feed| feed.1 != key);
                                }));
                                drop(
                                    backlog
                                        .into_iter()
                                        .chain(incoming)
                                        .try_for_each(|event| writer.write_all(event.as_bytes())),
                                );
                                break;
                            }
                            Next::Go => {}
                            Next::Greet => greeted = true,
                            Next::Stop => {
                                drop(STOP.set("shutdown request".to_owned()));
                                drop(UnixStream::connect(&lock(live).sock));
                            }
                        }
                    }
                    drop(writer.shutdown(Shutdown::Both));
                    let mut guard = lock(live);
                    drop(guard.conns.remove(&key));
                    guard.feeds.retain(|feed| feed.1 != key);
                    // a device's open segment closes as it leaves
                    if let Some(name) = device {
                        drop(guard.close(Some(&name)));
                        guard.tracks.entry(name).or_default().conn = None;
                    }
                    drop(guard);
                    Ok(())
                };
                drop(thread::spawn(connection));
            };
            // stop accepting, the open segments closed, then the last event, then the feeds end
            // once their queues are sent
            drop(listener);
            {
                let mut guard = lock(live);
                drop(guard.close(None));
                drop(guard.emit(&json!({"kind": "stop", "reason": reason})));
                guard.state = STOPPING;
                guard.feeds.clear();
            }
            // open connections may still ask for health inside the budget; then they are cut
            let (begun, budget) = (
                Instant::now(),
                Duration::from_millis(setting("RIFT_DRAIN_MS", 1000)),
            );
            while begun.elapsed() < budget && !lock(live).conns.is_empty() {
                thread::sleep(Duration::from_millis(10));
            }
            for conn in lock(live).conns.values() {
                drop(conn.shutdown(Shutdown::Both));
            }
            if fs::read_to_string(&pid).is_ok_and(|text| text == id().to_string()) {
                drop(fs::remove_file(&pid));
                drop(fs::remove_file(&sock));
            }
            write(json!({"event": "exit", "reason": reason}));
            ExitCode::SUCCESS
        }
        (&["setup"] | &["setup", "--print" | "--remove"], None) => {
            // hotswap's link through its `current`, never this executable: a checkout's setup
            // wires the installed release
            let exe = home.join(".local/bin/rift");
            let label = "dev.gigantic.rift";
            let plist = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>EnvironmentVariables</key>
  <dict><key>RIFT_ROOT</key><string>{}</string></dict>
  <key>ExitTimeOut</key>
  <integer>3</integer>
  <key>KeepAlive</key>
  <dict><key>SuccessfulExit</key><false/></dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array><string>{}</string><string>serve</string></array>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardErrorPath</key>
  <string>{}</string>
  <key>ThrottleInterval</key>
  <integer>10</integer>
</dict>
</plist>
"#,
                root.display(),
                exe.display(),
                // beside run/, not in it: launchd creates the parent of this path 0744, which serve refuses
                root.join("serve.err").display()
            );
            let path = home.join(format!("Library/LaunchAgents/{label}.plist"));
            let install = || -> io::Result<()> {
                if words.contains(&"--print") {
                    print!("{plist}");
                    return Ok(());
                }
                let uid = Command::new("id").arg("-u").output()?.stdout;
                let gui = format!("gui/{}", String::from_utf8_lossy(&uid).trim());
                let job = format!("{gui}/{label}");
                let call = |line: &[&str]| {
                    Command::new("launchctl").args(line).output().map_err(|cause| {
                        io::Error::other(format!("launchctl: {cause} (setup installs a launchd agent, macOS only; --print prints the plist)"))
                    })
                };
                let must = |line: &[&str]| {
                    let out = call(line)?;
                    let said = line.join(" ");
                    if !out.status.success() {
                        return Err(io::Error::other(format!(
                            "launchctl {said}: {}",
                            String::from_utf8_lossy(&out.stderr).trim()
                        )));
                    }
                    println!("launchctl {said}");
                    Ok(())
                };
                let loaded = |name: &str| {
                    Ok::<bool, io::Error>(
                        call(&["print", &format!("{gui}/{name}")])?.status.success(),
                    )
                };
                // a bootout returns before launchd lets go of the job, and a bootstrap before then fails
                let limit = Duration::from_millis(setting("RIFT_BOOTOUT_MS", 30_000));
                let unload = |name: &str| -> io::Result<()> {
                    if loaded(name)? {
                        must(&["bootout", &format!("{gui}/{name}")])?;
                    }
                    let begun = Instant::now();
                    while loaded(name)? {
                        if begun.elapsed() > limit {
                            return Err(io::Error::other(format!(
                                "{name} still loaded {} ms after its bootout",
                                limit.as_millis()
                            )));
                        }
                        thread::sleep(Duration::from_millis(100));
                    }
                    Ok(())
                };
                let gone = |file: &PathBuf| {
                    if fs::remove_file(file).is_ok() {
                        println!("removed {}", file.display());
                    }
                };
                // 2026-11-22: an earlier build installed under another label and state root. Any
                // other agent whose program is this binary's `serve` is such an install, retired
                // here and by --remove; its root moves to this one while this one does not exist,
                // since the log is the daemon's memory
                let ours = format!(
                    "<array><string>{}</string><string>serve</string></array>",
                    exe.display()
                );
                let field = |held: &str, key: &str| -> Option<String> {
                    let (_, after) = held.split_once(key)?.1.split_once("<string>")?;
                    Some(after.split_once("</string>")?.0.to_owned())
                };
                let mut older: Vec<(PathBuf, String, Option<String>)> = path
                    .parent()
                    .and_then(|dir| fs::read_dir(dir).ok())
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|other| {
                        *other != path && other.extension().is_some_and(|ext| ext == "plist")
                    })
                    .filter_map(|other| {
                        let held = fs::read_to_string(&other)
                            .ok()
                            .filter(|held| held.contains(&ours))?;
                        let name = field(&held, "<key>Label</key>")?;
                        Some((other, name, field(&held, "<key>RIFT_ROOT</key>")))
                    })
                    .collect();
                older.sort();
                // rift's own keychain and code-signing identity (docs/index.html#identity), so a
                // rebuild keeps the microphone's grant; a tool call past `$RIFT_SIGN_MS` is killed,
                // since a keychain asking at the screen would hold setup forever
                let (keychain, pass) = (
                    root.join("rift.keychain-db"),
                    root.join("rift.keychain-pass"),
                );
                let (kc, allowed) = (
                    keychain.to_string_lossy(),
                    Duration::from_millis(setting("RIFT_SIGN_MS", 30_000)),
                );
                let ask = |name: &str, line: &[&str]| -> io::Result<Output> {
                    let said = format!("{name} {}", line.first().unwrap_or(&""));
                    let mut child = Command::new(name)
                        .args(line)
                        .stdin(Stdio::null())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .map_err(|cause| io::Error::other(format!("{said}: {cause}")))?;
                    let begun = Instant::now();
                    while child.try_wait()?.is_none() {
                        if begun.elapsed() > allowed {
                            drop(child.kill());
                            return Err(io::Error::other(format!(
                                "{said}: no answer in {} ms; is it asking at the screen?",
                                allowed.as_millis()
                            )));
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                    child.wait_with_output()
                };
                let need = |name: &str, line: &[&str]| -> io::Result<()> {
                    let out = ask(name, line)?;
                    if !out.status.success() {
                        return Err(io::Error::other(format!(
                            "{name} {}: {}",
                            line.first().unwrap_or(&""),
                            String::from_utf8_lossy(&out.stderr).trim()
                        )));
                    }
                    Ok(())
                };
                if words.contains(&"--remove") {
                    // first: it refuses while a swap runs, before anything is booted out
                    hotswap::forget("rift").map_err(io::Error::other)?;
                    for agent in &older {
                        unload(&agent.1)?;
                        gone(&agent.0);
                    }
                    unload(label)?;
                    gone(&path);
                    gone(&exe);
                    // files, never `security delete-keychain`, which might ask at the screen to
                    // unlock it; setup never puts it on the search list
                    gone(&keychain);
                    gone(&pass);
                    return Ok(());
                }
                // asked before anything is written: no launchctl (Linux) leaves nothing behind
                let swap =
                    loaded(label)? && fs::read_to_string(&path).is_ok_and(|held| held == plist);
                for agent in &older {
                    unload(&agent.1)?;
                    if let Some(from) = agent.2.as_ref().map(PathBuf::from)
                        && fs::exists(&from)?
                        && !fs::exists(&root)?
                    {
                        root.parent().map_or(Ok(()), fs::create_dir_all)?;
                        fs::rename(&from, &root)?;
                        println!("moved {} to {}", from.display(), root.display());
                    }
                    gone(&agent.0);
                }
                // the release into hotswap's layout, the link onto its `current`, and the upgrade
                // agent, hotswap's alone
                let linked = hotswap::ensure("rift", &manifest()).map_err(io::Error::other)?;
                println!(
                    "linked {} \u{2192} {}",
                    linked.display(),
                    fs::canonicalize(&linked)?.display()
                );
                // before the agent loads: launchd would create it, 0744, as serve.err's parent
                DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&root)?;
                fs::set_permissions(&root, Permissions::from_mode(0o700))?;
                if !fs::exists(&pass)? {
                    // a keychain whose password is gone opens no more: it is made again
                    drop(fs::remove_file(&keychain));
                    let mut noise = [0u8; 16];
                    fs::File::open("/dev/urandom")?.read_exact(&mut noise)?;
                    OpenOptions::new()
                        .create_new(true)
                        .mode(0o600)
                        .write(true)
                        .open(&pass)?
                        .write_all(hex(&noise).as_bytes())?;
                    println!("wrote {}", pass.display());
                }
                let secret = fs::read_to_string(&pass)?;
                if !fs::exists(&keychain)? {
                    need("security", &["create-keychain", "-p", &secret, &kc])?;
                    // no lock timeout and none at sleep: only a reboot locks it
                    need("security", &["set-keychain-settings", &kc])?;
                    println!("made {}", keychain.display());
                }
                need("security", &["unlock-keychain", "-p", &secret, &kc])?;
                // the identity's SHA-1, the first such word `find-identity` prints, else empty
                let found = || -> io::Result<String> {
                    let listed = ask("security", &["find-identity", "-p", "codesigning", &kc])?;
                    Ok(String::from_utf8_lossy(&listed.stdout)
                        .split_whitespace()
                        .find(|word| {
                            word.len() == 40 && word.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                        .unwrap_or_default()
                        .to_owned())
                };
                let mut hash = found()?;
                if hash.is_empty() {
                    // the key and certificate in a 0700 directory, gone once imported; the bundle
                    // 3DES with a SHA-1 MAC, which `security import` reads from LibreSSL and
                    // OpenSSL 3 alike; the partition list, else codesign's first use of the key
                    // asks at the screen
                    need(
                        "sh",
                        &[
                            "-c",
                            r#"set -e; rm -rf "$1"; mkdir -m 700 "$1"; trap 'rm -rf "$1"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 36500 -subj /CN=rift -addext keyUsage=critical,digitalSignature -addext extendedKeyUsage=critical,codeSigning -keyout "$1/key.pem" -out "$1/cert.pem"
openssl pkcs12 -export -inkey "$1/key.pem" -in "$1/cert.pem" -out "$1/identity.p12" -passout "pass:$2" -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1
security import "$1/identity.p12" -k "$3" -P "$2" -T /usr/bin/codesign
security set-key-partition-list -S apple-tool:,apple: -s -k "$2" "$3" >/dev/null"#,
                            "sh",
                            &root.join("identity.new").to_string_lossy(),
                            &secret,
                            &kc,
                        ],
                    )?;
                    hash = found()?;
                    println!(
                        "made the code-signing identity {hash} in {}",
                        keychain.display()
                    );
                }
                // the release itself, not the link: signed unless it already is, by this identity
                let target = fs::canonicalize(&linked)?;
                let program = target.to_string_lossy();
                let wanted =
                    format!("-R=identifier \"{label}\" and certificate leaf = H\"{hash}\"");
                if !ask("codesign", &["-v", &wanted, &program])?
                    .status
                    .success()
                {
                    // a copy signed and renamed over it: a running rift keeps the file it mapped
                    let copy = target.with_added_extension("new");
                    let _: u64 = fs::copy(&target, &copy)?;
                    need(
                        "codesign",
                        &[
                            "-f",
                            "-s",
                            &hash,
                            "--keychain",
                            &kc,
                            "-i",
                            label,
                            &copy.to_string_lossy(),
                        ],
                    )
                    .and_then(|()| fs::rename(&copy, &target))
                    .inspect_err(|_| drop(fs::remove_file(&copy)))?;
                    println!("signed {} as {label} by {hash}", target.display());
                }
                let staged = path.with_extension("plist.new");
                path.parent().map_or(Ok(()), fs::create_dir_all)?;
                fs::write(&staged, &plist)?;
                fs::rename(&staged, &path)?;
                println!("wrote {}", path.display());
                // the pid of a daemon answering health ready on the socket, if one does
                let serving = || -> Option<Value> {
                    let mut stream = UnixStream::connect(&sock).ok()?;
                    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
                    let hello =
                        json!({"client": format!("rift/{VERSION}"), "op": "hello", "proto": PROTO});
                    writeln!(stream, "{hello}\n{}", json!({"op": "health"})).ok()?;
                    let reply: Value =
                        serde_json::from_str(&BufReader::new(stream).lines().nth(1)?.ok()?).ok()?;
                    (reply["state"] == "ready").then(|| reply["pid"].clone())
                };
                // the same plist loaded: a swap is one kickstart; else it is loaded afresh
                let before = if swap {
                    let before = serving();
                    must(&["kickstart", "-k", &job])?;
                    before
                } else {
                    unload(label)?;
                    must(&["bootstrap", &gui, &path.to_string_lossy()])?;
                    None
                };
                // launchd holds a respawn until ThrottleInterval after the last launch: wait it out
                let (begun, patience) = (
                    Instant::now(),
                    Duration::from_millis(setting("RIFT_READY_MS", 30_000)),
                );
                loop {
                    if serving().is_some_and(|now| Some(now) != before) {
                        return Ok(());
                    }
                    if begun.elapsed() > patience {
                        return Err(io::Error::other(format!(
                            "{label} not ready {} ms after its load; see {}",
                            patience.as_millis(),
                            root.join("serve.err").display()
                        )));
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            };
            install().map_or_else(
                |cause| {
                    eprintln!("rift setup: {cause}");
                    ExitCode::FAILURE
                },
                |()| ExitCode::SUCCESS,
            )
        }
        (&["upgrade", ..], None) => u8::try_from(hotswap::upgrade(
            "rift",
            &manifest(),
            args.get(2..).unwrap_or_default(),
        ))
        .map_or(ExitCode::FAILURE, ExitCode::from),
        (&["version"], None) => {
            println!("{VERSION}");
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!(
                "usage: rift serve | status | quiet on|off | ears on|off | keep <window s> [<archive>] | feed [--after <boot>:<seq>] | listen <track> | read <track> <from us> <to us> | device ear|mouth <name> <file.pcm> [--fast] | device ear <name> --mic [<microphone>] | setup [--print | --remove] | upgrade [--check | --back | --retry] | version"
            );
            ExitCode::from(2)
        }
    }
}

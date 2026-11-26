//! `rift`. `serve` is the daemon, an append-only event log served over a unix socket
//! (docs/protocol.html, protocol 1) and an audio stream its devices feed onto one clock; `status`,
//! `quiet`, `ears`, `keep`, `feed`, `listen` and `read` are thin clients of that socket, `device`
//! one that plays a raw PCM file as frames or, on macOS, hears a microphone (`ear.rs`) or plays
//! what it is given (`mouth.rs`), `play` and `hush` a mouth's givers; `setup` installs the
//! release, signs it and loads its launchd agents, `upgrade` swaps in a newer release; `version`
//! prints the build.

#[cfg(target_os = "macos")]
mod ear;
#[cfg(target_os = "macos")]
mod mic;
#[cfg(target_os = "macos")]
mod mouth;
#[cfg(target_os = "macos")]
mod speaker;
mod tail;

use serde_json::{Value, json};
use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::signal_name;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
use std::process::{Command, ExitCode, Output, Stdio, exit, id};
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A feed client this many events behind is dropped (docs/protocol.html#feed).
const BEHIND: usize = 1024;

/// A frame's bytes: 20 ms of 16 kHz mono 16-bit little-endian PCM.
const FRAME: usize = 640;

/// The longest request line, its `\n` not counted (docs/protocol.html#framing).
const LINE: u64 = 1 << 16;

/// How long a hushed mouth has to answer before it is cut (docs/protocol.html#plays).
const PATIENCE: Duration = Duration::from_secs(1);

/// The wire protocol this build speaks.
const PROTO: u64 = 1;

/// Given samples a mouth's interpolator holds untaken as it waits for more, its window both
/// sides (src/mouth.rs): no progress owed (docs/protocol.html#plays).
const REACH: u64 = 32;

/// The `stopping` state, the one the request handler tests for.
const STOPPING: &str = "stopping";

/// Woken at every change to a play: a withheld give, a hush and the drain wait on it with `live`.
static SLACK: Condvar = Condvar::new();

/// The release (a checkout's build: the crate version), then `build.rs`'s short commit: hello's
/// and health's `version`.
const VERSION: &str = concat!(env!("RIFT_RELEASE"), "-", env!("RIFT_BUILD"));

/// Why the daemon stops (`signal TERM`, `shutdown request`), set once before `accept` is woken.
static STOP: OnceLock<String> = OnceLock::new();

/// An engine: a client doing one kind of work on one track (docs/protocol.html#engines).
struct Engine {
    /// Where the last frame it was given ends: a report naming a time past it is refused.
    given: u64,
    /// Its connection's key, for an echo engine dropped behind its ruling.
    key: u64,
    /// Its name.
    name: String,
    /// An echo engine's: every sample before it is ruled on; from where its track ended as it
    /// connected, since frames before then went on unruled.
    ruled: u64,
    /// A turn engine's: whether its voice is on.
    voice: bool,
}

/// What a connection does after its reply is written.
enum Next {
    /// Close it: a refused hello or a line past the limit.
    Close,
    /// Become a device or an engine: the lines it starts with, then its replies and what it is
    /// told, through this queue, by a writer of its own.
    Device(Box<dyn Iterator<Item = String> + Send>, Receiver<String>),
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
    /// How far ahead of its output a play's mouth may hold it, in milliseconds.
    ahead: u64,
    /// The directory a segment leaving the window moves into, else null: it is deleted.
    archive: Value,
    /// This boot's number, derived from the log.
    boot: u64,
    /// Every open connection by key, for the drain's forced close and a slow feed's drop.
    conns: BTreeMap<u64, UnixStream>,
    /// Whether the ears are open: whether devices' frames are taken.
    ears: bool,
    /// Each mouth's last play to end, by its track: its last line, its giver's and its hush's
    /// reply, kept until the next one ends.
    ended: BTreeMap<String, Value>,
    /// Each connected engine, by its track and kind.
    engines: BTreeMap<(String, String), Engine>,
    /// Each feed's queue, its connection's key and its topic: `""` the events, `"ears"` a
    /// device's replies and notices, else the track a listener follows.
    feeds: Vec<(SyncSender<String>, u64, String)>,
    /// The closed segments still under `audio/` and not handed to the sweep, as their events, in
    /// log order.
    held: Vec<Value>,
    /// The last connection key handed out.
    keys: u64,
    /// The latest time each track's logged reports of each kind name, any boot: a `words`,
    /// `turn` or `mask` its end, a `voice` its start; a report before it is refused.
    latest: BTreeMap<(String, String), u64>,
    /// The last play's number, across every boot.
    numbered: u64,
    /// Each open play, by its mouth's track.
    plays: BTreeMap<String, Play>,
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
    /// How long a play's mouth may report nothing while it owes progress before it is ended.
    stall: Duration,
    /// `ready`, `degraded` (the last write to the log or a segment failed) or `stopping`.
    state: &'static str,
    /// The sweep's queue: each batch of segments leaving the window and the archive they move
    /// into, moved by a thread of its own, so no request waits on the archive's disk.
    sweeps: Sender<(Value, Vec<Value>)>,
    /// Every track of this boot, and every one with masks or turns in the window, by name.
    tracks: BTreeMap<String, Track>,
    /// The window in seconds: a closed segment that ended longer ago leaves `audio/`.
    window: u64,
}

/// A play: audio given for a mouth and passed to the device streaming it, which holds what waits
/// (docs/protocol.html#plays).
struct Play {
    /// Whether its giver sent `end`.
    end: bool,
    /// Its samples given so far, at its rate.
    given: u64,
    /// Its giver's connection key.
    giver: u64,
    /// When its mouth last reported, or it was opened, given to or ended: the stall rule's clock,
    /// so progress is owed only from the moment it is.
    heard: Instant,
    /// Why it is hushed, once the mouth is told.
    hushed: Option<&'static str>,
    /// Its number.
    num: u64,
    /// The given audio's rate in Hz.
    rate: u64,
    /// The run under way: its first sample, its samples so far and when the first sounded.
    run: Option<(u64, u64, u64)>,
}

/// A track, one device's sound on the daemon's clock, in this boot.
#[derive(Default)]
struct Track {
    /// The key of the connection streaming it, while one does.
    conn: Option<u64>,
    /// Where its next frame goes on the timeline, once it has had one.
    end: Option<u64>,
    /// The microphone (a mouth's: the output) its last frame named, else null.
    mic: Value,
    /// The next segment's number.
    next: u64,
    /// The open segment's number, start and samples written.
    open: Option<(u64, u64, u64)>,
    /// An ear's frames placed and not yet passed on, held while an echo engine rules on them.
    pending: VecDeque<(u64, String)>,
    /// Whether the device streaming it plays (docs/protocol.html#plays).
    plays: bool,
    /// Its logged `mask`s and `turn`s ending inside the window, any boot, by kind, rising: the
    /// masks applied to the frames its turn and words engines read, the turns a words engine's.
    spans: BTreeMap<String, Vec<(u64, u64)>>,
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
                Some((name.clone(), labelled(json!({"kind": "segment", "n": num, "path": format!("audio/{boot}/{name}/{num}.pcm"), "samples": samples, "start": start, "track": name}), &track.mic)))
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

    /// Ends `track`'s open play for `reason`: its run under way and `played` logged, its last
    /// line kept for its giver and its hush, the mouth's open segment closed, its giver woken.
    fn finish(&mut self, track: &str, reason: &str) {
        let Some(play) = self.plays.remove(track) else {
            return;
        };
        let (num, played) = (play.num, play.played());
        if let Some((from, samples, start)) = play.run {
            drop(self.emit(&json!({"from": from, "kind": "sound", "play": num, "samples": samples, "start": start, "track": track})));
        }
        let mut last = json!({"dropped": play.given.saturating_sub(played), "given": play.given, "kind": "played", "play": num, "played": played, "reason": reason, "track": track});
        drop(self.emit(&last));
        if let Some(fields) = last.as_object_mut() {
            drop(fields.remove("kind"));
            drop(fields.insert("seq".to_owned(), self.seq.into()));
        }
        drop(self.ended.insert(track.to_owned(), last));
        drop(self.close(Some(track)));
        drop(
            self.conns
                .get(&play.giver)
                .map(|conn| conn.shutdown(Shutdown::Read)),
        );
        self.fan(
            "runs",
            &format!("{}\n", json!({"played": num, "track": track})),
        );
        SLACK.notify_all();
    }

    /// The health reply: hello's fields, then the stream's and the switches'.
    fn health(&self) -> Value {
        let mut reply = self.hello();
        let mut engines = json!({});
        for (slot, engine) in &self.engines {
            engines[&slot.0][&slot.1] = engine.name.clone().into();
        }
        reply["engines"] = engines;
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
            reply["playing"],
            reply["quiet"],
            reply["seq"],
            reply["window"],
        ) = (
            self.archive.clone(),
            json!(devices),
            self.ears.into(),
            (self.plays.iter())
                .map(|(track, play)| (track.clone(), Value::from(play.num)))
                .collect(),
            self.quiet.into(),
            self.seq.into(),
            self.window.into(),
        );
        reply
    }

    /// Holds the frame placed on `track` at `at` for its echo engine's ruling, given it already as
    /// a listener is, then passes on what is ruled; one 1024 frames behind is dropped.
    fn hear(&mut self, track: &str, at: u64, pcm: String) {
        let held = self.tracks.entry(track.to_owned()).or_default();
        held.pending.push_back((at, pcm));
        let behind = held.pending.len() > BEHIND;
        if let Some(echo) = self.engines.get_mut(&(track.to_owned(), "echo".to_owned())) {
            echo.given = at.saturating_add(20_000);
            if behind {
                drop(
                    self.conns
                        .get(&echo.key)
                        .map(|conn| conn.shutdown(Shutdown::Both)),
                );
            }
        }
        self.release(track);
    }

    /// The hello reply.
    fn hello(&self) -> Value {
        json!({"boot": self.boot, "ok": true, "pid": id(), "proto": PROTO, "since": self.since, "state": self.state, "version": VERSION})
    }

    /// Tells `track`'s mouth to drop its open play for `reason`, the first hush's reason kept; one
    /// given nothing, which the mouth has never heard of, ends at once.
    fn hush(&mut self, track: &str, reason: &'static str) {
        // every caller holds an open play: with none, a hush of play 0 is no one's
        let (num, empty, hushed) = (self.plays.get_mut(track))
            .map(|play| {
                let hushed = *play.hushed.get_or_insert(reason);
                (play.num, play.given == 0, hushed)
            })
            .unwrap_or_default();
        self.tell(track, &json!({"hush": num}));
        if empty {
            self.finish(track, hushed);
        }
    }

    /// Logs the end of the engine at `only`, else of every engine: the drain's, before `stop`.
    fn leave(&mut self, only: Option<&(String, String)>) {
        let gone: Vec<(String, String)> = (self.engines.keys())
            .filter(|slot| only.is_none_or(|one| one == *slot))
            .cloned()
            .collect();
        for slot in gone {
            // a words engine's partials readers end with it
            let partials = format!("{} partial", slot.0);
            self.feeds
                .retain(|feed| slot.1 != "words" || feed.2 != partials);
            let name = self.engines.remove(&slot).map(|engine| engine.name);
            drop(self.emit(&json!({"kind": "engine", "name": name, "on": false, "stage": slot.1, "track": slot.0})));
            // an echo engine gone, its held frames go on unruled: the stage fails open
            self.release(&slot.0);
        }
    }

    /// `wrote`, a write to the log or a segment, as it came; the state `ready` once one
    /// succeeds, `degraded` while the last failed.
    const fn mark<T>(&mut self, wrote: io::Result<T>) -> io::Result<T> {
        self.state = if wrote.is_ok() { "ready" } else { "degraded" };
        wrote
    }

    /// Passes `track`'s held frames its echo engine has ruled past, all with none connected, to
    /// its turn and words engines, the logged masks applied.
    fn release(&mut self, track: &str) {
        let ruled = (self.engines.get(&(track.to_owned(), "echo".to_owned())))
            .map_or(u64::MAX, |echo| echo.ruled);
        let held = self.tracks.entry(track.to_owned()).or_default();
        let mut lines = Vec::new();
        while let Some((at, pcm)) = held
            .pending
            .pop_front_if(|frame| frame.0.saturating_add(20_000) <= ruled)
        {
            lines.push((at, masked(&held.spans, at, &pcm)));
        }
        for (at, line) in lines {
            for kind in ["turn", "words"] {
                if let Some(engine) = self.engines.get_mut(&(track.to_owned(), kind.to_owned())) {
                    engine.given = at.saturating_add(20_000);
                }
            }
            self.fan(&format!("{track} heard"), &line);
        }
    }

    /// `track`'s frames on disk from `from` on, each segment read as the replay reaches it, and
    /// where the last ends: a turn or words engine's replay, the live frames joined after it. A
    /// words engine's carries each logged turn ending after `from`, after the frame holding its end.
    fn replay(
        &self,
        track: &str,
        from: u64,
        words: bool,
    ) -> (Box<dyn Iterator<Item = String> + Send>, u64) {
        let mut spans: Vec<(u64, u64, PathBuf)> = (self.segments(track).iter())
            .filter(|held| end(held) > from)
            .map(|held| {
                let count = held["samples"].as_u64().unwrap_or_default();
                (
                    held["start"].as_u64().unwrap_or_default(),
                    count.checked_div(320).unwrap_or_default(),
                    self.root.join(held["path"].as_str().unwrap_or_default()),
                )
            })
            .collect();
        spans.sort();
        // up to the first frame its echo engine holds, which comes live once ruled on
        let kept = self.tracks.get(track);
        let cut = (kept.and_then(|known| known.pending.front())).map_or(u64::MAX, |frame| frame.0);
        let given = (spans.iter())
            .map(|span| span.0.saturating_add(span.1.saturating_mul(20_000)))
            .max()
            .unwrap_or_default()
            .min(cut);
        let marks = kept.map(|known| known.spans.clone()).unwrap_or_default();
        let mut turns: Vec<(u64, u64)> = (marks.get("turn").into_iter().flatten())
            .filter(|turn| words && turn.1 > from)
            .copied()
            .collect();
        let frames = spans.into_iter().flat_map(move |(start, count, path)| {
            (fs::read(path).unwrap_or_default().chunks_exact(FRAME))
                .take(usize::try_from(count).unwrap_or_default())
                .zip((start..).step_by(20_000))
                .filter(|&(_, at)| at.saturating_add(20_000) > from && at < cut)
                .map(|(pcm, at)| (at, masked(&marks, at, &hex(pcm))))
                .collect::<Vec<_>>()
        });
        // before each frame the turns that ended by its start, the rest after the last
        let lines = frames.map(Some).chain([None]).flat_map(move |frame| {
            let due = frame.as_ref().map_or(u64::MAX, |held| held.0);
            (turns.extract_if(.., |turn| turn.1 <= due))
                .map(|(start, stop)| {
                    format!("{}\n", json!({"turn": {"end": stop, "start": start}}))
                })
                .chain(frame.map(|held| held.1))
                .collect::<Vec<_>>()
        });
        (Box::new(lines), given)
    }

    /// `slot`'s engine's report, checked and acted on (docs/protocol.html#engines): its reply, else
    /// `bad_request` and nothing logged. A span starts on or after the latest of its kind and ends
    /// no later than the last frame the engine was given; words rise inside it.
    fn report(&mut self, slot: &(String, String), request: &Value) -> io::Result<Value> {
        let (track, op) = (&slot.0, request["op"].as_str().unwrap_or_default());
        // a ruling and a voice name one time, `to` and `start`
        let first = request[if op == "ruled" { "to" } else { "start" }].as_u64();
        let last = if matches!(op, "ruled" | "voice") {
            first
        } else {
            request["end"].as_u64()
        };
        let (latest, (given, name, ruled, voice)) = (
            (self
                .latest
                .get(&(track.clone(), op.replace("partial", "words"))))
            .copied()
            .unwrap_or_default(),
            (self.engines.get(slot))
                .map(|engine| {
                    (
                        engine.given,
                        engine.name.clone(),
                        engine.ruled,
                        engine.voice,
                    )
                })
                .unwrap_or_default(),
        );
        // a mask starts at or after the ruling, and alone may reach past what was heard: the
        // echo of a run is known as it sounds
        let floor = latest.max(if matches!(op, "mask" | "ruled") {
            ruled
        } else {
            0
        });
        let ceiling = if op == "mask" { u64::MAX } else { given };
        let timed = first
            .zip(last)
            .is_some_and(|(start, end)| floor <= start && start <= end && end <= ceiling);
        let mut reach = first.unwrap_or_default();
        let worded = request["words"].as_array().is_some_and(|said| {
            said.iter().all(|word| {
                let span =
                    (word["start"].as_u64().zip(word["end"].as_u64())).filter(|&(start, end)| {
                        reach <= start
                            && start <= end
                            && Some(end) <= last
                            && word["text"].is_string()
                    });
                reach = span.map_or(u64::MAX, |held| held.1);
                span.is_some()
            })
        });
        // the fields its event carries, a partial's line the same but its kind
        let mut event = json!({"engine": name, "kind": op, "track": track});
        let fields: &[&str] = match op {
            "partial" | "words" => &["end", "start", "words"],
            "voice" => &["on", "start"],
            _ => &["end", "start"],
        };
        for &field in fields {
            event[field] = request[field].clone();
        }
        let (begun, reached) = (first.unwrap_or_default(), last.unwrap_or_default());
        match (slot.1.as_str(), op) {
            ("echo", "ruled") if timed => {
                if let Some(engine) = self.engines.get_mut(slot) {
                    engine.ruled = reached;
                }
                self.release(track);
                Ok(json!({"ok": true}))
            }
            ("words", "partial") if timed && worded => {
                drop(event.as_object_mut().and_then(|held| held.remove("kind")));
                self.fan(&format!("{track} partial"), &format!("{event}\n"));
                Ok(json!({"ok": true}))
            }
            // voice on and off alternate from on
            ("echo", "mask") | ("turn", "turn" | "voice") | ("words", "words")
                if timed
                    && (worded || slot.1 != "words")
                    && (op != "voice" || request["on"] == !voice) =>
            {
                drop(self.emit(&event)?);
                let _: Option<u64> = self.latest.insert((track.clone(), op.to_owned()), reached);
                if let Some(engine) = self.engines.get_mut(slot) {
                    engine.voice ^= op == "voice";
                }
                // a mask kept for the frames it covers, a turn for the words engine's replays
                if matches!(op, "mask" | "turn") {
                    let held = self.tracks.entry(track.clone()).or_default();
                    held.spans
                        .entry(op.to_owned())
                        .or_default()
                        .push((begun, reached));
                }
                // a turn's line to the words engine, after the frame holding its end
                if op == "turn" {
                    let line = json!({"turn": {"end": reached, "start": begun}});
                    self.fan(&format!("{track} turn"), &format!("{line}\n"));
                }
                Ok(json!({"ok": true, "seq": self.seq}))
            }
            _ => Ok(error("bad_request")),
        }
    }

    /// `track`'s segments on disk, as their events: the closed ones still under `audio/`, then
    /// the open one as it stands.
    fn segments(&self, track: &str) -> Vec<Value> {
        let open = (self.tracks.get(track).and_then(|held| held.open)).map(|(num, start, written)| {
            json!({"path": format!("audio/{}/{track}/{num}.pcm", self.boot), "samples": written, "start": start, "track": track})
        });
        (self.held.iter().map(|event| &event["e"]))
            .chain(open.as_ref())
            .filter(|held| held["track"] == track)
            .cloned()
            .collect()
    }

    /// Ends `mouth` every play whose mouth owes progress (its play's end after `end`, or given
    /// past what it took by more than the interpolator's `REACH`, which waits on the next give)
    /// and has reported nothing for `$RIFT_STALL_MS`: an output failed or stuck in place, its
    /// giver answered; the mouth is told to drop it.
    fn stalled(&mut self) {
        let stuck: Vec<String> = (self.plays.iter())
            .filter(|&(_, play)| {
                play.heard.elapsed() >= self.stall
                    && (play.end || play.given > play.played().saturating_add(REACH))
            })
            .map(|(track, _)| track.clone())
            .collect();
        for track in stuck {
            let num = self.plays.get(&track).map(|play| play.num);
            self.tell(&track, &json!({"hush": num}));
            self.finish(&track, "mouth");
        }
    }

    /// Hands every closed segment that ended more than the window ago to the sweep's thread,
    /// which moves it out of `audio/`.
    fn sweep(&mut self) {
        let cutoff = now().saturating_sub(self.window.saturating_mul(1_000_000));
        let due = self
            .held
            .extract_if(.., |event| end(&event["e"]) < cutoff)
            .collect();
        for spans in self
            .tracks
            .values_mut()
            .flat_map(|track| track.spans.values_mut())
        {
            spans.retain(|span| span.1 >= cutoff);
        }
        drop(self.sweeps.send((self.archive.clone(), due)));
    }

    /// Queues `line` to the playing device streaming `track`, the way its replies leave, under the
    /// topic `<track>>`; one whose queue is full is dropped as a feed is, its play ending as it
    /// leaves.
    fn tell(&mut self, track: &str, line: &Value) {
        self.fan(&format!("{track}>"), &format!("{line}\n"));
    }
}

impl Play {
    /// The samples its output took: the run under way's end, runs being contiguous from 0.
    fn played(&self) -> u64 {
        self.run
            .map_or(0, |(from, samples, _)| from.saturating_add(samples))
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

/// A stream event naming what its frames name, `mic` on an ear's track and `speaker` on a
/// mouth's, only when they name one: a file-fed device's events are as they were.
fn labelled(mut event: Value, heard: &Value) -> Value {
    let key = if event["track"]
        .as_str()
        .is_some_and(|track| track.starts_with("mouth-"))
    {
        "speaker"
    } else {
        "mic"
    };
    event[key] = heard.clone();
    (event.as_object_mut().into_iter())
        .for_each(|fields| fields.retain(|_, field| !field.is_null()));
    event
}

/// A frame's line for a turn or words engine: `pcm` placed at `at`, the samples a logged mask of
/// `spans` covers zeros and the frame marked `masked`.
fn masked(spans: &BTreeMap<String, Vec<(u64, u64)>>, at: u64, pcm: &str) -> String {
    let (mut text, mut hit) = (pcm.as_bytes().to_vec(), false);
    // a time's place in the frame's hex, four digits a sample
    let digit = |time: u64| {
        let into = samples(time).saturating_sub(samples(at)).min(320);
        usize::try_from(into.saturating_mul(4)).unwrap_or_default()
    };
    let masks = spans.get("mask").map_or(&[][..], Vec::as_slice);
    for &(start, stop) in (masks.iter())
        .skip(masks.partition_point(|mask| mask.1 <= at))
        .take_while(|mask| mask.0 < at.saturating_add(20_000))
    {
        if let Some(zeroed) = text.get_mut(digit(start)..digit(stop)) {
            hit |= !zeroed.is_empty();
            zeroed.fill(b'0');
        }
    }
    let mut line = json!({"pcm": String::from_utf8(text).unwrap_or_default(), "t": at});
    if hit {
        line["masked"] = true.into();
    }
    format!("{line}\n")
}

/// `track`'s open play hushed for `reason` and waited on until it ends; a mouth that has not
/// answered within `patience` is cut and the play ended at its last report, `mouth` (the drain's:
/// `stop`). Its last line, or `"play":null` with none open.
fn quell<'guard>(
    mut guard: MutexGuard<'guard, Live>,
    track: &str,
    reason: &'static str,
    patience: Duration,
) -> (MutexGuard<'guard, Live>, Value) {
    let Some(num) = guard.plays.get(track).map(|play| play.num) else {
        return (guard, json!({"play": null}));
    };
    guard.hush(track, reason);
    let begun = Instant::now();
    while guard.plays.get(track).is_some_and(|play| play.num == num) {
        let left = patience.saturating_sub(begun.elapsed());
        if left.is_zero() {
            let mouth = guard.tracks.get(track).and_then(|held| held.conn);
            drop(
                mouth
                    .and_then(|key| guard.conns.get(&key))
                    .map(|conn| conn.shutdown(Shutdown::Both)),
            );
            guard.finish(track, if reason == "stop" { reason } else { "mouth" });
        }
        guard = SLACK
            .wait_timeout(guard, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    let last = guard.ended.get(track).cloned().unwrap_or_default();
    (guard, last)
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

/// Whether `<kind>-<name>` is a track: an ear or a mouth, its name 1 to 64 ASCII letters, digits,
/// `-` or `_` (docs/protocol.html#stream).
fn well(kind: &str, name: &str) -> bool {
    matches!(kind, "ear" | "mouth")
        && (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
}

/// The guard on `live`, a poisoned lock taken as it stands.
fn lock(live: &Mutex<Live>) -> MutexGuard<'_, Live> {
    live.lock().unwrap_or_else(PoisonError::into_inner)
}

/// rift's hotswap manifest (gigantic.dev/hotswap/): releases from jgeschwendt/rift under its
/// packslip signer and `rift setup` after each swap. No agent to kickstart and no drain step:
/// setup itself swaps the daemon, whose SIGTERM is the daemon's own drain, and returns once the
/// new one answers ready. The smoke is the new binary's `version`, then
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

/// `file` copied to `<file>.1` and emptied once it holds `cap` bytes. Emptied, not renamed: a
/// stderr launchd opened appends (observed 2026-11-25 · lsof's `AP` on two agents' stderr), so its
/// writer goes on at the start, where after a rename it would write on into `.1`.
fn rotate(file: &Path, cap: u64) {
    if fs::metadata(file).is_ok_and(|meta| meta.len() >= cap) {
        drop(
            fs::copy(file, file.with_added_extension("1"))
                .and_then(|_| OpenOptions::new().write(true).open(file)?.set_len(0)),
        );
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

#[expect(
    clippy::cognitive_complexity,
    reason = "main is one dispatch over the verbs: split, each arm would be a function called once"
)]
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
        ["device", "mouth", name, "--speaker"] => {
            Some(json!({"kind": "mouth", "name": name, "op": "device", "plays": true}))
        }
        ["device", kind, name, _] | ["device", kind, name, _, "--fast"] => {
            Some(json!({"kind": kind, "name": name, "op": "device"}))
        }
        [op @ ("ears" | "quiet"), on @ ("on" | "off")] => Some(json!({"on": on == "on", "op": op})),
        ["engine", "run", "tail", track] | ["engine", "run", "tail", track, "--tail-ms", _] => {
            (words.get(5).is_none_or(|ms| ms.parse::<u64>().is_ok()))
                .then(|| json!({"kind": "echo", "name": "tail", "op": "engine", "track": track}))
        }
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
        ["hush", track] => Some(json!({"op": "hush", "track": track})),
        ["listen", track] => Some(json!({"op": "listen", "track": track})),
        ["play", track, rate] | ["play", track, rate, _] => rate
            .parse::<u64>()
            .ok()
            .map(|hz| json!({"op": "play", "rate": hz, "track": track})),
        ["read", track, from, to] => from.parse::<u64>().ok().zip(to.parse::<u64>().ok()).map(
            |(start, stop)| json!({"from": start, "op": "read", "to": stop, "track": track}),
        ),
        ["status"] => Some(json!({"op": "health"})),
        _ => None,
    };
    match (words.as_slice(), asked) {
        (&[verb, ..], Some(wanted)) => {
            // an ear's alone: a mouth's `--mic` is the file it plays; a standing device is either
            let mic = matches!(*words.as_slice(), ["device", "ear", _, "--mic", ..]);
            let speaker = matches!(*words.as_slice(), ["device", "mouth", _, "--speaker"]);
            let stand = mic || speaker;
            #[cfg(not(target_os = "macos"))]
            if stand {
                eprintln!(
                    "rift device: --mic and --speaker are macOS only; a file-fed device plays anywhere"
                );
                return ExitCode::from(2);
            }
            // a stop signal ends a standing device at once, wherever it waits, exit 0: it holds
            // nothing an exit does not let go, and the daemon closes its segment as it goes; it
            // ends a play the same way, exit 1, its connection's end hushing the play
            if stand || verb == "play" {
                let said = verb.to_owned();
                drop(Signals::new([SIGHUP, SIGINT, SIGTERM]).map(|mut signals| {
                    thread::spawn(move || {
                        let signal = signals.forever().next().unwrap_or_default();
                        let name = signal_name(signal).unwrap_or_default();
                        drop(writeln!(io::stderr(), "rift {said}: stopped by {name}"));
                        exit(i32::from(!stand))
                    })
                }));
            }
            // a play's audio: its file, opened before it connects, else stdin as it comes
            let mut audio: Box<dyn io::Read> = match words
                .get(3)
                .filter(|_| verb == "play")
                .map(fs::File::open)
                .transpose()
            {
                Ok(file) => file.map_or_else(
                    || -> Box<dyn io::Read> { Box::new(io::stdin()) },
                    |held| Box::new(held),
                ),
                Err(cause) => {
                    eprintln!("rift play: {cause}");
                    return ExitCode::FAILURE;
                }
            };
            // a device's file, read before it connects
            let pcm = match words
                .get(3)
                .filter(|_| verb == "device" && !stand)
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
            let poll = Duration::from_millis(setting("RIFT_POLL_MS", 1000));
            // an ear stands: whatever ends an attempt, it waits a poll and connects again. Each
            // refusal (a protocol mismatch, a refused hello or device) is said on stderr too, since
            // launchd keeps no stdout: once a code until it connects, the wait doubled per refusal
            // in a row, 64 polls at most, as a failed open's
            let (refused, strikes) = (Cell::new(Value::Null), Cell::new(0u32));
            let strike = |code: &Value| {
                strikes.set(strikes.get().saturating_add(1));
                stand && refused.replace(code.clone()) != *code
            };
            let tell = |line: &Value| {
                println!("{line}");
                if strike(&line["code"]) {
                    eprintln!("rift device: refused {line}");
                }
            };
            loop {
                // a standing device's launchd stderr, capped as the daemon's is, at each attempt
                if stand {
                    let err = if mic { "ear.err" } else { "mouth.err" };
                    rotate(&root.join(err), setting("RIFT_LOG_CAP", 1u64 << 20));
                }
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
                let code = match talk() {
                    Ok((hello, _, _)) if hello["code"] == "proto" => {
                        if !stand || strike(&hello["code"]) {
                            eprintln!(
                                "rift {verb}: the daemon speaks protocol {} ({}), this client protocol {PROTO} ({VERSION}); fix: rift setup, so both are one release",
                                hello["proto"],
                                hello["version"].as_str().unwrap_or_default()
                            );
                        }
                        ExitCode::FAILURE
                    }
                    // a daemon that plays nothing, an older release: waited out as a refusal
                    Ok((hello, reply, _))
                        if speaker && reply["ok"] == true && reply["plays"] != true =>
                    {
                        println!("{reply}");
                        if strike(&json!("plays")) {
                            eprintln!(
                                "rift device: the daemon {} plays nothing; fix: rift setup",
                                hello["version"].as_str().unwrap_or_default()
                            );
                        }
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
                            if stand {
                                refused.set(Value::Null);
                                strikes.set(0);
                                if mic {
                                    ear::run(reader, &reply, words.get(4).copied());
                                } else {
                                    mouth::run(reader, &reply);
                                }
                                thread::sleep(poll);
                                continue;
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
                                // a failed write and a failed read are one path: which one a
                                // daemon gone mid-play fails is a race
                                writeln!(
                                    reader.get_mut(),
                                    "{}",
                                    json!({"op": "frame", "pcm": hex(frame), "t": base.saturating_add(at)})
                                )
                                // notices and refusals printed, pings answered, until its reply
                                .and_then(|()| loop {
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
                                })
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
                        // a play: each 16384 bytes given as its reply allows, then `end`, then the
                        // play's last line, which has no `ok`; a write a play's end beat is no matter
                        if verb == "play" && reply["ok"] == true {
                            println!("{reply}");
                            drop(reader.get_ref().set_read_timeout(None));
                            let mut chunk = Vec::new();
                            let last = loop {
                                chunk.clear();
                                drop((&mut audio).take(0x4000).read_to_end(&mut chunk));
                                // whole samples: an odd last byte is dropped
                                chunk.truncate(chunk.len() & !1);
                                let line = if chunk.is_empty() {
                                    json!({"op": "end"})
                                } else {
                                    json!({"op": "give", "pcm": hex(&chunk)})
                                };
                                drop(writeln!(reader.get_mut(), "{line}"));
                                match answer(&mut reader) {
                                    Ok(said) if said.get("ok").is_some() && !chunk.is_empty() => {}
                                    Ok(said) if said.get("ok").is_some() => {
                                        break answer(&mut reader);
                                    }
                                    other => break other,
                                }
                            };
                            return last.map_or_else(
                                |_| {
                                    println!("{}", error("unknown"));
                                    ExitCode::FAILURE
                                },
                                |said| {
                                    println!("{said}");
                                    ExitCode::from(u8::from(said["reason"] != "end"))
                                },
                            );
                        }
                        // the tail engine: its reply, then its work until the connection ends
                        if verb == "engine" && reply["ok"] == true {
                            println!("{reply}");
                            drop(reader.get_ref().set_read_timeout(None));
                            let ms = words.get(5).and_then(|held| held.parse().ok());
                            tail::run(reader, ms.unwrap_or(200u64).saturating_mul(1000));
                            println!("{}", error("unknown"));
                            return ExitCode::FAILURE;
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
                        tell(&reply);
                        ExitCode::from(u8::from(reply["ok"] != true))
                    }
                    Ok((turned, _, _)) => {
                        tell(&turned);
                        ExitCode::FAILURE
                    }
                    Err(_) => {
                        println!("{}", error(if sent.get() { "unknown" } else { "down" }));
                        ExitCode::FAILURE
                    }
                };
                if !stand {
                    break code;
                }
                thread::sleep(poll.saturating_mul(2u32.saturating_pow(strikes.get().min(6))));
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
                    rotate(file, cap);
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
                // each play the log holds without its end, with what its logged runs sounded
                let of = |kind: &'static str| {
                    held_events
                        .iter()
                        .map(|event| &event["e"])
                        .filter(move |event| event["kind"] == kind)
                };
                let over: BTreeSet<u64> = of("played")
                    .filter_map(|event| event["play"].as_u64())
                    .collect();
                let unended: Vec<(&Value, &Value, u64)> = of("play")
                    .filter(|event| {
                        !event["play"]
                            .as_u64()
                            .is_some_and(|num| over.contains(&num))
                    })
                    .map(|event| {
                        let runs = of("sound").filter(|run| run["play"] == event["play"]);
                        let sounded = runs.map(|run| run["samples"].as_u64().unwrap_or_default());
                        (&event["play"], &event["track"], sounded.sum())
                    })
                    .collect();
                let live: &'static Mutex<Live> = Box::leak(Box::new(Mutex::new(Live {
                    ahead: setting("RIFT_AHEAD_MS", 200),
                    archive: kept.map_or(Value::Null, |keep| keep["archive"].clone()),
                    boot: held_events
                        .last()
                        .and_then(|event| event["boot"].as_u64())
                        .unwrap_or(0)
                        .saturating_add(1),
                    conns: BTreeMap::new(),
                    ears: last("ears").is_some_and(|ears| ears["on"] == true),
                    ended: BTreeMap::new(),
                    engines: BTreeMap::new(),
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
                    // reports rise, so each kind's last logged is its latest
                    latest: held_events
                        .iter()
                        .map(|event| &event["e"])
                        .filter(|event| {
                            matches!(
                                event["kind"].as_str(),
                                Some("mask" | "turn" | "voice" | "words")
                            )
                        })
                        .map(|event| {
                            (
                                (
                                    event["track"].as_str().unwrap_or_default().to_owned(),
                                    event["kind"].as_str().unwrap_or_default().to_owned(),
                                ),
                                // a voice's start, every other kind's end
                                event["end"]
                                    .as_u64()
                                    .max(event["start"].as_u64())
                                    .unwrap_or_default(),
                            )
                        })
                        .collect(),
                    numbered: last("play")
                        .map_or(0, |play| play["play"].as_u64().unwrap_or_default()),
                    plays: BTreeMap::new(),
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
                    stall: Duration::from_millis(setting("RIFT_STALL_MS", 15_000)),
                    state: "ready",
                    sweeps,
                    // the logged masks and turns, the sweep cutting those past the window
                    tracks: of("mask").chain(of("turn")).fold(
                        BTreeMap::new(),
                        |mut tracks, span| {
                            let field = |key: &str| span[key].as_str().unwrap_or_default();
                            let at = |key: &str| span[key].as_u64().unwrap_or_default();
                            let entry: &mut Track =
                                tracks.entry(field("track").to_owned()).or_default();
                            let spans = entry.spans.entry(field("kind").to_owned()).or_default();
                            spans.push((at("start"), at("end")));
                            tracks
                        },
                    ),
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
                        let mut guard = lock(live);
                        guard.sweep();
                        guard.stalled();
                    }
                }));
                // the pid file first: past the boot event only a failed append or rename refuses
                fs::write(&pid, id().to_string())?;
                let mut guard = lock(live);
                drop(guard.emit(&json!({"kind": "boot", "pid": id(), "version": VERSION}))?);
                // a play a kill left open: what its logged runs sounded, the rest not on disk
                for (num, track, played) in unended {
                    drop(guard.emit(&json!({"kind": "played", "play": num, "played": played, "reason": "lost", "track": track}))?);
                }
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
                    // and its microphone that event's: a microphone's change opens with a hole; a
                    // play's runs are in its own samples, never the track's
                    let (start, mic) = held_events
                        .iter()
                        .rev()
                        .find(|event| {
                            event["boot"] == boot
                                && event["e"]["track"] == track
                                && event["e"]["start"].is_u64()
                                && event["e"]["kind"] != "sound"
                        })
                        .map_or((0, Value::Null), |event| {
                            let heard = &event["e"];
                            let mic = heard.get("mic").or_else(|| heard.get("speaker"));
                            (end(heard), mic.cloned().unwrap_or_default())
                        });
                    let num: u64 = name.trim_end_matches(".pcm").parse().unwrap_or_default();
                    let logged = guard.emit(&labelled(json!({"kind": "segment", "n": num, "path": path, "samples": frames.saturating_mul(320), "start": start, "track": track}), &mic))?;
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
                    // writer drains; a giver's mouth and play
                    let (mut device, mut clock, mut pinged, mut queue, mut giving) = (
                        None::<String>,
                        None::<(u64, u64)>,
                        Instant::now(),
                        None::<SyncSender<String>>,
                        None::<(String, u64)>,
                    );
                    // an engine's track and kind
                    let mut engaged = None::<(String, String)>;
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
                        let track = request["track"].as_str().unwrap_or_default();
                        let tracked = track
                            .split_once('-')
                            .is_some_and(|(kind, name)| well(kind, name));
                        let named = request["kind"]
                            .as_str()
                            .zip(request["name"].as_str())
                            // only a mouth plays
                            .filter(|&(kind, name)| {
                                well(kind, name) && (kind == "mouth" || request["plays"] != true)
                            })
                            .map(|(kind, name)| format!("{kind}-{name}"));
                        // an engine's kind and name: echo, turn and words on an ear, speech on a
                        // mouth, `from` a turn's and a words engine's alone
                        let role = request["kind"]
                            .as_str()
                            .zip(request["name"].as_str())
                            .filter(|&(kind, name)| {
                                let reads = matches!(kind, "turn" | "words");
                                tracked
                                    && well("ear", name)
                                    && match kind {
                                        "echo" => track.starts_with("ear-"),
                                        "speech" => track.starts_with("mouth-"),
                                        _ => reads && track.starts_with("ear-"),
                                    }
                                    && request.get("from").is_some() == reads
                                    && (request["from"].is_null() || request["from"].is_u64())
                            });
                        // what a frame names: a mouth's its output
                        let heard = &request[if device
                            .as_ref()
                            .is_some_and(|name| name.starts_with("mouth-"))
                        {
                            "speaker"
                        } else {
                            "mic"
                        }];
                        // lowercase digits only: from_str_radix alone takes uppercase and a `+`
                        let digits = |text: &&str| {
                            text.bytes()
                                .all(|digit| matches!(digit, b'0'..=b'9' | b'a'..=b'f'))
                        };
                        // a give's audio: 2 to 16384 bytes of whole samples
                        let given = request["pcm"].as_str().filter(|text| {
                            (4..=0x8000).contains(&text.len())
                                && text.len().is_multiple_of(4)
                                && digits(text)
                        });
                        let pcm = request["pcm"]
                            .as_str()
                            .filter(|text| {
                                (heard.is_null() || heard.is_string())
                                    && text.len() == FRAME * 2
                                    && digits(text)
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
                            // a device's connection carries its frames, never a stream of replies,
                            // and a play's its gives
                            (
                                true,
                                Some(
                                    "device" | "engine" | "feed" | "hush" | "listen" | "partials"
                                    | "play" | "read",
                                ),
                            ) if device.is_some() => (Ok(error("bad_request")), Next::Go),
                            (true, Some(op)) if giving.is_some() && op != "give" && op != "end" => {
                                (Ok(error("bad_request")), Next::Go)
                            }
                            (true, Some(_)) if engaged.is_some() => (
                                guard.report(&engaged.clone().unwrap_or_default(), &request),
                                Next::Go,
                            ),
                            (true, Some("engine"))
                                if request["from"].as_u64().is_some_and(|from| {
                                    from < now()
                                        .saturating_sub(guard.window.saturating_mul(1_000_000))
                                }) =>
                            {
                                (Ok(error("gone")), Next::Go)
                            }
                            (true, Some("engine"))
                                if role.is_some_and(|(kind, _)| {
                                    guard
                                        .engines
                                        .contains_key(&(track.to_owned(), kind.to_owned()))
                                }) =>
                            {
                                (Ok(error("taken")), Next::Go)
                            }
                            (true, Some("engine")) if role.is_some() => {
                                let (kind, name) = role.unwrap_or_default();
                                let on = guard.emit(&json!({"kind": "engine", "name": name, "on": true, "stage": kind, "track": track}));
                                let (sender, incoming) = mpsc::sync_channel(BEHIND);
                                match on {
                                    Ok(_) => {
                                        let slot = (track.to_owned(), kind.to_owned());
                                        // a turn or words engine's frames: from disk from its
                                        // `from` (none for live), then live as each is placed
                                        let (backlog, reach) = guard.replay(
                                            track,
                                            request["from"].as_u64().unwrap_or(u64::MAX),
                                            kind == "words",
                                        );
                                        let mut reply = json!({"ok": true, "seq": guard.seq});
                                        if !request["id"].is_null() {
                                            reply["id"] = request["id"].clone();
                                        }
                                        if matches!(kind, "turn" | "words") {
                                            let latest = |what: &str| {
                                                guard
                                                    .latest
                                                    .get(&(track.to_owned(), what.to_owned()))
                                                    .copied()
                                            };
                                            // a turn engine's: its turns' and its voice's
                                            reply["through"] = latest(kind)
                                                .max(latest(&kind.replace("turn", "voice")))
                                                .into();
                                        }
                                        // what it reads: an echo engine its ear's frames as a
                                        // listener does and every mouth's runs; a speech engine
                                        // its mouth's says; a turn engine the ruled frames; a
                                        // words engine those and the turns
                                        let topics = match kind {
                                            "echo" => vec![track.to_owned(), "runs".to_owned()],
                                            "speech" => vec![format!("{track} say")],
                                            "words" => vec![
                                                format!("{track} heard"),
                                                format!("{track} turn"),
                                            ],
                                            _ => vec![format!("{track} heard")],
                                        };
                                        for topic in topics {
                                            guard.feeds.push((sender.clone(), key, topic));
                                        }
                                        let ruled = (guard.tracks.get(track))
                                            .and_then(|held| held.end)
                                            .unwrap_or_default();
                                        drop(guard.engines.insert(
                                            slot.clone(),
                                            Engine {
                                                given: reach,
                                                key,
                                                name: name.to_owned(),
                                                ruled,
                                                voice: false,
                                            },
                                        ));
                                        (queue, engaged) = (Some(sender), Some(slot));
                                        (
                                            Ok(Value::Null),
                                            Next::Device(
                                                Box::new(
                                                    iter::once(format!("{reply}\n")).chain(backlog),
                                                ),
                                                incoming,
                                            ),
                                        )
                                    }
                                    Err(cause) => (Err(cause), Next::Go),
                                }
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
                                let plays = request["plays"] == true;
                                let held = guard.tracks.entry(name.clone()).or_default();
                                (held.conn, held.plays) = (Some(key), plays);
                                guard.feeds.push((sender.clone(), key, "ears".to_owned()));
                                let mut reply = json!({"ears": guard.ears, "ok": true, "ping": now(), "track": name});
                                // a playing one is told its plays under a topic of its own
                                if plays {
                                    reply["plays"] = true.into();
                                    guard.feeds.push((sender.clone(), key, format!("{name}>")));
                                }
                                queue = Some(sender);
                                device = Some(name);
                                (Ok(reply), Next::Device(Box::new(iter::empty()), incoming))
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
                                        // quiet on hushes every play, each waited on apart
                                        for playing in
                                            guard.plays.keys().filter(|_| op == "quiet" && on)
                                        {
                                            let mouth = playing.clone();
                                            drop(thread::spawn(move || {
                                                drop(quell(lock(live), &mouth, "quiet", PATIENCE));
                                            }));
                                        }
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
                                            let (known, mic) = (state.tracks.get(&name), heard);
                                            let (start, event) = match known
                                                .and_then(|held| held.end)
                                            {
                                                None => (
                                                    at,
                                                    Some(labelled(
                                                        json!({"kind": "track", "start": at, "track": name}),
                                                        mic,
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
                                                    let gap = labelled(
                                                        json!({"kind": "hole", "samples": samples(at.saturating_sub(last)), "start": last, "track": name}),
                                                        mic,
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
                                                heard.clone(),
                                            );
                                            // the frame is placed: a close that fails to log
                                            // leaves the segment open for the next frame's close
                                            if full >= state.segment {
                                                drop(state.close(Some(&name)));
                                            }
                                            let sound = hex(&bytes);
                                            state.fan(
                                                &name,
                                                &format!("{}\n", json!({"pcm": sound, "t": start})),
                                            );
                                            state.hear(&name, start, sound);
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
                            // a giver's audio: counted as it is read and passed to the mouth, its
                            // reply withheld while the mouth holds `$RIFT_AHEAD_MS` of it untaken
                            (true, Some("give")) if giving.is_some() && given.is_none() => {
                                (Ok(error("bad_frame")), Next::Go)
                            }
                            // only to a play neither ended nor hushed: a give after either is
                            // refused, never counted or passed on
                            (true, Some("give"))
                                if giving.as_ref().is_some_and(|held| {
                                    guard.plays.get(&held.0).is_some_and(|play| {
                                        play.num == held.1 && !play.end && play.hushed.is_none()
                                    })
                                }) =>
                            {
                                let (mouth, num) = giving.clone().unwrap_or_default();
                                let (ahead, text) = (guard.ahead, given.unwrap_or_default());
                                // four hex digits a sample
                                let count = u64::try_from(text.len())
                                    .unwrap_or_default()
                                    .checked_div(4)
                                    .unwrap_or_default();
                                let (total, rate) =
                                    guard.plays.get_mut(&mouth).map_or((0, 0), |play| {
                                        // a give after a pause starts the clock afresh
                                        (play.given, play.heard) =
                                            (play.given.saturating_add(count), Instant::now());
                                        (play.given, play.rate)
                                    });
                                let most = ahead
                                    .saturating_mul(rate)
                                    .checked_div(1000)
                                    .unwrap_or_default();
                                guard
                                    .tell(&mouth, &json!({"pcm": text, "play": num, "rate": rate}));
                                let open = |held: &Live, full: u64| {
                                    held.plays.get(&mouth).is_some_and(|play| {
                                        play.num == num
                                            && play.given.saturating_sub(play.played()) >= full
                                    })
                                };
                                while open(&guard, most) {
                                    guard =
                                        SLACK.wait(guard).unwrap_or_else(PoisonError::into_inner);
                                }
                                let next = if open(&guard, 0) {
                                    Next::Go
                                } else {
                                    Next::Close
                                };
                                (Ok(json!({"given": total, "ok": true})), next)
                            }
                            (true, Some("end"))
                                if giving.as_ref().is_some_and(|held| {
                                    guard
                                        .plays
                                        .get(&held.0)
                                        .is_some_and(|play| play.num == held.1 && !play.end)
                                }) =>
                            {
                                let (mouth, num) = giving.clone().unwrap_or_default();
                                let empty = guard.plays.get_mut(&mouth).is_some_and(|play| {
                                    (play.end, play.heard) = (true, Instant::now());
                                    play.given == 0
                                });
                                guard.tell(&mouth, &json!({"end": num}));
                                // given nothing, it has played all it was given
                                if empty {
                                    guard.finish(&mouth, "end");
                                }
                                (Ok(json!({"ok": true})), Next::Go)
                            }
                            (true, Some("hush")) if tracked && track.starts_with("mouth-") => {
                                let (held, mut last) = quell(guard, track, "hush", PATIENCE);
                                (guard, last["ok"]) = (held, true.into());
                                (Ok(last), Next::Go)
                            }
                            (true, Some("play"))
                                if tracked
                                    && track.starts_with("mouth-")
                                    && request["rate"]
                                        .as_u64()
                                        .is_some_and(|rate| (8000..=48_000).contains(&rate)) =>
                            {
                                let refused = if guard.quiet {
                                    "quiet"
                                } else if !guard
                                    .tracks
                                    .get(track)
                                    .is_some_and(|held| held.conn.is_some() && held.plays)
                                {
                                    "absent"
                                } else if guard.plays.contains_key(track) {
                                    "busy"
                                } else {
                                    ""
                                };
                                let num = guard.numbered.saturating_add(1);
                                let rate = request["rate"].as_u64().unwrap_or_default();
                                let opened = if refused.is_empty() {
                                    guard.emit(&json!({"kind": "play", "play": num, "rate": rate, "track": track})).map(|_| {
                                        guard.numbered = num;
                                        let opening = json!({"play": num, "rate": rate, "track": track});
                                        guard.fan("runs", &format!("{opening}\n"));
                                        let _: Option<Play> = guard.plays.insert(track.to_owned(), Play { end: false, given: 0, giver: key, heard: Instant::now(), hushed: None, num, rate, run: None });
                                        giving = Some((track.to_owned(), num));
                                        json!({"ok": true, "play": num, "seq": guard.seq})
                                    })
                                } else {
                                    Ok(error(refused))
                                };
                                (opened, Next::Go)
                            }
                            // a playing mouth's reports, answered by none: the run so far, then the
                            // play over
                            (true, Some(op @ ("done" | "sound")))
                                if device.as_ref().is_some_and(|name| {
                                    guard.tracks.get(name).is_some_and(|held| held.plays)
                                }) && request["play"].is_u64()
                                    && (op == "done"
                                        || request["from"].is_u64()
                                            && request["samples"].is_u64()
                                            && request["t"].is_u64()) =>
                            {
                                let mouth = device.clone().unwrap_or_default();
                                let num = request["play"].as_u64();
                                let reply = match clock {
                                    None => error("unsynced"),
                                    Some((theirs, ours)) => {
                                        let state = &mut *guard;
                                        let field =
                                            |name: &str| request[name].as_u64().unwrap_or_default();
                                        let start =
                                            field("t").saturating_add(ours).saturating_sub(theirs);
                                        match state
                                            .plays
                                            .get_mut(&mouth)
                                            .filter(|play| Some(play.num) == num)
                                        {
                                            Some(play) if op == "done" => {
                                                let reason = play.hushed.unwrap_or(if play.end {
                                                    "end"
                                                } else {
                                                    "mouth"
                                                });
                                                state.finish(&mouth, reason);
                                            }
                                            Some(play) => {
                                                // a new first sample starts the next run, the last logged
                                                let from = field("from");
                                                let (past, kept) = (
                                                    play.run.filter(|run| run.0 != from),
                                                    play.run.filter(|run| run.0 == from),
                                                );
                                                (play.run, play.heard) = (
                                                    Some((
                                                        from,
                                                        field("samples"),
                                                        kept.map_or(start, |run| run.2),
                                                    )),
                                                    Instant::now(),
                                                );
                                                let (played, growing) = (play.num, play.run);
                                                if let Some((first, samples, begun)) = past {
                                                    drop(state.emit(&json!({"from": first, "kind": "sound", "play": played, "samples": samples, "start": begun, "track": mouth})));
                                                }
                                                // the run as it stands to every echo engine
                                                if let Some((first, samples, begun)) = growing {
                                                    let told = json!({"from": first, "play": played, "samples": samples, "start": begun, "track": mouth});
                                                    state.fan("runs", &format!("{told}\n"));
                                                }
                                                SLACK.notify_all();
                                            }
                                            None => {}
                                        }
                                        Value::Null
                                    }
                                };
                                (Ok(reply), Next::Go)
                            }
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
                            // a words engine's partials, live, until it leaves
                            (true, Some("partials"))
                                if tracked
                                    && !guard
                                        .engines
                                        .contains_key(&(track.to_owned(), "words".to_owned())) =>
                            {
                                (Ok(error("absent")), Next::Go)
                            }
                            (true, Some("partials")) if tracked => {
                                let (sender, incoming) = mpsc::sync_channel(BEHIND);
                                guard.feeds.push((sender, key, format!("{track} partial")));
                                (
                                    Ok(json!({"ok": true})),
                                    Next::Feed(Box::new(iter::empty()), incoming),
                                )
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
                                let (ping, theirs, back) = (
                                    request["ping"].as_u64().unwrap_or_default(),
                                    request["t"].as_u64().unwrap_or_default(),
                                    now(),
                                );
                                // the pong was stamped between the ping and its reading: the
                                // first is placed at the midpoint, a later one moves the offset
                                // only as far as that span proves it wrong, so a late pong moves
                                // nothing
                                let ours = clock.map_or_else(
                                    || ping.midpoint(back),
                                    |(was, at)| {
                                        theirs
                                            .saturating_add(at)
                                            .saturating_sub(was)
                                            .max(ping)
                                            .min(back)
                                    },
                                );
                                clock = Some((theirs, ours));
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
                                    let mut spans: Vec<(u64, PathBuf)> = guard
                                        .segments(track)
                                        .iter()
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
                                    "device" | "done" | "ears" | "end" | "engine" | "feed"
                                    | "frame" | "give" | "hush" | "keep" | "listen" | "partials"
                                    | "play" | "pong" | "quiet" | "read" | "sound",
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
                            Next::Device(backlog, incoming) => {
                                drop(writer.try_clone().map(|mut out| {
                                    thread::spawn(move || {
                                        backlog
                                            .chain(incoming)
                                            .try_for_each(|line| out.write_all(line.as_bytes()))
                                    })
                                }));
                            }
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
                    // a play's last line once it is over; its giver gone before `end` hushes it,
                    // after `end` leaves it playing, with no one to tell
                    if let Some((mouth, num)) = giving {
                        let mut guard = lock(live);
                        let unended = (guard.plays.get(&mouth))
                            .is_some_and(|play| play.num == num && !play.end);
                        if unended {
                            guard = quell(guard, &mouth, "left", PATIENCE).0;
                        }
                        let last = (guard.ended.get(&mouth))
                            .filter(|last| last["play"] == num)
                            .cloned();
                        drop(guard);
                        if let Some(line) = last {
                            drop(writer.write_all(format!("{line}\n").as_bytes()));
                        }
                    }
                    drop(writer.shutdown(Shutdown::Both));
                    let mut guard = lock(live);
                    drop(guard.conns.remove(&key));
                    guard.feeds.retain(|feed| feed.1 != key);
                    // an engine's end however it comes, unless the drain logged it
                    if let Some(slot) = engaged {
                        guard.leave(Some(&slot));
                    }
                    // a device's open segment closes as it leaves, and the play it held ends
                    if let Some(name) = device {
                        guard.finish(&name, "mouth");
                        drop(guard.close(Some(&name)));
                        let held = guard.tracks.entry(name).or_default();
                        (held.conn, held.plays) = (None, false);
                    }
                    drop(guard);
                    Ok(())
                };
                drop(thread::spawn(connection));
            };
            // stop accepting, the open segments closed, then the last event, then the feeds end
            // once their queues are sent
            drop(listener);
            let budget = Duration::from_millis(setting("RIFT_DRAIN_MS", 1000));
            {
                // every play hushed and waited on inside the budget, else ended at its last report
                let (mut guard, begun) = (lock(live), Instant::now());
                while let Some(track) = guard.plays.keys().next().cloned() {
                    let left = budget.saturating_sub(begun.elapsed());
                    guard = quell(guard, &track, "stop", left).0;
                }
                // every engine's end logged before `stop`, which nothing follows
                guard.leave(None);
                drop(guard.close(None));
                drop(guard.emit(&json!({"kind": "stop", "reason": reason})));
                guard.state = STOPPING;
                guard.feeds.clear();
            }
            // open connections may still ask for health inside the budget; then they are cut
            let begun = Instant::now();
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
            // the ear's program: the signed release copied to one path every release shares,
            // since macOS keys a bare program's microphone grant by its path (observed
            // 2026-11-26 · tccd prompting anew for each `hotswap/rift/<version>/rift`)
            let fixed = home.join(".local/libexec/rift/rift");
            // the mouth's: a copy of its own beside it, a path no setup older than the mouth
            // writes, so a rollback leaves the mouth on a release that knows it
            let mouthcopy = fixed.with_file_name("mouth");
            let (label, ear, mouth) = (
                "dev.gigantic.rift",
                "dev.gigantic.rift.ear",
                "dev.gigantic.rift.mouth",
            );
            // an agent's plist: `program` run with `said`, `keep` its KeepAlive, `err` its stderr
            let sheet = |name: &str, program: &Path, said: &str, keep: &str, err: &str| {
                format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>EnvironmentVariables</key>
  <dict><key>RIFT_ROOT</key><string>{}</string></dict>
  <key>ExitTimeOut</key>
  <integer>3</integer>
  <key>KeepAlive</key>
  {keep}
  <key>Label</key>
  <string>{name}</string>
  <key>ProgramArguments</key>
  <array><string>{}</string>{said}</array>
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
                    program.display(),
                    // beside run/, not in it: launchd creates the parent of this path 0744, which serve refuses
                    root.join(err).display()
                )
            };
            let path = home.join(format!("Library/LaunchAgents/{label}.plist"));
            let heard = path.with_file_name(format!("{ear}.plist"));
            let mouthfile = path.with_file_name(format!("{mouth}.plist"));
            let install = || -> io::Result<()> {
                // a tool call past `$RIFT_SIGN_MS` is killed, since a keychain asking at the
                // screen, or a launchctl that never answers, would hold setup forever
                let allowed = Duration::from_millis(setting("RIFT_SIGN_MS", 30_000));
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
                // the standing ear's track is this Mac's for good: its local host name, which
                // Sharing sets and no network renames, lowercased
                let named = || -> io::Result<String> {
                    let host = ask("scutil", &["--get", "LocalHostName"])?;
                    let mac = String::from_utf8_lossy(&host.stdout)
                        .trim()
                        .to_ascii_lowercase();
                    if !host.status.success() || !well("ear", &mac) {
                        return Err(io::Error::other(format!(
                            "scutil --get LocalHostName gave {:?}, no track name (1 to 64 letters, digits, - or _)",
                            String::from_utf8_lossy(&[host.stdout, host.stderr].concat()).trim()
                        )));
                    }
                    Ok(mac)
                };
                let plist = sheet(
                    label,
                    &exe,
                    "<string>serve</string>",
                    "<dict><key>SuccessfulExit</key><false/></dict>",
                    "serve.err",
                );
                // a standing device's: the ear's on its fixed copy, the mouth's on its own
                let standing = |job: &str, kind: &str, flag: &str, mac: &str| {
                    sheet(
                        job,
                        if kind == "ear" { &fixed } else { &mouthcopy },
                        &format!(
                            "<string>device</string><string>{kind}</string><string>{mac}</string><string>{flag}</string>"
                        ),
                        "<true/>",
                        &format!("{kind}.err"),
                    )
                };
                // with no host name (off macOS, no scutil) there is no ear and no mouth: the
                // daemon's alone
                if words.contains(&"--print") {
                    print!(
                        "{plist}{}",
                        named().map_or_else(
                            |_| String::new(),
                            |mac| format!(
                                "{}{}",
                                standing(ear, "ear", "--mic", &mac),
                                standing(mouth, "mouth", "--speaker", &mac)
                            )
                        )
                    );
                    return Ok(());
                }
                let uid = Command::new("id").arg("-u").output()?.stdout;
                let gui = format!("gui/{}", String::from_utf8_lossy(&uid).trim());
                // under the same deadline as the signing tools: a launchctl that never answers
                // fails setup naming it, never holds it
                let call = |line: &[&str]| {
                    ask("launchctl", line).map_err(|cause| {
                        io::Error::other(format!("{cause} (setup installs launchd agents, macOS only; --print prints their plists)"))
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
                // rebuild at the ear's fixed path keeps the microphone's grant
                let keychain = root.join("rift.keychain-db");
                let (kc, pass) = (keychain.to_string_lossy(), root.join("rift.keychain-pass"));
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
                    for (job, file) in [(ear, &heard), (mouth, &mouthfile), (label, &path)] {
                        unload(job)?;
                        gone(file);
                    }
                    gone(&exe);
                    gone(&fixed);
                    gone(&mouthcopy);
                    drop(fixed.parent().map(fs::remove_dir));
                    // files, never `security delete-keychain`, which might ask at the screen to
                    // unlock it; setup never puts it on the search list
                    gone(&keychain);
                    gone(&pass);
                    return Ok(());
                }
                // asked before anything is written: no launchctl (Linux) leaves nothing behind
                let swap =
                    loaded(label)? && fs::read_to_string(&path).is_ok_and(|held| held == plist);
                let mac = named()?;
                let (listened, mouthsheet) = (
                    standing(ear, "ear", "--mic", &mac),
                    standing(mouth, "mouth", "--speaker", &mac),
                );
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
                // trimmed: a hand-made file's newline would be part of the password
                let secret = fs::read_to_string(&pass)?.trim_end().to_owned();
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
                    // OpenSSL 3 alike
                    need(
                        "sh",
                        &[
                            "-c",
                            r#"set -e; rm -rf "$1"; mkdir -m 700 "$1"; trap 'rm -rf "$1"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -days 36500 -subj /CN=rift -addext keyUsage=critical,digitalSignature -addext extendedKeyUsage=critical,codeSigning -keyout "$1/key.pem" -out "$1/cert.pem"
openssl pkcs12 -export -inkey "$1/key.pem" -in "$1/cert.pem" -out "$1/identity.p12" -passout "pass:$2" -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1
security import "$1/identity.p12" -k "$3" -P "$2" -T /usr/bin/codesign"#,
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
                // every run, else codesign's use of the key asks at the screen: an import that a
                // failure or a kill left without one is found next run and never made again
                need(
                    "security",
                    &[
                        "set-key-partition-list",
                        "-S",
                        "apple-tool:,apple:",
                        "-s",
                        "-k",
                        &secret,
                        &kc,
                    ],
                )?;
                // the release itself, not the link: signed unless it already is, by this identity
                let target = fs::canonicalize(&linked)?;
                let program = target.to_string_lossy();
                let wanted =
                    format!("-R=identifier \"{label}\" and certificate leaf = H\"{hash}\"");
                let signed = !ask("codesign", &["-v", &wanted, &program])?
                    .status
                    .success();
                if signed {
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
                // the signed release at a fixed path, unless already there byte for byte; renamed
                // over it, so a running device keeps the file it mapped
                let place = |at: &Path| -> io::Result<()> {
                    if fs::read(at).ok() != Some(fs::read(&target)?) {
                        let staged = at.with_added_extension("new");
                        at.parent().map_or(Ok(()), fs::create_dir_all)?;
                        let _: u64 = fs::copy(&target, &staged)?;
                        fs::rename(&staged, at)?;
                        println!("installed {} as {}", target.display(), at.display());
                    }
                    Ok(())
                };
                let write = |file: &Path, text: &str| -> io::Result<()> {
                    let staged = file.with_extension("plist.new");
                    fs::write(&staged, text)?;
                    fs::rename(&staged, file)?;
                    println!("wrote {}", file.display());
                    Ok(())
                };
                place(&fixed)?;
                path.parent().map_or(Ok(()), fs::create_dir_all)?;
                write(&path, &plist)?;
                write(&heard, &listened)?;
                // the health of a daemon answering ready on the socket, if one does
                let serving = || -> Option<Value> {
                    let mut stream = UnixStream::connect(&sock).ok()?;
                    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
                    let hello =
                        json!({"client": format!("rift/{VERSION}"), "op": "hello", "proto": PROTO});
                    writeln!(stream, "{hello}\n{}", json!({"op": "health"})).ok()?;
                    let reply: Value =
                        serde_json::from_str(&BufReader::new(stream).lines().nth(1)?.ok()?).ok()?;
                    (reply["state"] == "ready").then_some(reply)
                };
                // launchd holds a respawn until ThrottleInterval after the last launch: wait it out
                let ready = |wait: Duration, done: &dyn Fn(&Value) -> bool| {
                    let begun = Instant::now();
                    while !serving().is_some_and(|health| done(&health)) {
                        if begun.elapsed() > wait {
                            return false;
                        }
                        thread::sleep(Duration::from_millis(100));
                    }
                    true
                };
                let (first, patience) = (
                    Duration::from_millis(setting("RIFT_RELOAD_MS", 15_000)),
                    Duration::from_millis(setting("RIFT_READY_MS", 30_000)),
                );
                // `name` kickstarted, else loaded afresh, then waited on until the daemon's
                // health is `done`; a job launchd refused to spawn stays refused through any
                // kickstart, so one not done past the first budget is loaded afresh, once
                let up = |name: &str,
                          file: &Path,
                          err: &str,
                          kick: bool,
                          done: &dyn Fn(&Value) -> bool| {
                    let load = || -> io::Result<()> {
                        unload(name)?;
                        must(&["bootstrap", &gui, &file.to_string_lossy()])
                    };
                    if kick {
                        must(&["kickstart", "-k", &format!("{gui}/{name}")])?;
                    } else {
                        load()?;
                    }
                    if ready(first, done) {
                        return Ok(());
                    }
                    println!(
                        "{name} not ready {} ms after its load; loading it again",
                        first.as_millis()
                    );
                    load()?;
                    if ready(patience, done) {
                        return Ok(());
                    }
                    Err(io::Error::other(format!(
                        "{name} not ready {} ms after its load; see {}",
                        patience.as_millis(),
                        root.join(err).display()
                    )))
                };
                // the ear and the mouth out first, so the new daemon lists only what loads after it
                unload(ear)?;
                unload(mouth)?;
                // the same plist loaded and the release not re-signed: a swap is one kickstart;
                // else it is loaded afresh, since launchd refused a kickstart onto a changed
                // signature (AGENTS.md, observed 2026-11-25)
                let before = serving().map(|health| health["pid"].clone());
                up(label, &path, "serve.err", swap && !signed, &|health| {
                    Some(&health["pid"]) != before.as_ref()
                })?;
                // the ear, then the mouth, only once the daemon is up: whatever fails there leaves
                // it serving. One disabled in launchd is written, never loaded: the way to opt out
                let disabled =
                    String::from_utf8_lossy(&call(&["print-disabled", &gui])?.stdout).into_owned();
                let stand = |job: &str, kind: &str, file: &Path| -> io::Result<()> {
                    if disabled.contains(&format!("\"{job}\" => disabled")) {
                        println!(
                            "{job} is disabled, so written and not loaded; launchctl enable {gui}/{job} stands it"
                        );
                        return Ok(());
                    }
                    let track = json!(format!("{kind}-{mac}"));
                    let listed = |health: &Value| {
                        health["devices"]
                            .as_array()
                            .is_some_and(|held| held.contains(&track))
                    };
                    // a device already holding the track (a hand-run one): the standing one is
                    // loaded to take it over once that one leaves, and there is no readiness to
                    // wait for
                    if serving().is_some_and(|health| listed(&health)) {
                        must(&["bootstrap", &gui, &file.to_string_lossy()])?;
                        println!(
                            "{kind}-{mac} is held by another device; {job} takes it over once it leaves"
                        );
                        return Ok(());
                    }
                    up(job, file, &format!("{kind}.err"), false, &listed)
                };
                stand(ear, "ear", &heard)?;
                // the mouth's copy and plist written only now, the daemon and the ear good; from
                // here a failure boots it out and takes both away, so a failed setup, and the
                // older setup a rollback runs after it, find nothing of the mouth
                (|| {
                    place(&mouthcopy)?;
                    write(&mouthfile, &mouthsheet)?;
                    stand(mouth, "mouth", &mouthfile)
                })()
                .inspect_err(|_| {
                    // the files go whatever the bootout says; the cause is what is reported
                    drop(unload(mouth));
                    gone(&mouthfile);
                    gone(&mouthcopy);
                })
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
                "usage: rift serve | status | quiet on|off | ears on|off | keep <window s> [<archive>] | feed [--after <boot>:<seq>] | listen <track> | read <track> <from us> <to us> | play <track> <rate> [<file.pcm>] | hush <track> | device ear|mouth <name> <file.pcm> [--fast] | device ear <name> --mic [<microphone>] | device mouth <name> --speaker | engine run tail <ear> [--tail-ms <ms>] | setup [--print | --remove] | upgrade [--check | --back | --retry] | version"
            );
            ExitCode::from(2)
        }
    }
}

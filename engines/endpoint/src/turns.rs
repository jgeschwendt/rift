//! The endpoint's work, its models aside: a speech probability per 32 ms window segmented into
//! voice, a score of the turn so far at each voice's close deciding the turn, and one
//! connection's frames in and reports out (docs/protocol.html#engines).

use std::path::Path;
use std::{io, iter};

use link::{Conn, RATE, samples};
use serde_json::{Value, json};

/// Samples per window at 16 kHz, Silero's fixed 32 ms.
pub const WINDOW: usize = 512;
/// Smart Turn's context: the turn's last 8 s.
pub const CONTEXT: usize = 128_000;
/// A window's span in µs.
const SPAN: u64 = 32_000;
/// The hello's client.
const CLIENT: &str = "endpoint/0.1.0";
/// Logged events read back on connect for the turn the last engine held: a held turn's voices
/// lie within its ceiling, far inside this many.
const LOOK: u64 = 256;
/// A window at or over this opens a voice (Silero's own onset).
const ONSET: f32 = 0.5;
/// Windows under this count toward a close: Silero's hysteresis, its onset less 0.15.
const OFFSET: f32 = 0.35;
/// Windows under [`OFFSET`] in a row that close a voice: 8 × 32 ms = 256 ms.
const CLOSE: u32 = 8;
/// The close window in µs: a hole shorter than this is heard as silence of its length, a
/// longer one closes the voice under way at the hole's start and forgets the model's state.
const QUIET: u64 = 256_000;
/// The speech a voice spans before it is reported, 3 windows: a blip, or the first window a
/// model hears after a hole, is never a voice of its own.
const VOICED: u64 = 96_000;

/// A speech probability per [`WINDOW`], stateful across a stream.
pub trait Prob {
    /// The probability that `window` holds speech.
    fn prob(&mut self, window: &[f32]) -> io::Result<f32>;
    /// Forgets the stream: a hole or a new connection.
    fn reset(&mut self);
}

/// The confidence that a turn's audio, its last [`CONTEXT`] samples, is a finished turn.
pub trait Score {
    /// The score for `tail`.
    fn score(&mut self, tail: &[f32]) -> io::Result<f32>;
}

/// A voice heard and not yet closed.
#[derive(Clone, Copy, Debug)]
struct Voice {
    /// The end of its last speech-like window.
    last: u64,
    /// Windows under [`OFFSET`] since.
    quiet: u32,
    /// Whether its start was reported: only once it spans [`VOICED`].
    said: bool,
    /// The start of its first window.
    start: u64,
}

/// One stream's segmenter and verdicts, every time on the daemon's clock in µs.
#[derive(Debug)]
pub struct Endpoint<'model, P, S> {
    /// The time of `carry`'s first sample.
    at: u64,
    /// A score over this commits a turn.
    bar: f32,
    /// Samples short of a window.
    carry: Vec<f32>,
    /// How long a turn held under the bar waits in silence before it is closed anyway.
    ceiling: u64,
    /// A turn held under the bar: its end and when it was held.
    held: Option<(u64, u64)>,
    /// Where the next frame is due: the end of the last, none before the first.
    next: Option<u64>,
    /// The speech model.
    prob: &'model mut P,
    /// The turn model.
    score: &'model mut S,
    /// The audio since the last turn, its last [`CONTEXT`] samples.
    tail: Vec<f32>,
    /// The start of the turn under way.
    turn: Option<u64>,
    /// The voice under way.
    voice: Option<Voice>,
}

impl<'model, P: Prob, S: Score> Endpoint<'model, P, S> {
    /// Closes the voice under way, if one was reported: its end (`edge`, else its last speech)
    /// reported, the turn scored, committed over the bar, else held. One never reported closes
    /// with nothing said.
    fn close(&mut self, edge: Option<u64>, said: &mut Vec<Value>) -> io::Result<()> {
        if let Some(voice) = self.voice.take().filter(|voice| voice.said) {
            let end = edge.unwrap_or(voice.last);
            said.push(json!({"on": false, "op": "voice", "start": end}));
            if self.score.score(&self.tail)? > self.bar {
                self.commit(end, said);
            } else {
                self.held = Some((end, self.at.max(end)));
            }
        }
        Ok(())
    }

    /// Reports the turn under way as ending at `end`; the next starts from silence.
    fn commit(&mut self, end: u64, said: &mut Vec<Value>) {
        let start = self.turn.take().unwrap_or(end);
        said.push(json!({"end": end, "op": "turn", "start": start}));
        self.tail.clear();
        self.held = None;
    }

    /// The reports the frame at `time` holding `pcm` completes, in order. A jump in `time` is a
    /// hole: one shorter than [`QUIET`] is heard as silence of its length; a longer one (or a
    /// step back) closes the voice under way at the hole's start and forgets the model's state.
    #[inline]
    pub fn frame(&mut self, time: u64, pcm: &[f32]) -> io::Result<Vec<Value>> {
        let mut said = Vec::new();
        if let Some(due) = self.next.filter(|&due| due != time) {
            let gap = time.saturating_sub(due);
            if (1..QUIET).contains(&gap) {
                let silence = vec![0.0; gap.saturating_mul(RATE).div_euclid(1_000_000) as usize];
                self.hear(due, &silence, &mut said)?;
            } else {
                self.close(Some(due), &mut said)?;
                self.prob.reset();
                self.carry.clear();
            }
        }
        self.hear(time, pcm, &mut said)?;
        Ok(said)
    }

    /// Hears `pcm` from `time` on, window by window.
    fn hear(&mut self, time: u64, pcm: &[f32], said: &mut Vec<Value>) -> io::Result<()> {
        if self.carry.is_empty() {
            self.at = time;
        }
        self.next = Some(
            time.saturating_add(
                (pcm.len() as u64)
                    .saturating_mul(1_000_000)
                    .div_euclid(RATE),
            ),
        );
        self.carry.extend_from_slice(pcm);
        self.tail.extend_from_slice(pcm);
        drop(self.tail.drain(..self.tail.len().saturating_sub(CONTEXT)));
        while self.carry.len() >= WINDOW {
            let speech = self.prob.prob(&self.carry[..WINDOW])?;
            drop(self.carry.drain(..WINDOW));
            let start = self.at;
            self.at = self.at.saturating_add(SPAN);
            match self.voice.as_mut() {
                None if speech >= ONSET => {
                    self.voice = Some(Voice {
                        last: self.at,
                        quiet: 0,
                        said: false,
                        start,
                    });
                }
                Some(voice) if speech >= OFFSET => (voice.last, voice.quiet) = (self.at, 0),
                Some(voice) if voice.quiet.saturating_add(1) < CLOSE => {
                    voice.quiet = voice.quiet.saturating_add(1);
                }
                Some(_) => self.close(None, said)?,
                None => {}
            }
            // a voice is reported once it spans enough speech, from its first window
            if let Some(voice) = self
                .voice
                .as_mut()
                .filter(|voice| !voice.said && voice.last.saturating_sub(voice.start) >= VOICED)
            {
                voice.said = true;
                said.push(json!({"on": true, "op": "voice", "start": voice.start}));
                let _: &mut u64 = self.turn.get_or_insert(voice.start);
                self.held = None;
            }
            if let Some((end, since)) = self.held
                && self.at.saturating_sub(since) >= self.ceiling
            {
                self.commit(end, said);
            }
        }
        Ok(())
    }

    /// A fresh stream: `bar` the score that commits a turn, `ceiling` µs of silence that close
    /// one held under it.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            clippy::single_call_fn,
            reason = "the constructor, named for its type; the tests call it as often"
        )
    )]
    pub fn new(prob: &'model mut P, score: &'model mut S, bar: f32, ceiling: u64) -> Self {
        prob.reset();
        Self {
            at: 0,
            bar,
            carry: Vec::with_capacity(WINDOW),
            ceiling,
            held: None,
            next: None,
            prob,
            score,
            tail: Vec::with_capacity(CONTEXT),
            turn: None,
            voice: None,
        }
    }

    /// Rebuilds the turn the last engine held as it left, from `events`, the track's logged voices
    /// and turns in order: a voice on after the last turn starts the turn under way, and a voice
    /// off after it holds it from there, the ceiling closing it as usual.
    #[inline]
    pub fn resume<I: Iterator<Item = Value>>(&mut self, events: I) {
        for event in events {
            match (
                event["kind"].as_str(),
                &event["on"],
                event["start"].as_u64(),
            ) {
                (Some("turn"), _, _) => (self.turn, self.held) = (None, None),
                (Some("voice"), &Value::Bool(true), Some(start)) => {
                    let _: &mut u64 = self.turn.get_or_insert(start);
                    self.held = None;
                }
                (Some("voice"), &Value::Bool(false), Some(end)) if self.turn.is_some() => {
                    self.held = Some((end, end));
                }
                _ => {}
            }
        }
    }
}

/// One connection to `sock` as the turn engine of `track`: its frames from its kind's `through`
/// on the track (`"from":"through"`, live when none), so a fresh or reconnected engine resumes
/// in one connection, through `endpoint`, each report sent and a refused one said; ends with the
/// connection, an error.
#[cfg_attr(
    not(test),
    expect(
        clippy::single_call_fn,
        reason = "the session's own function; the tests call it as often"
    )
)]
pub fn session<P: Prob, S: Score>(
    sock: &Path,
    track: &str,
    mut endpoint: Endpoint<'_, P, S>,
) -> io::Result<()> {
    let request = json!({"from": "through", "kind": "turn", "name": "endpoint", "op": "engine", "track": track});
    let (mut conn, reply) = Conn::open(sock, CLIENT, &request)?;
    // the turn the last engine held, from the log's last events before this one's own
    let seq = reply["seq"].as_u64().unwrap_or_default();
    let after = json!({"boot": conn.hello()["boot"], "seq": seq.saturating_sub(LOOK)});
    let (mut feed, _) = Conn::open(sock, CLIENT, &json!({"after": after, "op": "feed"}))?;
    endpoint.resume(
        iter::from_fn(|| feed.line().ok())
            .take_while(|line| line["seq"].as_u64() < Some(seq))
            .map(|line| line["e"].clone())
            .filter(|event| event["track"] == track),
    );
    feed.shutdown();
    loop {
        let line = conn.line()?;
        // a report's reply: only a refusal is news
        if line["ok"].is_boolean() {
            if line["ok"] != true {
                eprintln!("endpoint: refused: {line}");
            }
            continue;
        }
        let pcm = samples(line["pcm"].as_str().unwrap_or_default());
        for report in endpoint.frame(line["t"].as_u64().unwrap_or_default(), &pcm)? {
            conn.send(&report)?;
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "a test asserts, and propagates its setup with ?"
)]
mod tests {
    use std::io;
    use std::iter::repeat_n;
    use std::thread;

    use link::fake::Fake;
    use link::hex;
    use serde_json::{Value, json};

    use super::{CONTEXT, Endpoint, Prob, Score, session};

    /// Speech wherever a window is loud: 1 over 0.5, 0.4 over 0.05, else 0.
    #[derive(Debug, Default)]
    struct Loud {
        /// Resets seen.
        resets: u32,
    }

    impl Prob for Loud {
        fn prob(&mut self, window: &[f32]) -> io::Result<f32> {
            let peak = window
                .iter()
                .fold(0.0f32, |held, sample| held.max(sample.abs()));
            Ok(if peak > 0.5f32 {
                1.0f32
            } else if peak > 0.05f32 {
                0.4f32
            } else {
                0.0f32
            })
        }

        fn reset(&mut self) {
            self.resets = self.resets.saturating_add(1);
        }
    }

    /// Scores in turn from a script, each tail's length kept.
    #[derive(Debug, Default)]
    struct Scripted {
        /// The scores to give, first first.
        scores: Vec<f32>,
        /// The tails' lengths seen.
        tails: Vec<usize>,
    }

    impl Score for Scripted {
        fn score(&mut self, tail: &[f32]) -> io::Result<f32> {
            self.tails.push(tail.len());
            Ok(if self.scores.is_empty() {
                1.0f32
            } else {
                self.scores.remove(0)
            })
        }
    }

    /// 20 ms frames from `at` µs, each `(level, frames)` a run of that sample value.
    fn frames(at: u64, runs: &[(f32, usize)]) -> Vec<(u64, Vec<f32>)> {
        runs.iter()
            .flat_map(|&(level, count)| repeat_n(level, count))
            .zip(0u64..)
            .map(|(level, step)| {
                (
                    at.saturating_add(step.saturating_mul(20_000)),
                    vec![level; 320],
                )
            })
            .collect()
    }

    /// Every report of `frames` through a fresh endpoint scoring from `scores`, ceiling 1 s.
    fn reports(scores: &[f32], frames: &[(u64, Vec<f32>)]) -> io::Result<(Vec<Value>, Scripted)> {
        let (mut prob, mut score) = (
            Loud::default(),
            Scripted {
                scores: scores.to_vec(),
                tails: Vec::new(),
            },
        );
        let mut endpoint = Endpoint::new(&mut prob, &mut score, 0.5f32, 1_000_000);
        let mut said = Vec::new();
        for frame in frames {
            said.extend(endpoint.frame(frame.0, &frame.1)?);
        }
        Ok((said, score))
    }

    /// A voice, its close after 256 ms under the offset, a score over the bar: one turn from its
    /// start to its last speech; a dip between offset and onset keeps it open.
    #[test]
    fn committed() -> io::Result<()> {
        // windows of 32 ms: speech covers [0, 640 ms) in frames, a dip, then silence
        let heard = frames(0, &[(1.0f32, 16), (0.1f32, 8), (0.0f32, 20)]);
        let (said, score) = reports(&[0.9f32], &heard)?;
        assert_eq!(
            said,
            [
                json!({"on": true, "op": "voice", "start": 0u64}),
                json!({"on": false, "op": "voice", "start": 480_000u64}),
                json!({"end": 480_000u64, "op": "turn", "start": 0u64}),
            ],
            "on, off, the turn"
        );
        assert_eq!(score.tails, [37 * 320], "scored at the close on all heard");
        Ok(())
    }

    /// A close under the bar holds the turn: a voice resuming joins it, and a hold the ceiling
    /// outlasts closes it at the last voice's end.
    #[test]
    fn held() -> io::Result<()> {
        let heard = frames(0, &[(1.0f32, 16), (0.0f32, 20), (1.0f32, 16), (0.0f32, 80)]);
        let (said, _) = reports(&[0.2f32, 0.2f32], &heard)?;
        let kinds: Vec<String> = said.iter().map(Value::to_string).collect();
        assert_eq!(
            kinds,
            [
                r#"{"on":true,"op":"voice","start":0}"#,
                r#"{"on":false,"op":"voice","start":320000}"#,
                r#"{"on":true,"op":"voice","start":704000}"#,
                r#"{"on":false,"op":"voice","start":1056000}"#,
                r#"{"end":1056000,"op":"turn","start":0}"#,
            ],
            "one turn over both voices, closed by the ceiling"
        );
        Ok(())
    }

    /// A hole past the close window closes the voice under way at the hole's start, the tail
    /// never more than the context; a loud frame alone between such holes, or a hole in
    /// silence, says nothing.
    #[test]
    fn long_holes() -> io::Result<()> {
        let mut heard = frames(0, &[(1.0f32, 460)]);
        for at in 0u64..20 {
            heard.extend(frames(
                at.saturating_mul(400_000).saturating_add(20_000_000),
                &[(1.0f32, 2)],
            ));
        }
        heard.extend(frames(30_000_000, &[(0.0f32, 20)]));
        heard.extend(frames(40_000_000, &[(0.0f32, 1)]));
        let (said, score) = reports(&[0.9f32], &heard)?;
        assert_eq!(
            said,
            [
                json!({"on": true, "op": "voice", "start": 0u64}),
                json!({"on": false, "op": "voice", "start": 9_200_000u64}),
                json!({"end": 9_200_000u64, "op": "turn", "start": 0u64}),
            ],
            "the voice closed at the hole, and nothing of the blips between holes"
        );
        assert_eq!(score.tails, [CONTEXT], "the last 8 s");
        Ok(())
    }

    /// A hole under the close window is heard as silence of its length: a voice runs on across
    /// it, one voice and one turn; such holes in silence say nothing.
    #[test]
    fn short_holes() -> io::Result<()> {
        let mut heard = frames(0, &[(0.0f32, 5)]);
        heard.extend(frames(200_000, &[(0.0f32, 5)]));
        heard.extend(frames(400_000, &[(1.0f32, 10)]));
        heard.extend(frames(700_000, &[(1.0f32, 10), (0.0f32, 20)]));
        let (said, _) = reports(&[0.9f32], &heard)?;
        assert_eq!(
            said,
            [
                json!({"on": true, "op": "voice", "start": 384_000u64}),
                json!({"on": false, "op": "voice", "start": 928_000u64}),
                json!({"end": 928_000u64, "op": "turn", "start": 384_000u64}),
            ],
            "one voice across the hole"
        );
        Ok(())
    }

    /// A blip shorter than a voice, one window loud, is no voice.
    #[test]
    fn blips() -> io::Result<()> {
        let heard = frames(0, &[(0.0f32, 10), (1.0f32, 1), (0.0f32, 30)]);
        assert_eq!(reports(&[], &heard)?.0, Vec::<Value>::new(), "nothing said");
        Ok(())
    }

    /// A session: `"from":"through"` each time, frames read and reports sent, a refusal said,
    /// the end an error.
    #[test]
    fn sessions() -> io::Result<()> {
        let fake = Fake::new("endpoint-sessions")?;
        let sock = fake.sock.clone();
        let engine = thread::spawn(move || {
            let (mut prob, mut score) = (Loud::default(), Scripted::default());
            let mut ends = Vec::new();
            for _ in 0u8..2 {
                let endpoint = Endpoint::new(&mut prob, &mut score, 0.5f32, 1_000_000);
                let ended = session(&sock, "ear-desk", endpoint);
                ends.push(
                    ended
                        .err()
                        .map(|cause| cause.to_string())
                        .unwrap_or_default(),
                );
            }
            ends
        });
        let (mut conn, request) = fake.accept()?;
        assert_eq!(
            request,
            json!({"from": "through", "kind": "turn", "name": "endpoint", "op": "engine", "track": "ear-desk"}),
            "resumed by the daemon"
        );
        conn.send(&json!({"ok": true, "seq": 1u8, "through": 7u8}))?;
        // nothing logged before its own engine event: nothing held
        let (mut feed, asked) = fake.accept()?;
        assert_eq!(
            asked,
            json!({"after": {"boot": 1u8, "seq": 0u8}, "op": "feed"}),
            "the log read back"
        );
        feed.send(&json!({"ok": true}))?;
        feed.send(&json!({"e": {"kind": "engine", "track": "ear-desk"}, "seq": 1u8}))?;
        let mut lines = Vec::new();
        for (time, pcm) in frames(1_000_000, &[(1.0f32, 16), (0.0f32, 13)]) {
            conn.send(&json!({"pcm": hex(&pcm), "t": time}))?;
        }
        for reply in [
            json!({"ok": true, "seq": 2u8}),
            json!({"code": "bad_request", "ok": false}),
        ] {
            lines.push(conn.line()?);
            conn.send(&reply)?;
        }
        lines.push(conn.line()?);
        drop(conn);
        let (mut again, second) = fake.accept()?;
        assert_eq!(second["from"], "through", "resumed by the daemon again");
        again.send(&json!({"code": "taken", "ok": false}))?;
        assert_eq!(lines.len(), 3, "voice on, off, the turn");
        assert_eq!(
            engine.join().unwrap_or_default(),
            ["the connection ended", r#"{"code":"taken","ok":false}"#],
            "each session's end"
        );
        Ok(())
    }

    /// A reconnect while a turn was held: the log's last voices rebuild it, and the ceiling of
    /// silence after it closes it from its first voice's start to its last voice's end; another
    /// track's voices and an earlier turn change nothing.
    #[test]
    fn resumed() -> io::Result<()> {
        let fake = Fake::new("endpoint-resumed")?;
        let sock = fake.sock.clone();
        let engine = thread::spawn(move || {
            let (mut prob, mut score) = (Loud::default(), Scripted::default());
            let endpoint = Endpoint::new(&mut prob, &mut score, 0.5f32, 1_000_000);
            session(&sock, "ear-desk", endpoint)
                .err()
                .map(|cause| cause.to_string())
        });
        let (mut conn, _) = fake.accept()?;
        conn.send(&json!({"ok": true, "seq": 300u16, "through": 1_500_000u32}))?;
        let (mut feed, asked) = fake.accept()?;
        assert_eq!(
            asked["after"],
            json!({"boot": 1u8, "seq": 44u8}),
            "256 events back"
        );
        feed.send(&json!({"ok": true}))?;
        let event = |seq: u16, event: Value| json!({"e": event, "seq": seq});
        for line in [
            event(
                294,
                json!({"end": 100_000u32, "kind": "words", "start": 0u8, "track": "ear-desk"}),
            ),
            event(
                295,
                json!({"kind": "voice", "on": true, "start": 100_000u32, "track": "ear-desk"}),
            ),
            event(
                296,
                json!({"end": 300_000u32, "kind": "turn", "start": 100_000u32, "track": "ear-desk"}),
            ),
            event(
                297,
                json!({"kind": "voice", "on": true, "start": 1_000_000u32, "track": "ear-desk"}),
            ),
            event(
                298,
                json!({"kind": "voice", "on": true, "start": 1_200_000u32, "track": "ear-hall"}),
            ),
            event(
                299,
                json!({"kind": "voice", "on": false, "start": 1_500_000u32, "track": "ear-desk"}),
            ),
            event(
                300,
                json!({"kind": "engine", "on": true, "track": "ear-desk"}),
            ),
        ] {
            feed.send(&line)?;
        }
        for (time, pcm) in frames(1_500_000, &[(0.0f32, 60)]) {
            conn.send(&json!({"pcm": hex(&pcm), "t": time}))?;
        }
        assert_eq!(
            conn.line()?,
            json!({"end": 1_500_000u32, "op": "turn", "start": 1_000_000u32}),
            "the held turn closed by the ceiling"
        );
        drop((conn, feed));
        assert_eq!(
            engine.join().ok().flatten().as_deref(),
            Some("the connection ended"),
            "its end"
        );
        Ok(())
    }
}

//! The endpoint's work, its models aside: a speech probability per 32 ms window segmented into
//! voice, a score of the turn so far at each voice's close deciding the turn, and one
//! connection's frames in and reports out (docs/protocol.html#engines).

use std::collections::VecDeque;
use std::io;
use std::path::Path;

use link::{Conn, RATE, samples};
use serde_json::{Value, json};

/// Samples per window at 16 kHz, Silero's fixed 32 ms.
pub const WINDOW: usize = 512;
/// Smart Turn's context: the turn's last 8 s.
pub const CONTEXT: usize = 128_000;
/// A window's span in µs.
const SPAN: u64 = 32_000;
/// A window at or over this opens a voice (Silero's own onset).
const ONSET: f32 = 0.5;
/// Windows under this count toward a close: Silero's hysteresis, its onset less 0.15.
const OFFSET: f32 = 0.35;
/// Windows under [`OFFSET`] in a row that close a voice: 8 × 32 ms = 256 ms.
const CLOSE: u32 = 8;

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
}

/// One stream's segmenter and verdicts, every time on the daemon's clock in µs.
#[derive(Debug)]
pub struct Endpoint<'m, P, S> {
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
    prob: &'m mut P,
    /// The turn model.
    score: &'m mut S,
    /// The audio since the last turn, its last [`CONTEXT`] samples.
    tail: Vec<f32>,
    /// The start of the turn under way.
    turn: Option<u64>,
    /// The voice under way.
    voice: Option<Voice>,
}

impl<'m, P: Prob, S: Score> Endpoint<'m, P, S> {
    /// A fresh stream: `bar` the score that commits a turn, `ceiling` µs of silence that close
    /// one held under it.
    pub fn new(prob: &'m mut P, score: &'m mut S, bar: f32, ceiling: u64) -> Self {
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

    /// The reports the frame at `t` holding `pcm` completes, in order: a jump in `t` is a hole,
    /// which closes the voice under way where it was last heard.
    pub fn frame(&mut self, t: u64, pcm: &[f32]) -> io::Result<Vec<Value>> {
        let mut said = Vec::new();
        if self.next.is_some_and(|due| due != t) {
            self.close(&mut said)?;
            self.prob.reset();
            self.carry.clear();
        }
        if self.carry.is_empty() {
            self.at = t;
        }
        self.next = Some(t + pcm.len() as u64 * 1_000_000 / RATE);
        self.carry.extend_from_slice(pcm);
        self.tail.extend_from_slice(pcm);
        drop(self.tail.drain(..self.tail.len().saturating_sub(CONTEXT)));
        while self.carry.len() >= WINDOW {
            let speech = self.prob.prob(&self.carry[..WINDOW])?;
            drop(self.carry.drain(..WINDOW));
            let start = self.at;
            self.at += SPAN;
            match self.voice.as_mut() {
                None if speech >= ONSET => {
                    said.push(json!({"on": true, "op": "voice", "start": start}));
                    self.voice = Some(Voice {
                        last: self.at,
                        quiet: 0,
                    });
                    let _: &mut u64 = self.turn.get_or_insert(start);
                    self.held = None;
                }
                Some(voice) if speech >= OFFSET => {
                    *voice = Voice {
                        last: self.at,
                        quiet: 0,
                    }
                }
                Some(voice) if voice.quiet + 1 < CLOSE => voice.quiet += 1,
                Some(_) => self.close(&mut said)?,
                None => {}
            }
            if let Some((end, since)) = self.held
                && self.at - since >= self.ceiling
            {
                self.commit(end, &mut said);
            }
        }
        Ok(said)
    }

    /// Closes the voice under way, if any: its end reported, the turn scored, committed over
    /// the bar, else held.
    fn close(&mut self, said: &mut Vec<Value>) -> io::Result<()> {
        if let Some(voice) = self.voice.take() {
            said.push(json!({"on": false, "op": "voice", "start": voice.last}));
            if self.score.score(&self.tail)? > self.bar {
                self.commit(voice.last, said);
            } else {
                self.held = Some((voice.last, self.at));
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
}

/// One connection to `sock` as the turn engine of `track`: its frames from `through` (live when
/// none) through `endpoint`, each report sent, each the daemon logs moving `through`; ends with
/// the connection, an error. A `from` gone from the window resumes live next time.
pub fn session<P: Prob, S: Score>(
    sock: &Path,
    track: &str,
    through: &mut Option<u64>,
    mut endpoint: Endpoint<'_, P, S>,
) -> io::Result<()> {
    let request = json!({"from": *through, "kind": "turn", "name": "endpoint", "op": "engine", "track": track});
    let (mut conn, reply) = Conn::open(sock, "endpoint/0.1.0", &request).inspect_err(|cause| {
        if cause.to_string().contains(r#""gone""#) {
            *through = None;
        }
    })?;
    *through = reply["through"].as_u64();
    let mut sent = VecDeque::new();
    loop {
        let line = conn.line()?;
        // a report's reply, in the order the reports went
        if line["ok"].is_boolean() {
            let time = sent.pop_front();
            if line["ok"] == true {
                *through = (*through).max(time);
            } else {
                eprintln!("endpoint: refused: {line}");
            }
            continue;
        }
        let pcm = samples(line["pcm"].as_str().unwrap_or_default());
        for report in endpoint.frame(line["t"].as_u64().unwrap_or_default(), &pcm)? {
            sent.push_back(
                report["end"]
                    .as_u64()
                    .or_else(|| report["start"].as_u64())
                    .unwrap_or_default(),
            );
            conn.send(&report)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
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
                .fold(0.0_f32, |held, sample| held.max(sample.abs()));
            Ok(if peak > 0.5 {
                1.0
            } else if peak > 0.05 {
                0.4
            } else {
                0.0
            })
        }

        fn reset(&mut self) {
            self.resets += 1;
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
                1.0
            } else {
                self.scores.remove(0)
            })
        }
    }

    /// 20 ms frames from `at` µs, each `(level, frames)` a run of that sample value.
    fn frames(at: u64, runs: &[(f32, u64)]) -> Vec<(u64, Vec<f32>)> {
        let mut t = at;
        runs.iter()
            .flat_map(|&(level, count)| (0..count).map(move |_| level))
            .map(|level| {
                t += 20_000;
                (t - 20_000, vec![level; 320])
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
        let mut endpoint = Endpoint::new(&mut prob, &mut score, 0.5, 1_000_000);
        let mut said = Vec::new();
        for (t, pcm) in frames {
            said.extend(endpoint.frame(*t, pcm)?);
        }
        Ok((said, score))
    }

    /// A voice, its close after 256 ms under the offset, a score over the bar: one turn from its
    /// start to its last speech; a dip between offset and onset keeps it open.
    #[test]
    fn committed() -> io::Result<()> {
        // windows of 32 ms: speech covers [0, 640 ms) in frames, a dip, then silence
        let (said, score) = reports(&[0.9], &frames(0, &[(1.0, 16), (0.1, 8), (0.0, 20)]))?;
        assert_eq!(
            said,
            [
                json!({"on": true, "op": "voice", "start": 0}),
                json!({"on": false, "op": "voice", "start": 480_000}),
                json!({"end": 480_000, "op": "turn", "start": 0}),
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
        let heard = frames(0, &[(1.0, 16), (0.0, 20), (1.0, 16), (0.0, 80)]);
        let (said, _) = reports(&[0.2, 0.2], &heard)?;
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

    /// A hole closes the voice under way where it was last heard and resets the model; the
    /// tail never holds more than the context.
    #[test]
    fn holes() -> io::Result<()> {
        let mut heard = frames(0, &[(1.0, 460)]);
        heard.extend(frames(20_000_000, &[(0.0, 2)]));
        // a hole in silence closes nothing
        heard.extend(frames(30_000_000, &[(0.0, 1)]));
        let (said, score) = reports(&[0.9], &heard)?;
        assert_eq!(
            said.last(),
            Some(&json!({"end": 9_184_000, "op": "turn", "start": 0})),
            "closed at the last window before the hole"
        );
        assert_eq!(score.tails, [CONTEXT], "the last 8 s");
        Ok(())
    }

    /// A session: `from` its through, frames read and reports sent, each one logged moving
    /// `through`, a refusal said and not moving it, the end an error; a `gone` from resumes live.
    #[test]
    fn sessions() -> io::Result<()> {
        let fake = Fake::new("endpoint-sessions")?;
        let sock = fake.sock.clone();
        let engine = thread::spawn(move || {
            let (mut prob, mut score) = (Loud::default(), Scripted::default());
            let mut through = Some(5);
            let mut ends = Vec::new();
            for _ in 0..3 {
                let endpoint = Endpoint::new(&mut prob, &mut score, 0.5, 1_000_000);
                let ended = session(&sock, "ear-desk", &mut through, endpoint);
                ends.push(format!(
                    "{through:?} {}",
                    ended
                        .err()
                        .map(|cause| cause.to_string())
                        .unwrap_or_default()
                ));
            }
            ends
        });
        let (mut conn, request) = fake.accept()?;
        assert_eq!(
            request,
            json!({"from": 5, "kind": "turn", "name": "endpoint", "op": "engine", "track": "ear-desk"}),
            "from the through it held"
        );
        conn.send(&json!({"ok": true, "seq": 1, "through": 7}))?;
        let mut lines = Vec::new();
        for (t, pcm) in frames(1_000_000, &[(1.0, 16), (0.0, 13)]) {
            conn.send(&json!({"pcm": hex(&pcm), "t": t}))?;
        }
        for reply in [
            json!({"ok": true, "seq": 2}),
            json!({"code": "bad_request", "ok": false}),
        ] {
            lines.push(conn.line()?);
            conn.send(&reply)?;
        }
        lines.push(conn.line()?);
        drop(conn);
        let (mut conn, request) = fake.accept()?;
        assert_eq!(
            request["from"], 1_000_000,
            "the voice logged; the voice's end refused"
        );
        conn.send(&json!({"code": "gone", "ok": false}))?;
        drop(conn);
        let (_, request) = fake.accept()?;
        assert_eq!(request["from"], Value::Null, "live after gone");
        assert_eq!(lines.len(), 3, "voice on, off, the turn");
        assert_eq!(
            engine.join().unwrap_or_default(),
            [
                "Some(1000000) the connection ended",
                r#"None {"code":"gone","ok":false}"#,
                "None the connection ended",
            ],
            "each session's end"
        );
        Ok(())
    }
}

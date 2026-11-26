//! The endpoint's work, its models aside: a speech probability per 32 ms window segmented into
//! voice, a score of the turn so far at each voice's close deciding the turn, and one
//! connection's frames in and reports out (docs/protocol.html#engines).

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

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
/// How far inside the window's edge a connect reads from, µs: clear of the sweep's next cut.
const MARGIN: u64 = 2_000_000;
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
    /// A voice was under way at the first time heard: the turn it starts began before the
    /// oldest audio held, and is dropped rather than reported short.
    clipped: bool,
    /// The first time heard: where a replay began.
    first: Option<u64>,
    /// The reply's `through`, none with nothing logged: a voice it covers is logged, and its
    /// reports are dropped.
    floor: Option<u64>,
    /// A turn held under the bar: its end and when it was held.
    held: Option<(u64, u64)>,
    /// Where the next frame is due: the end of the last, none before the first.
    next: Option<u64>,
    /// The speech model.
    prob: &'model mut P,
    /// A voice dropped as logged is under way, so its off is dropped too.
    quiet: bool,
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
        Ok(said
            .into_iter()
            .filter(|report| self.fresh(report))
            .collect())
    }

    /// Whether `report` is news: a voice the floor covers (its on and its off) is logged
    /// already; a turn begun with a voice under way at the oldest audio held would be short,
    /// so it is dropped and said once.
    fn fresh(&mut self, report: &Value) -> bool {
        let start = report["start"].as_u64().unwrap_or_default();
        match (report["op"].as_str(), &report["on"]) {
            (Some("voice"), &Value::Bool(true)) if self.floor.is_some_and(|at| start <= at) => {
                self.quiet = true;
                false
            }
            (Some("voice"), &Value::Bool(false)) if self.quiet => {
                self.quiet = false;
                false
            }
            (Some("turn"), _) if self.clipped && self.first == Some(start) => {
                eprintln!("endpoint: a turn begun before the oldest audio held is dropped");
                self.clipped = false;
                false
            }
            _ => true,
        }
    }

    /// Hears `pcm` from `time` on, window by window.
    fn hear(&mut self, time: u64, pcm: &[f32], said: &mut Vec<Value>) -> io::Result<()> {
        if self.carry.is_empty() {
            self.at = time;
        }
        let _: &mut u64 = self.first.get_or_insert(time);
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
                    // a replay's oldest audio, a turn's middle when a voice is under way there
                    self.clipped |= self.floor.is_some() && self.first == Some(start);
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
            clipped: false,
            first: None,
            floor: None,
            held: None,
            next: None,
            prob,
            quiet: false,
            score,
            tail: Vec::with_capacity(CONTEXT),
            turn: None,
            voice: None,
        }
    }
}

/// One connection to `sock` as the turn engine of `track`: its frames from the oldest audio the
/// window holds, any boot (`"from":"through"` when that edge is gone meanwhile), heard again so
/// a turn held as the last engine left is rebuilt whole, what the reply's `through` covers kept
/// out of its reports, each report sent with its time as its `id` and a refusal of a new one
/// said; ends with the connection, an error.
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
    // from the oldest audio the window holds, any boot, so a turn held as the last engine left is
    // heard again whole; this machine's clock is the daemon's
    let (_, health) = Conn::open(sock, CLIENT, &json!({"op": "health"}))?;
    let window = health["window"]
        .as_u64()
        .unwrap_or_default()
        .saturating_mul(1_000_000);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let back = (now as u64).saturating_sub(window).saturating_add(MARGIN);
    let request =
        json!({"from": back, "kind": "turn", "name": "endpoint", "op": "engine", "track": track});
    let (mut conn, reply) = match Conn::open(sock, CLIENT, &request) {
        // the window's edge swept meanwhile: from `through`, and a held turn is not rebuilt
        Err(cause) if cause.to_string().contains(r#""gone""#) => {
            eprintln!("endpoint: resumed from through; a turn held before it is not rebuilt");
            Conn::open(
                sock,
                CLIENT,
                &json!({"from": "through", "kind": "turn", "name": "endpoint", "op": "engine", "track": track}),
            )?
        }
        other => other?,
    };
    endpoint.floor = reply["through"].as_u64();
    loop {
        let line = conn.line()?;
        // a report's reply: a refusal of one the floor covered is a logged one heard again
        if line["ok"].is_boolean() {
            let heard = line["id"]
                .as_u64()
                .zip(endpoint.floor)
                .is_some_and(|(id, at)| id <= at);
            if line["ok"] != true && !heard {
                eprintln!("endpoint: refused: {line}");
            }
            continue;
        }
        let pcm = samples(line["pcm"].as_str().unwrap_or_default());
        for mut report in endpoint.frame(line["t"].as_u64().unwrap_or_default(), &pcm)? {
            report["id"] = report["end"]
                .as_u64()
                .or_else(|| report["start"].as_u64())
                .into();
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

    /// A voice under way at the oldest audio heard: its voice reported, its turn, which began
    /// before, dropped rather than reported short; the next voice's turn reported whole.
    #[test]
    fn clipped() -> io::Result<()> {
        let runs = [(1.0f32, 25), (0.0, 25), (1.0, 25), (0.0, 25)];
        let (mut prob, mut score) = (Loud::default(), Scripted::default());
        let mut endpoint = Endpoint::new(&mut prob, &mut score, 0.5f32, 1_000_000);
        // resumed: something was logged before the oldest audio replayed
        endpoint.floor = Some(0);
        let mut said = Vec::new();
        for (time, pcm) in frames(5_000_000, &runs) {
            said.extend(endpoint.frame(time, &pcm)?);
        }
        assert_eq!(
            said,
            [
                json!({"on": true, "op": "voice", "start": 5_000_000u32}),
                json!({"on": false, "op": "voice", "start": 5_512_000u32}),
                json!({"on": true, "op": "voice", "start": 5_992_000u32}),
                json!({"on": false, "op": "voice", "start": 6_504_000u32}),
                json!({"end": 6_504_000u32, "op": "turn", "start": 5_992_000u32}),
            ],
            "the first turn dropped, the second whole"
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

    /// A session: from the window's oldest audio after a health, frames read and reports sent
    /// with their times as ids, a refusal said, the end an error; the window's edge gone, from
    /// `through`.
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
        let (mut asked, health) = fake.accept()?;
        assert_eq!(health, json!({"op": "health"}), "the window asked first");
        asked.send(&json!({"ok": true, "window": 600u16}))?;
        let (mut conn, request) = fake.accept()?;
        assert!(
            request["from"].as_u64() > Some(1_700_000_000_000_000),
            "a time 600 s back"
        );
        conn.send(&json!({"ok": true, "seq": 1u8, "through": 7u8}))?;
        let mut lines = Vec::new();
        for (time, pcm) in frames(900_000, &[(0.0f32, 5), (1.0, 16), (0.0, 20)]) {
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
        let (mut windowed, _) = fake.accept()?;
        windowed.send(&json!({"ok": true, "window": 600u16}))?;
        let (mut gone, _) = fake.accept()?;
        gone.send(&json!({"code": "gone", "ok": false}))?;
        let (mut again, second) = fake.accept()?;
        assert_eq!(second["from"], "through", "from through, the edge gone");
        again.send(&json!({"code": "taken", "ok": false}))?;
        let ids: Vec<&Value> = lines.iter().map(|report| &report["id"]).collect();
        assert_eq!(
            ids,
            [
                &json!(996_000u32),
                &json!(1_348_000u32),
                &json!(1_348_000u32)
            ],
            "voice on, off, the turn"
        );
        assert_eq!(
            engine.join().unwrap_or_default(),
            ["the connection ended", r#"{"code":"taken","ok":false}"#],
            "each session's end"
        );
        Ok(())
    }

    /// A reconnect while a turn was held, however many events were logged since: the audio from
    /// the window's oldest heard again rebuilds it whole, the voices `through` covers kept out,
    /// and the ceiling of silence after it closes it; a logged turn heard again is sent for the
    /// daemon to refuse, that refusal unsaid.
    #[test]
    fn resumed() -> io::Result<()> {
        let fake = Fake::new("endpoint-resumed")?;
        let sock = fake.sock.clone();
        let engine = thread::spawn(move || {
            let mut prob = Loud::default();
            let mut score = Scripted {
                scores: vec![0.9f32, 0.2],
                tails: Vec::new(),
            };
            let endpoint = Endpoint::new(&mut prob, &mut score, 0.5f32, 1_000_000);
            session(&sock, "ear-desk", endpoint)
                .err()
                .map(|cause| cause.to_string())
        });
        let (mut asked, _) = fake.accept()?;
        asked.send(&json!({"ok": true, "window": 600u16}))?;
        let (mut conn, _) = fake.accept()?;
        conn.send(&json!({"ok": true, "seq": 300u16, "through": 1_728_000u32}))?;
        // replayed from long before: a logged turn, quiet, then the held turn's voice
        let runs = [(0.0f32, 10), (1.0, 25), (0.0, 25), (1.0, 25), (0.0, 75)];
        for (time, pcm) in frames(0, &runs) {
            conn.send(&json!({"pcm": hex(&pcm), "t": time}))?;
        }
        assert_eq!(
            conn.line()?,
            json!({"end": 704_000u32, "id": 704_000u32, "op": "turn", "start": 192_000u32}),
            "the logged turn heard again, its voices dropped, the daemon to refuse it"
        );
        conn.send(&json!({"code": "bad_request", "id": 704_000u32, "ok": false}))?;
        assert_eq!(
            conn.line()?,
            json!({"end": 1_728_000u32, "id": 1_728_000u32, "op": "turn", "start": 1_184_000u32}),
            "the held turn rebuilt whole, its voices logged already, closed by the ceiling"
        );
        drop(conn);
        assert_eq!(
            engine.join().ok().flatten().as_deref(),
            Some("the connection ended"),
            "its end"
        );
        Ok(())
    }
}

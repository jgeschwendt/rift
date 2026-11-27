//! The speaking, its model aside (docs/protocol.html#engines).
//!
//! A say's text is spoken as it comes in spans of whole sentences, each as long as the model
//! speaks in one piece, so commas, colons and the joins between sentences keep the voice's own
//! prosody; each span's audio given through the say's play as it is made, a `mark` once a span
//! is given, the play ended after the say's end (`done` when nothing sounded), a hush stopping
//! it mid-span.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;

use link::{Conn, hex};
use serde_json::{Value, json};

use crate::pocket::PIECE;

/// The hello's client.
const CLIENT: &str = "read-aloud/0.1.0";
/// Samples in a give at most: 16384 bytes of 16-bit PCM.
const GIVE: usize = 8192;
/// Bytes in a span of several sentences at most: Pocket's piece in tokens at 3.25 bytes each,
/// the fewest its tokenizer gave three texts (3.26 to 3.72, measured 2026-11-27), so a span is
/// one piece and its mark lands where the model would start afresh.
const SPAN: usize = PIECE.saturating_mul(13).div_euclid(4);

/// A text-to-speech model.
pub trait Voice {
    /// The rate it speaks at, given to the mouth as it is.
    fn rate(&self) -> u32;
    /// Speaks `text`, handing `give` each chunk of audio as it is made, until `give` says stop.
    fn speak(
        &mut self,
        text: &str,
        give: &mut dyn FnMut(&[f32]) -> io::Result<bool>,
    ) -> io::Result<()>;
}

/// What the reader tells the speaking as it reads, ahead of the lines it queues.
#[derive(Debug, Default)]
struct Heard {
    /// The highest say a hush has named.
    hushed: AtomicU64,
    /// The say whose play is open, and a handle on that play to shut it at its hush, so a give
    /// waiting on its reply ends at once.
    playing: Mutex<Option<(u64, Conn)>>,
}

impl Heard {
    /// A hush of `num`: said, and its play, if open, shut.
    fn hush(&self, num: u64) {
        let _prior = self.hushed.fetch_max(num, Ordering::Relaxed);
        if let Ok(held) = self.playing.lock()
            && let Some(play) = (held.as_ref())
                .filter(|playing| playing.0 <= num)
                .map(|playing| &playing.1)
        {
            play.shutdown();
        }
    }
}

/// A say under way.
#[derive(Debug, Default)]
struct Say {
    /// Samples its play has been given.
    given: u64,
    /// The samples its last mark named: marks rise strictly, so a span that gave no audio marks
    /// nothing.
    marked: u64,
    /// Its number.
    num: u64,
    /// Hushed, refused or ended: nothing more is spoken.
    over: bool,
    /// Its play's connection, opened at its first audio.
    play: Option<Conn>,
    /// Bytes of `text` spoken so far.
    spoken: usize,
    /// All the text it was given.
    text: String,
}

impl Say {
    /// Gives `chunk` to the play, opening it first; whether to go on.
    fn give(&mut self, chunk: &[f32], heard: &Heard, request: &Value, sock: &Path) -> bool {
        self.over |= heard.hushed.load(Ordering::Relaxed) >= self.num;
        if self.play.is_none() && !self.over {
            match Conn::open(sock, CLIENT, request).and_then(|(play, _)| Ok((play.twin()?, play))) {
                Ok((handle, play)) => {
                    if let Ok(mut held) = heard.playing.lock() {
                        *held = Some((self.num, handle));
                    }
                    self.play = Some(play);
                }
                Err(cause) => {
                    eprintln!("read-aloud: say {}: {cause}", self.num);
                    self.over = true;
                }
            }
        }
        for piece in chunk.chunks(GIVE) {
            let Some(play) = self.play.as_mut().filter(|_| !self.over) else {
                break;
            };
            let give = json!({"op": "give", "pcm": hex(piece)});
            // a give refused or answered by the play's last line: hushed, or ended under it
            if play
                .send(&give)
                .and_then(|()| play.line())
                .is_ok_and(|reply| reply["ok"] == true)
            {
                self.given = self.given.saturating_add(piece.len() as u64);
            } else {
                self.over = true;
            }
        }
        !self.over
    }
}

/// One engine connection's speaking.
struct Speaker<'run> {
    /// What the reader saw ahead: the hushes.
    heard: &'run Heard,
    /// The say under way.
    say: Option<Say>,
    /// The daemon's socket, for the plays.
    sock: &'run Path,
    /// The mouth.
    track: &'run str,
    /// The model.
    voice: &'run mut dyn Voice,
    /// The engine connection's write side, for marks and `done`.
    write: Conn,
}

impl Speaker<'_> {
    /// Takes one line the daemon sent: a report's reply, a say's text, its end or a hush; any
    /// other, the protocol grown since, ignored.
    fn line(&mut self, line: &Value) -> io::Result<()> {
        if line["ok"] == false {
            eprintln!("read-aloud: refused: {line}");
        }
        let Some(num) = ["end", "hush", "say"]
            .iter()
            .find_map(|key| line[key].as_u64())
        else {
            return Ok(());
        };
        let mut say = self.say.take().filter(|say| say.num == num).unwrap_or(Say {
            num,
            ..Say::default()
        });
        if line["hush"].is_u64() {
            return Ok(());
        }
        say.text.push_str(line["text"].as_str().unwrap_or_default());
        let all = line["end"].is_u64();
        self.speak(&mut say, all)?;
        match (all, say.play.as_mut()) {
            (false, _) => self.say = Some(say),
            // a play gone under it reads as ended
            (true, Some(play)) => {
                drop(play.send(&json!({"op": "end"})));
                while play.line().is_ok_and(|last| last["reason"].is_null()) {}
            }
            (true, None) => self.write.send(&json!({"op": "done", "say": num}))?,
        }
        Ok(())
    }

    /// Speaks each whole span `say` holds unspoken, with `all` the rest too, marking each.
    fn speak(&mut self, say: &mut Say, all: bool) -> io::Result<()> {
        let request =
            json!({"op": "play", "rate": self.voice.rate(), "say": say.num, "track": self.track});
        while let Some(at) =
            cut(say.text.get(say.spoken..).unwrap_or_default(), all).filter(|_| !say.over)
        {
            let end = say.spoken.saturating_add(at);
            let span = (say.text.get(say.spoken..end).unwrap_or_default().trim()).to_owned();
            say.spoken = end;
            if span.is_empty() {
                continue;
            }
            let (heard, sock) = (self.heard, self.sock);
            let mut give = |chunk: &[f32]| Ok(say.give(chunk, heard, &request, sock));
            self.voice.speak(&span, &mut give)?;
            if !say.over && say.given > say.marked {
                say.marked = say.given;
                let text = say
                    .text
                    .get(..say.spoken)
                    .unwrap_or_default()
                    .chars()
                    .count();
                let mark =
                    json!({"op": "mark", "samples": say.given, "say": say.num, "text": text});
                self.write.send(&mark)?;
            }
        }
        Ok(())
    }
}

/// Where the next span of `rest` ends: just past its last sentence end that a space follows
/// within `SPAN` bytes, else past its first (a longer sentence is one span); with `all`, the rest
/// when it fits or has no end.
#[cfg_attr(
    not(test),
    expect(
        clippy::single_call_fn,
        reason = "the sentence cut, named for what it decides; the tests call it as often"
    )
)]
fn cut(rest: &str, all: bool) -> Option<usize> {
    let ends: Vec<usize> = rest
        .char_indices()
        .zip(rest.chars().skip(1))
        .filter(|&((_, mark), next)| ".!?".contains(mark) && next.is_whitespace())
        .map(|((at, _), _)| at.saturating_add(1))
        .collect();
    let within = (ends.iter().copied())
        .take_while(|&end| end <= SPAN)
        .last()
        .or_else(|| ends.first().copied());
    let whole = (all && !rest.is_empty()).then_some(rest.len());
    whole.filter(|&len| len <= SPAN).or(within).or(whole)
}

/// Hands `take` each line `queued` holds in order, a say's texts already waiting back to back
/// joined into one, its `end` too when that follows, so a span holds all that has come.
#[cfg_attr(
    not(test),
    expect(
        clippy::single_call_fn,
        reason = "the join, named for what it decides; a test calls it on a queue of its own"
    )
)]
fn hear(
    queued: &Receiver<Value>,
    take: &mut dyn FnMut(&Value) -> io::Result<()>,
) -> io::Result<()> {
    let mut held = queued.recv().ok();
    while let Some(mut line) = held.take() {
        held = queued.try_recv().ok();
        while let Some(more) = held.as_ref().filter(|more| {
            line["say"].is_u64()
                && line["end"].is_null()
                && (more["say"] == line["say"] || more["end"] == line["say"])
        }) {
            let joined = [&line, more]
                .map(|part| part["text"].as_str().unwrap_or_default())
                .concat();
            if let (Some(into), Some(from)) = (line.as_object_mut(), more.as_object()) {
                into.extend(from.clone());
            }
            line["text"] = joined.into();
            held = queued.try_recv().ok();
        }
        take(&line)?;
        held = held.or_else(|| queued.recv().ok());
    }
    Ok(())
}

/// One connection to `sock` as the speech engine of the mouth `track`, speaking through
/// `voice`; ends with the connection, an error.
#[inline]
pub fn session(sock: &Path, track: &str, voice: &mut dyn Voice) -> io::Result<()> {
    let request = json!({"kind": "speech", "name": "read-aloud", "op": "engine", "track": track});
    let (mut conn, _) = Conn::open(sock, CLIENT, &request)?;
    let write = conn.twin()?;
    let heard = Arc::new(Heard::default());
    // the daemon's 8 unwritten pieces ahead at most, so its pacing of a client's text holds and
    // what came while a span was spoken is there to join into the next
    let (lines, queued) = mpsc::sync_channel(8);
    let hearing = Arc::clone(&heard);
    let reader = thread::spawn(move || -> io::Result<()> {
        loop {
            let line = conn.line()?;
            // a hush is seen as it is read, so a span being made stops at its next chunk
            hearing.hush(line["hush"].as_u64().unwrap_or_default());
            lines.send(line).map_err(io::Error::other)?;
        }
    });
    let mut speaker = Speaker {
        heard: &heard,
        say: None,
        sock,
        track,
        voice,
        write,
    };
    let worked = hear(&queued, &mut |line| speaker.line(line));
    speaker.write.shutdown();
    worked?;
    reader.join().unwrap_or(Ok(()))
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "a test asserts, and propagates its setup with ?"
)]
mod tests {
    use std::sync::mpsc;
    use std::{io, thread};

    use link::Conn;
    use link::fake::Fake;
    use serde_json::{Value, json};

    use super::{GIVE, SPAN, Voice, cut, hear, session};

    /// A chunk of 1000 samples a byte of text, then one of 10.
    #[derive(Debug)]
    struct Hum;

    impl Voice for Hum {
        fn rate(&self) -> u32 {
            24_000
        }

        fn speak(
            &mut self,
            text: &str,
            give: &mut dyn FnMut(&[f32]) -> io::Result<bool>,
        ) -> io::Result<()> {
            // a span of a dash alone makes no sound
            for len in [text.len().saturating_mul(1000), 10]
                .into_iter()
                .filter(|_| text != "-.")
            {
                if !give(&vec![0.25f32; len])? {
                    break;
                }
            }
            Ok(())
        }
    }

    /// A span ends past the last sentence end a space follows within `SPAN` bytes, never at a
    /// comma, colon or semicolon, a longer sentence whole; the rest only with the say's end.
    #[test]
    fn sentences() {
        assert_eq!(
            ["Hi, you: yes; no. Ok? Go", "Wow! ", "e.g.x", "Wait, "]
                .map(|text| (cut(text, false), cut(text, true))),
            [
                (Some(21), Some(24)),
                (Some(4), Some(5)),
                (None, Some(5)),
                (None, Some(6))
            ],
            "the last end, a bang, no space after a mark, a comma"
        );
        let (many, long, endless) = (
            "Fine. ".repeat(100),
            format!("{}. b", "a".repeat(400)),
            "x".repeat(400),
        );
        assert_eq!(SPAN, 390, "120 tokens at 3.25 bytes");
        assert_eq!(
            [&many, &long, &endless].map(|text| (cut(text, false), cut(text, true))),
            [
                (Some(389), Some(389)),
                (Some(401), Some(401)),
                (None, Some(400))
            ],
            "the last end inside a span; a longer sentence whole; no end, all of it"
        );
    }

    /// A say's texts waiting back to back reach the speaking as one line, its end with them, in
    /// order among the rest; another say's are its own.
    #[test]
    fn joins() -> io::Result<()> {
        let (lines, queued) = mpsc::sync_channel(8);
        for line in [
            json!({"say": 1u8, "text": "One. "}),
            json!({"say": 1u8, "text": "Two, "}),
            json!({"say": 1u8, "text": "three. "}),
            json!({"end": 1u8}),
            json!({"say": 2u8, "text": "Hi. "}),
            json!({"say": 3u8, "text": "Yo. "}),
        ] {
            lines.send(line).map_err(io::Error::other)?;
        }
        drop(lines);
        let mut taken = Vec::new();
        let mut take = |line: &Value| {
            taken.push(line.clone());
            Ok(())
        };
        hear(&queued, &mut take)?;
        assert_eq!(
            taken,
            [
                json!({"end": 1u8, "say": 1u8, "text": "One. Two, three. "}),
                json!({"say": 2u8, "text": "Hi. "}),
                json!({"say": 3u8, "text": "Yo. "}),
            ],
            "joined"
        );
        Ok(())
    }

    /// Answers each give of `play` until `refuse`, then refuses; the samples given.
    fn plays(play: &mut Conn, gives: usize, refuse: bool) -> io::Result<u64> {
        let mut taken = 0u64;
        for at in 0..gives {
            let pcm = play.line()?;
            taken = taken.saturating_add(
                (pcm["pcm"].as_str().unwrap_or_default().len() as u64).div_euclid(4),
            );
            let reply = if refuse && at == 0 {
                json!({"code": "bad_request", "ok": false})
            } else {
                json!({"given": taken, "ok": true})
            };
            play.send(&reply)?;
        }
        Ok(taken)
    }

    /// A session: a say spoken span by span through its play, each to its last sentence end
    /// within a span, commas and all, gives of at most 8192 samples, a mark per span, the play
    /// ended after the
    /// say's; a give refused stopping the say; a play refused, then `done`; a span of spaces
    /// skipped and one silent unmarked; a hush; a refused report said; the connection's end an
    /// error.
    #[test]
    fn sessions() -> io::Result<()> {
        let fake = Fake::new("read-aloud-sessions")?;
        let sock = fake.sock.clone();
        let engine = thread::spawn(move || {
            session(&sock, "mouth-desk", &mut Hum).map_err(|cause| cause.to_string())
        });
        let (mut conn, request) = fake.accept()?;
        assert_eq!(
            request,
            json!({"kind": "speech", "name": "read-aloud", "op": "engine", "track": "mouth-desk"}),
            "the engine request"
        );
        conn.send(&json!({"ok": true, "seq": 1u8}))?;
        // a line of none of its kinds, the protocol grown since: ignored, never a say 0
        conn.send(&json!({"news": true, "text": "Spoken by none. "}))?;
        conn.send(&json!({"say": 1u8, "text": "Hello there. Hi, you: yes; no. How"}))?;
        let (mut play, tagged) = fake.accept()?;
        assert_eq!(
            tagged,
            json!({"op": "play", "rate": 24_000u32, "say": 1u8, "track": "mouth-desk"}),
            "tagged"
        );
        play.send(&json!({"ok": true, "play": 1u8, "seq": 2u8}))?;
        let given = plays(&mut play, 5, false)?;
        assert_eq!(
            given, 30_010,
            "two sentences in one span, in pieces of at most 8192"
        );
        assert_eq!(
            conn.line()?,
            json!({"op": "mark", "samples": 30_010u32, "say": 1u8, "text": 30u8}),
            "marked"
        );
        conn.send(&json!({"code": "bad_request", "ok": false}))?;
        conn.send(&json!({"say": 1u8, "text": " are you?"}))?;
        conn.send(&json!({"end": 1u8}))?;
        let more = plays(&mut play, 3, false)?;
        assert_eq!(
            conn.line()?,
            json!({"op": "mark", "samples": 30_010u64.saturating_add(more), "say": 1u8, "text": 43u8}),
            "all"
        );
        conn.send(&json!({"ok": true}))?;
        assert_eq!(
            play.line()?,
            json!({"op": "end"}),
            "the play's end after the say's"
        );
        play.send(&json!({"ok": true}))?;
        play.send(&json!({"play": 1u8, "reason": "end"}))?;
        // say 2: its first give refused, so nothing more of it is spoken
        conn.send(&json!({"say": 2u8, "text": "Stop now, please. "}))?;
        let (mut stopped, _) = fake.accept()?;
        stopped.send(&json!({"ok": true, "play": 2u8, "seq": 3u8}))?;
        assert_eq!(plays(&mut stopped, 1, true)?, 8192, "one give, refused");
        conn.send(&json!({"hush": 2u8}))?;
        // say 3: its play refused; say 4: only spaces; say 5: silent; each ends in `done`
        for (num, text) in [(3u8, "Hi."), (4u8, " "), (5u8, "-.")] {
            conn.send(&json!({"say": num, "text": text}))?;
            conn.send(&json!({"end": num}))?;
            if num == 3 {
                let (mut busy, _) = fake.accept()?;
                busy.send(&json!({"code": "busy", "ok": false}))?;
            }
            assert_eq!(
                conn.line()?,
                json!({"op": "done", "say": num}),
                "nothing sounded"
            );
        }
        // say 6, hushed before its text: no play opened
        conn.send(&json!({"hush": 6u8}))?;
        conn.send(&json!({"say": 6u8, "text": "Gone. "}))?;
        conn.send(&json!({"end": 6u8}))?;
        assert_eq!(
            conn.line()?,
            json!({"op": "done", "say": 6u8}),
            "hushed: nothing sounded"
        );
        // say 7, hushed mid-span while its gives are answered: its play shut at once, no
        // give after the hush and no mark
        conn.send(&json!({"say": 7u8, "text": "A long sentence here. "}))?;
        let (mut hushed, _) = fake.accept()?;
        hushed.send(&json!({"ok": true, "play": 7u8, "seq": 4u8}))?;
        assert_eq!(plays(&mut hushed, 1, false)?, 8192, "one give answered");
        drop(hushed.line()?);
        conn.send(&json!({"hush": 7u8}))?;
        assert!(hushed.line().is_err(), "the play shut, nothing more given");
        conn.send(&json!({"say": 8u8, "text": " "}))?;
        conn.send(&json!({"end": 8u8}))?;
        assert_eq!(
            conn.line()?,
            json!({"op": "done", "say": 8u8}),
            "no mark for say 7"
        );
        // say 9, longer than a span: one span per span's worth of sentences, each marked, the
        // marks rising to every byte but the last space, which the end then speaks as nothing
        conn.send(&json!({"say": 9u8, "text": "Fine. ".repeat(100)}))?;
        let (mut long, _) = fake.accept()?;
        long.send(&json!({"ok": true, "play": 9u8, "seq": 5u8}))?;
        let mut sounded = 0u64;
        for (len, text) in [(389usize, 389u16), (209, 599)] {
            let answers = len.saturating_mul(1000).div_ceil(GIVE).saturating_add(1);
            sounded = sounded.saturating_add(plays(&mut long, answers, false)?);
            assert_eq!(
                conn.line()?,
                json!({"op": "mark", "samples": sounded, "say": 9u8, "text": text}),
                "a span's mark"
            );
        }
        assert_eq!(sounded, 598_020, "every sentence once");
        conn.send(&json!({"end": 9u8}))?;
        assert_eq!(long.line()?, json!({"op": "end"}), "ended");
        long.send(&json!({"ok": true}))?;
        long.send(&json!({"play": 9u8, "reason": "end"}))?;
        drop(conn);
        assert_eq!(
            engine.join().ok().map(Result::err),
            Some(Some("the connection ended".to_owned())),
            "the end"
        );
        Ok(())
    }
}

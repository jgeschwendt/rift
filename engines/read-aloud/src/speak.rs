//! The speaking, its model aside (docs/protocol.html#engines).
//!
//! A say's text is cut clause by clause as it comes, each clause's audio given through the say's
//! play as it is made, a `mark` once a clause is given, the play ended after the say's end
//! (`done` when nothing sounded), a hush stopping it mid-clause.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use link::{Conn, hex};
use serde_json::{Value, json};

/// The hello's client.
const CLIENT: &str = "read-aloud/0.1.0";
/// Samples in a give at most: 16384 bytes of 16-bit PCM.
const GIVE: usize = 8192;

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

/// Where the next clause of `rest` ends: just past a clause's mark that a space follows; with
/// `all`, at the end of what is left.
fn cut(rest: &str, all: bool) -> Option<usize> {
    rest.char_indices()
        .find(|&(at, mark)| {
            ".!?;:,".contains(mark) && rest[at + 1..].starts_with(char::is_whitespace)
        })
        .map(|(at, _)| at + 1)
        .or_else(|| (all && !rest.is_empty()).then_some(rest.len()))
}

/// A say under way.
#[derive(Debug, Default)]
struct Say {
    /// Samples its play has been given.
    given: u64,
    /// Its number.
    num: u64,
    /// Hushed, refused or ended: nothing more is spoken.
    over: bool,
    /// Its play's connection, opened at its first audio.
    play: Option<Conn>,
    /// Bytes of `text` cut into clauses so far.
    spoken: usize,
    /// All the text it was given.
    text: String,
}

impl Say {
    /// Gives `chunk` to the play, opening it first; whether to go on.
    fn give(&mut self, chunk: &[f32], hushed: &AtomicU64, request: &Value, sock: &Path) -> bool {
        self.over |= hushed.load(Ordering::Relaxed) >= self.num;
        if self.play.is_none() && !self.over {
            match Conn::open(sock, CLIENT, request) {
                Ok((play, _)) => self.play = Some(play),
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
                self.given += piece.len() as u64;
            } else {
                self.over = true;
            }
        }
        !self.over
    }
}

/// One engine connection's speaking.
struct Speaker<'a> {
    /// The highest say a hush has named, seen by the reader as it comes.
    hushed: &'a AtomicU64,
    /// The say under way.
    say: Option<Say>,
    /// The daemon's socket, for the plays.
    sock: &'a Path,
    /// The mouth.
    track: &'a str,
    /// The model.
    voice: &'a mut dyn Voice,
    /// The engine connection's write side, for marks and `done`.
    write: Conn,
}

impl Speaker<'_> {
    /// Takes one line the daemon sent: a report's reply, a say's text, its end or a hush.
    fn line(&mut self, line: &Value) -> io::Result<()> {
        if line["ok"].is_boolean() {
            if line["ok"] != true {
                eprintln!("read-aloud: refused: {line}");
            }
            return Ok(());
        }
        let num = ["end", "hush", "say"]
            .iter()
            .find_map(|key| line[key].as_u64())
            .unwrap_or_default();
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

    /// Speaks each whole clause `say` holds unspoken, with `all` the rest too, marking each.
    fn speak(&mut self, say: &mut Say, all: bool) -> io::Result<()> {
        let request =
            json!({"op": "play", "rate": self.voice.rate(), "say": say.num, "track": self.track});
        while let Some(at) = cut(&say.text[say.spoken..], all).filter(|_| !say.over) {
            let clause = say.text[say.spoken..say.spoken + at].trim().to_owned();
            say.spoken += at;
            if clause.is_empty() {
                continue;
            }
            let (hushed, sock) = (self.hushed, self.sock);
            let mut give = |chunk: &[f32]| Ok(say.give(chunk, hushed, &request, sock));
            self.voice.speak(&clause, &mut give)?;
            if !say.over && say.play.is_some() {
                let text = say.text[..say.spoken].chars().count();
                let mark =
                    json!({"op": "mark", "samples": say.given, "say": say.num, "text": text});
                self.write.send(&mark)?;
            }
        }
        Ok(())
    }
}

/// One connection to `sock` as the speech engine of the mouth `track`, speaking through
/// `voice`; ends with the connection, an error.
pub fn session(sock: &Path, track: &str, voice: &mut dyn Voice) -> io::Result<()> {
    let request = json!({"kind": "speech", "name": "read-aloud", "op": "engine", "track": track});
    let (mut conn, _) = Conn::open(sock, CLIENT, &request)?;
    let write = conn.twin()?;
    let hushed = Arc::new(AtomicU64::new(0));
    let (lines, heard) = mpsc::channel();
    let hearing = Arc::clone(&hushed);
    let reader = thread::spawn(move || -> io::Result<()> {
        loop {
            let line = conn.line()?;
            // a hush is seen at once, so a clause being made stops at its next chunk
            let _: u64 =
                hearing.fetch_max(line["hush"].as_u64().unwrap_or_default(), Ordering::Relaxed);
            lines.send(line).map_err(io::Error::other)?;
        }
    });
    let mut speaker = Speaker {
        hushed: &hushed,
        say: None,
        sock,
        track,
        voice,
        write,
    };
    let worked = heard.iter().try_for_each(|line| speaker.line(&line));
    speaker.write.shutdown();
    worked.and_then(|()| reader.join().unwrap_or(Ok(())))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::thread;

    use link::Conn;
    use link::fake::Fake;
    use serde_json::json;

    use super::{Voice, cut, session};

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
            for len in [text.len() * 1000, 10] {
                if !give(&vec![0.25; len])? {
                    break;
                }
            }
            Ok(())
        }
    }

    /// Clauses end past a mark a space follows; the rest only with the say's end.
    #[test]
    fn clauses() {
        assert_eq!(
            ["Hi, you. ", "e.g.x", "Wait"].map(|text| (cut(text, false), cut(text, true))),
            [(Some(3), Some(3)), (None, Some(5)), (None, Some(4))],
            "a comma, no space after a mark, the end"
        );
    }

    /// Answers each give of `play` until `refuse`, then refuses; the samples given.
    fn plays(play: &mut Conn, gives: usize, refuse: bool) -> io::Result<u64> {
        let mut taken = 0;
        for at in 0..gives {
            taken += play.line()?["pcm"].as_str().unwrap_or_default().len() as u64 / 4;
            let reply = if refuse && at == 0 {
                json!({"code": "bad_request", "ok": false})
            } else {
                json!({"given": taken, "ok": true})
            };
            play.send(&reply)?;
        }
        Ok(taken)
    }

    /// A session: a say spoken clause by clause through its play, gives of at most 8192
    /// samples, a mark per clause, the play ended after the say's; a give refused stopping the
    /// say; a play refused, then `done`; a clause of spaces skipped; a hush; a refused report
    /// said; the connection's end an error.
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
        conn.send(&json!({"ok": true, "seq": 1}))?;
        conn.send(&json!({"say": 1, "text": "Hello there. How"}))?;
        let (mut play, request) = fake.accept()?;
        assert_eq!(
            request,
            json!({"op": "play", "rate": 24_000, "say": 1, "track": "mouth-desk"}),
            "tagged"
        );
        play.send(&json!({"ok": true, "play": 1, "seq": 2}))?;
        let given = plays(&mut play, 3, false)?;
        assert_eq!(given, 12_010, "the clause, in pieces of at most 8192");
        assert_eq!(
            conn.line()?,
            json!({"op": "mark", "samples": 12_010, "say": 1, "text": 12}),
            "marked"
        );
        conn.send(&json!({"code": "bad_request", "ok": false}))?;
        conn.send(&json!({"say": 1, "text": " are you?"}))?;
        conn.send(&json!({"end": 1}))?;
        let more = plays(&mut play, 3, false)?;
        assert_eq!(
            conn.line()?,
            json!({"op": "mark", "samples": 12_010 + more, "say": 1, "text": 25}),
            "all"
        );
        conn.send(&json!({"ok": true}))?;
        assert_eq!(
            play.line()?,
            json!({"op": "end"}),
            "the play's end after the say's"
        );
        play.send(&json!({"ok": true}))?;
        play.send(&json!({"play": 1, "reason": "end"}))?;
        // say 2: its first give refused, so nothing more of it is spoken
        conn.send(&json!({"say": 2, "text": "Stop now, please. "}))?;
        let (mut play, _) = fake.accept()?;
        play.send(&json!({"ok": true, "play": 2, "seq": 3}))?;
        assert_eq!(plays(&mut play, 1, true)?, 8192, "one give, refused");
        conn.send(&json!({"hush": 2}))?;
        // say 3: its play refused; say 4: only spaces; both end in `done`
        for (num, text) in [(3, "Hi."), (4, " ")] {
            conn.send(&json!({"say": num, "text": text}))?;
            conn.send(&json!({"end": num}))?;
            if num == 3 {
                let (mut play, _) = fake.accept()?;
                play.send(&json!({"code": "busy", "ok": false}))?;
            }
            assert_eq!(
                conn.line()?,
                json!({"op": "done", "say": num}),
                "nothing sounded"
            );
        }
        // say 5, hushed before its text: no play opened
        conn.send(&json!({"hush": 5}))?;
        conn.send(&json!({"say": 5, "text": "Gone. "}))?;
        conn.send(&json!({"end": 5}))?;
        assert_eq!(
            conn.line()?,
            json!({"op": "done", "say": 5}),
            "hushed: nothing sounded"
        );
        drop(conn);
        assert_eq!(
            engine.join().ok().map(Result::err),
            Some(Some("the connection ended".to_owned())),
            "the end"
        );
        Ok(())
    }
}

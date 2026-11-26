//! What rift's Rust engines share (docs/protocol.html#engines): the daemon's socket, a
//! connection past its hello, the stream's PCM as hex, a flag's value, a model's place in the
//! Hugging Face cache, and standing: a session run again forever, a stop signal one stderr line.

/// A daemon a test plays, for the engines' suites.
#[cfg(any(test, feature = "fake"))]
pub mod fake {
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::{env, fs, io, process};

    use serde_json::{Value, json};

    use super::{BufReader, Conn};

    /// A socket in a directory of its own under the system's temp directory.
    #[derive(Debug)]
    #[non_exhaustive]
    pub struct Fake {
        /// What accepts on it.
        pub listener: UnixListener,
        /// The socket's path.
        pub sock: PathBuf,
    }

    impl Fake {
        /// The next connection, its hello answered: it and its first request.
        #[inline]
        pub fn accept(&self) -> io::Result<(Conn, Value)> {
            let (write, _) = self.listener.accept()?;
            let mut conn = Conn {
                read: BufReader::new(write.try_clone()?),
                write,
            };
            drop(conn.line()?);
            conn.send(&json!({"ok": true, "proto": 1u8}))?;
            let request = conn.line()?;
            Ok((conn, request))
        }

        /// A fresh socket for the test `name`.
        #[inline]
        pub fn new(name: &str) -> io::Result<Self> {
            let dir = env::temp_dir().join(format!("rift-{name}-{}", process::id()));
            drop(fs::remove_dir_all(&dir));
            fs::create_dir_all(&dir)?;
            let sock = dir.join("rift.sock");
            Ok(Self {
                listener: UnixListener::bind(&sock)?,
                sock,
            })
        }
    }
}

use std::io::{self, BufRead as _, BufReader, Write as _};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, process, thread};

use serde_json::{Value, json};
use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use signal_hook::low_level::signal_name;

/// The stream's rate: 16 kHz mono.
pub const RATE: u64 = 16_000;

/// One connection to the daemon, past its hello.
#[derive(Debug)]
pub struct Conn {
    /// Its read side, a line at a time.
    read: BufReader<UnixStream>,
    /// Its write side.
    write: UnixStream,
}

impl Conn {
    /// The next line read, as JSON; the connection's end an error.
    #[inline]
    pub fn line(&mut self) -> io::Result<Value> {
        let mut text = String::new();
        if self.read.read_line(&mut text)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection ended",
            ));
        }
        serde_json::from_str(&text).map_err(io::Error::other)
    }

    /// Connects to `sock`, says hello as `client` and sends `request`: the connection and the
    /// request's reply, or the refusing line as the error.
    #[inline]
    pub fn open(sock: &Path, client: &str, request: &Value) -> io::Result<(Self, Value)> {
        let write = UnixStream::connect(sock)?;
        let mut conn = Self {
            read: BufReader::new(write.try_clone()?),
            write,
        };
        let mut reply = Value::Null;
        for line in [
            &json!({"client": client, "op": "hello", "proto": 1u8}),
            request,
        ] {
            conn.send(line)?;
            reply = conn.line()?;
            if reply["ok"] != true {
                return Err(io::Error::other(reply.to_string()));
            }
        }
        Ok((conn, reply))
    }

    /// Writes `line` and its newline.
    #[inline]
    pub fn send(&mut self, line: &Value) -> io::Result<()> {
        writeln!(self.write, "{line}")
    }

    /// Ends both directions, so a reader blocked on it wakes to its end.
    #[inline]
    pub fn shutdown(&self) {
        drop(self.write.shutdown(Shutdown::Both));
    }

    /// A second handle writing on the same connection, so one thread reads while another writes;
    /// only one of the two is ever read.
    #[inline]
    pub fn twin(&self) -> io::Result<Self> {
        Ok(Self {
            read: BufReader::new(self.write.try_clone()?),
            write: self.write.try_clone()?,
        })
    }
}

/// The value after `--<name>` among `args`.
#[must_use]
#[inline]
pub fn flag<'args>(args: &'args [String], name: &str) -> Option<&'args str> {
    args.iter()
        .skip_while(|arg| arg.strip_prefix("--") != Some(name))
        .nth(1)
        .map(String::as_str)
}

/// A frame's samples, from its hex of 16-bit little-endian PCM, as floats in [-1, 1).
#[must_use]
#[inline]
pub fn samples(hex: &str) -> Vec<f32> {
    let byte = |pair: &[u8]| {
        str::from_utf8(pair)
            .ok()
            .and_then(|text| u8::from_str_radix(text, 16).ok())
            .unwrap_or_default()
    };
    hex.as_bytes()
        .chunks_exact(4)
        .map(|quad| {
            let (low, high) = quad.split_at(2);
            f32::from(i16::from_le_bytes([byte(low), byte(high)])) / 32768.0
        })
        .collect()
}

/// Samples as a give's hex: clamped to 16 bits, little-endian, lowercase.
#[must_use]
#[inline]
pub fn hex(samples: &[f32]) -> String {
    samples
        .iter()
        .flat_map(|&sample| ((sample.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes())
        .fold(
            String::with_capacity(samples.len().saturating_mul(4)),
            |mut out, byte| {
                out.extend(
                    [byte >> 4u8, byte & 15u8]
                        .map(|nibble| char::from_digit(u32::from(nibble), 16).unwrap_or_default()),
                );
                out
            },
        )
}

/// The snapshot of Hugging Face repo `repo` at `revision` in the hub cache: `$HF_HUB_CACHE`,
/// else `$HF_HOME/hub`, else `~/.cache/huggingface/hub`, where `hf download` puts it.
#[must_use]
#[inline]
pub fn hub(repo: &str, revision: &str) -> PathBuf {
    let home = || PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".cache/huggingface");
    env::var_os("HF_HUB_CACHE")
        .map_or_else(
            || {
                env::var_os("HF_HOME")
                    .map_or_else(home, PathBuf::from)
                    .join("hub")
            },
            PathBuf::from,
        )
        .join(format!("models--{}", repo.replace('/', "--")))
        .join("snapshots")
        .join(revision)
}

/// The daemon's socket: `run/rift.sock` under `$RIFT_ROOT`, else under `~/.local/state/rift`.
#[must_use]
#[inline]
pub fn socket() -> PathBuf {
    env::var_os("RIFT_ROOT")
        .map_or_else(
            || PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".local/state/rift"),
            PathBuf::from,
        )
        .join("run/rift.sock")
}

/// Stands as `name`, as a device does, until a stop signal.
///
/// SIGTERM, SIGINT and SIGHUP each end it with one stderr line, `<name>: stopped by SIG<name>`,
/// and exit 0; `session` runs again forever, each end waited out `$RIFT_POLL_MS` (default 1000)
/// ms; its failure is one stderr line, said once until another cause or a session's clean end.
#[inline]
pub fn stand<F: FnMut() -> io::Result<()>>(name: &str, mut session: F) -> ! {
    let named = name.to_owned();
    drop(Signals::new([SIGHUP, SIGINT, SIGTERM]).map(|mut signals| {
        thread::spawn(move || signals.forever().next().map(|signal| stop(&named, signal)))
    }));
    let poll = env::var("RIFT_POLL_MS")
        .ok()
        .and_then(|held| held.parse().ok())
        .unwrap_or(1000);
    let mut said = String::new();
    loop {
        let cause = session()
            .err()
            .map(|cause| cause.to_string())
            .unwrap_or_default();
        if !cause.is_empty() && cause != said {
            drop(writeln!(io::stderr(), "{name}: {cause}"));
        }
        said = cause;
        thread::sleep(Duration::from_millis(poll));
    }
}

/// A stop signal's end: one stderr line, exit 0. A function of its own, so its lines keep their
/// count in a coverage profile the exit writes.
#[expect(
    clippy::exit,
    clippy::single_call_fn,
    reason = "the stop signal's exit 0, as rift's devices; a function of its own for coverage"
)]
fn stop(name: &str, signal: i32) -> ! {
    let said = signal_name(signal).unwrap_or_default();
    drop(writeln!(io::stderr(), "{name}: stopped by {said}"));
    process::exit(0)
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "a test asserts, and propagates its setup with ?"
)]
mod tests {
    use std::process::Command;
    use std::{env, io, thread};

    use serde_json::json;
    use signal_hook::consts::SIGTERM;
    use signal_hook::low_level::raise;

    use super::fake::Fake;
    use super::{Conn, flag, hex, hub, samples, socket, stand};

    /// PCM round-trips through hex, clamped, and a flag's value is the word after it.
    #[test]
    fn words() {
        let pcm = [0.0f32, 0.5, -1.0, 2.0];
        assert_eq!(
            hex(&pcm),
            "0000ff3f0180ff7f",
            "16-bit little-endian, clamped"
        );
        assert_eq!(
            samples(&hex(&pcm)),
            [
                0.0f32,
                16383.0 / 32768.0,
                -32767.0 / 32768.0,
                32767.0 / 32768.0
            ],
            "back as floats"
        );
        let args = ["ear-desk", "--bar", "0.7", "--x"].map(str::to_owned);
        assert_eq!(
            (flag(&args, "bar"), flag(&args, "x"), flag(&args, "y")),
            (Some("0.7"), None, None),
            "the word after the flag"
        );
    }

    /// The socket and the hub cache follow their variables, each case a child of this suite with
    /// its environment set, since the suite never mutates its own.
    #[test]
    fn places() -> io::Result<()> {
        if env::var_os("LINK_PLACES").is_some() {
            println!("\nat {} {}", socket().display(), hub("a/b", "c").display());
            return Ok(());
        }
        let mut seen = Vec::new();
        for vars in [
            &[("RIFT_ROOT", "r"), ("HF_HUB_CACHE", "h")][..],
            &[("HF_HOME", "f")],
            &[],
        ] {
            let out = Command::new(env::current_exe()?)
                .args(["--exact", "tests::places", "--nocapture", "--quiet"])
                .env_remove("RIFT_ROOT")
                .env_remove("HF_HUB_CACHE")
                .env_remove("HF_HOME")
                .env("HOME", "/home")
                .env("LINK_PLACES", "1")
                .envs(vars.iter().copied())
                .output()?;
            let printed = String::from_utf8_lossy(&out.stdout).into_owned();
            seen.push(
                printed
                    .lines()
                    .find_map(|line| line.strip_prefix("at "))
                    .unwrap_or_default()
                    .to_owned(),
            );
        }
        assert_eq!(
            seen,
            [
                "r/run/rift.sock h/models--a--b/snapshots/c",
                "/home/.local/state/rift/run/rift.sock f/hub/models--a--b/snapshots/c",
                "/home/.local/state/rift/run/rift.sock /home/.cache/huggingface/hub/models--a--b/snapshots/c",
            ],
            "each variable in its order"
        );
        Ok(())
    }

    /// A connection: hello then the request, its reply returned; a twin writing on it; a shutdown
    /// ending it, read as an error; a refusal its line.
    #[test]
    fn connection() -> io::Result<()> {
        let fake = Fake::new("link-connection")?;
        let sock = fake.sock.clone();
        let client = thread::spawn(move || -> io::Result<String> {
            let (conn, reply) = Conn::open(&sock, "test", &json!({"op": "x"}))?;
            conn.twin()?.send(&json!({"op": "y"}))?;
            conn.shutdown();
            let refused = Conn::open(&sock, "test", &json!({"op": "x"})).err();
            Ok(format!(
                "{reply} {}",
                refused.map(|cause| cause.to_string()).unwrap_or_default()
            ))
        });
        let (mut conn, request) = fake.accept()?;
        assert_eq!(request, json!({"op": "x"}), "the request after the hello");
        conn.send(&json!({"ok": true, "seq": 1u8}))?;
        assert_eq!(conn.line()?, json!({"op": "y"}), "the twin's line");
        assert_eq!(
            conn.line()
                .map_err(|cause| cause.to_string())
                .err()
                .as_deref(),
            Some("the connection ended"),
            "the shutdown"
        );
        let (mut other, _) = fake.accept()?;
        other.send(&json!({"code": "taken", "ok": false}))?;
        assert_eq!(
            client.join().ok().transpose()?.unwrap_or_default(),
            r#"{"ok":true,"seq":1} {"code":"taken","ok":false}"#,
            "the reply, then the refusal"
        );
        Ok(())
    }

    /// `stand`, run as a child of this suite: a failure said once while it repeats, the stop
    /// signal's line and exit 0.
    #[test]
    fn standing() -> io::Result<()> {
        if env::var_os("LINK_STAND").is_some() {
            let mut tries = 0u8;
            stand("probe", || {
                tries = tries.saturating_add(1);
                if tries == 3 {
                    drop(raise(SIGTERM));
                }
                Err(io::Error::other("down"))
            });
        }
        let out = Command::new(env::current_exe()?)
            .args(["--exact", "tests::standing", "--nocapture"])
            .env("LINK_STAND", "1")
            .env("RIFT_POLL_MS", "1")
            .output()?;
        assert_eq!(
            (
                String::from_utf8_lossy(&out.stderr).into_owned(),
                out.status.code()
            ),
            (
                "probe: down\nprobe: stopped by SIGTERM\n".to_owned(),
                Some(0i32)
            ),
            "one line per cause, then the stop"
        );
        Ok(())
    }
}

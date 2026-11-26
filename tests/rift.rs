//! End to end: the built `rift` over temp roots, against docs/protocol.html and the daemon rules;
//! a daemon a test starts is drained by it, or killed when its guard drops.

// the binary's dependencies, unused by the suite
#[cfg(target_os = "macos")]
use coreaudio as _;
#[cfg(target_os = "macos")]
use cpal as _;
use hotswap as _;
use signal_hook as _;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use std::env;
    use std::error::Error;
    use std::f64::consts::PI;
    use std::fmt::Write as _;
    use std::fs::{self, File, OpenOptions, Permissions};
    use std::io::{self, BufRead, BufReader, Write as _};
    use std::iter;
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _, symlink};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::process::{Child, ChildStderr, Command, ExitStatus, Output, Stdio, id};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    /// The hello every raw connection opens with.
    const HELLO: &str = r#"{"client":"test/1","op":"hello","proto":1}"#;

    /// A test's fallible outcome.
    type Outcome = Result<(), Box<dyn Error>>;

    /// A running `rift serve`, killed and reaped when it drops.
    struct Daemon(Child);

    impl Drop for Daemon {
        fn drop(&mut self) {
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }

    /// An attached disk image by its mount point, detached when it drops.
    struct Volume(String);

    impl Drop for Volume {
        fn drop(&mut self) {
            drop(bounded(
                Command::new("hdiutil").args(["detach", "-force", &self.0]),
                "hdiutil detach",
            ));
        }
    }

    /// The processes the `launchctl` stub started under a root, by the pids it wrote under
    /// `loaded/` and in `holder`: each still this build's binary is killed when it drops, on every
    /// path, a failed assertion's included.
    struct Started(String);

    impl Drop for Started {
        fn drop(&mut self) {
            let files = fs::read_dir(format!("{}/loaded", self.0))
                .into_iter()
                .flatten()
                .flatten()
                .map(|entry| entry.path().display().to_string())
                .chain([format!("{}/holder", self.0)]);
            for held in files.filter_map(|file| fs::read_to_string(file).ok()) {
                // a pid file is a hint: signalled only while it names this build's binary
                let pid = held.trim();
                let ours = bounded(Command::new("ps").args(["-o", "comm=", "-p", pid]), "ps")
                    .is_ok_and(|out| {
                        String::from_utf8_lossy(&out.stdout).trim() == env!("CARGO_BIN_EXE_rift")
                    });
                if ours {
                    drop(bounded(Command::new("kill").args(["-9", pid]), "kill"));
                }
            }
        }
    }

    /// `command` run to its end, its output kept; past a minute it is killed and the test fails
    /// naming `what`, so a command that never answers is a red test, never a held suite.
    fn bounded(command: &mut Command, what: &str) -> Result<Output, Box<dyn Error>> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let (pid, (done, ended)) = (child.id().to_string(), mpsc::channel::<()>());
        let watch = thread::spawn(move || {
            let late = matches!(
                ended.recv_timeout(Duration::from_secs(60)),
                Err(mpsc::RecvTimeoutError::Timeout)
            );
            if late {
                drop(Command::new("kill").args(["-9", &pid]).status());
            }
            late
        });
        let out = child.wait_with_output()?;
        drop(done);
        if watch.join().unwrap_or_default() {
            return Err(format!("{what}: no answer in 60 s").into());
        }
        Ok(out)
    }

    /// `left` equal to `right`, else an error showing both.
    fn same(left: &str, right: &str) -> Outcome {
        if left == right {
            Ok(())
        } else {
            Err(format!("\n left: {left}\nright: {right}").into())
        }
    }

    /// A fresh directory for `name`, short enough for a socket path under it.
    fn temp(name: &str) -> Result<String, Box<dyn Error>> {
        let dir = env::temp_dir().join(format!("rift-{name}-{}", id()));
        drop(fs::remove_dir_all(&dir));
        fs::create_dir_all(&dir)?;
        Ok(fs::canonicalize(dir)?.to_string_lossy().into_owned())
    }

    /// `rift args` over `root` with `envs`: its exit code, a newline, its stdout, then its stderr.
    fn rift(root: &str, envs: &[(&str, &str)], args: &[&str]) -> Result<String, Box<dyn Error>> {
        let out = bounded(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", root)
                .envs(envs.iter().copied())
                .args(args),
            &format!("rift {}", args.join(" ")),
        )?;
        Ok(format!(
            "{}\n{}{}",
            out.status.code().unwrap_or(-1),
            String::from_utf8(out.stdout)?,
            String::from_utf8(out.stderr)?
        ))
    }

    /// This build's version, as `rift version` prints it.
    fn version() -> Result<String, Box<dyn Error>> {
        Ok(rift("/nonexistent", &[], &["version"])?
            .trim_start_matches("0\n")
            .trim_end()
            .to_owned())
    }

    /// `rift serve` over `root` with `envs`, and the ready line it prints.
    fn serve(root: &str, envs: &[(&str, &str)]) -> Result<(Daemon, Value), Box<dyn Error>> {
        let mut daemon = Daemon(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", root)
                .envs(envs.iter().copied())
                .arg("serve")
                .stdout(Stdio::piped())
                .spawn()?,
        );
        let mut ready = String::new();
        let _: usize =
            BufReader::new(daemon.0.stdout.take().ok_or("stdout")?).read_line(&mut ready)?;
        Ok((daemon, serde_json::from_str(&ready)?))
    }

    /// A raw connection to the daemon over `root`, nothing sent.
    fn dial(root: &str) -> io::Result<BufReader<UnixStream>> {
        let stream = UnixStream::connect(format!("{root}/run/rift.sock"))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        Ok(BufReader::new(stream))
    }

    /// `line` sent on `conn`, and the next line read (empty at end of stream).
    fn say(conn: &mut BufReader<UnixStream>, line: &str) -> io::Result<String> {
        conn.get_mut().write_all(format!("{line}\n").as_bytes())?;
        next(conn)
    }

    /// The next line on `conn`, its newline trimmed; empty at end of stream.
    fn next<R: BufRead>(conn: &mut R) -> io::Result<String> {
        let mut line = String::new();
        let _: usize = conn.read_line(&mut line)?;
        Ok(line.trim_end().to_owned())
    }

    /// A connection that has said hello.
    fn greet(root: &str) -> Result<BufReader<UnixStream>, Box<dyn Error>> {
        let mut conn = dial(root)?;
        let hello: Value = serde_json::from_str(&say(&mut conn, HELLO)?)?;
        if hello["ok"] != true {
            return Err(format!("hello refused: {hello}").into());
        }
        Ok(conn)
    }

    /// Every line of `<root>/events.ndjson`, parsed.
    fn events(root: &str) -> Result<Vec<Value>, Box<dyn Error>> {
        Ok(fs::read_to_string(format!("{root}/events.ndjson"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?)
    }

    /// Each event's `(boot, seq, e)`, compact.
    fn positions(lines: &[Value]) -> String {
        lines
            .iter()
            .map(|event| format!("{}:{} {}", event["boot"], event["seq"], event["e"]))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Lines read from `conn` until end of stream, parsed.
    fn drain(conn: &mut BufReader<UnixStream>) -> Result<Vec<Value>, Box<dyn Error>> {
        Ok(conn
            .lines()
            .map_while(Result::ok)
            .map(|line| serde_json::from_str(&line))
            .collect::<Result<_, _>>()?)
    }

    /// A stand-in daemon at `<root>/run/rift.sock` for one connection: each of `replies`
    /// answers the next line it reads.
    fn fake(root: &str, replies: &[&str]) -> Outcome {
        fs::create_dir_all(format!("{root}/run"))?;
        drop(fs::remove_file(format!("{root}/run/rift.sock")));
        let listener = UnixListener::bind(format!("{root}/run/rift.sock"))?;
        let owned: Vec<String> = replies.iter().map(|&reply| reply.to_owned()).collect();
        drop(thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut conn = BufReader::new(stream);
                for reply in owned {
                    drop(next(&mut conn));
                    drop(conn.get_mut().write_all(format!("{reply}\n").as_bytes()));
                }
            }
        }));
        Ok(())
    }

    /// `done` asked every 10 ms until it holds, 30 s at most, a deadline against a hang and never
    /// a bound any check rests on: the sweep moves segments off the request path.
    fn until<F: Fn() -> io::Result<bool>>(done: F) -> Outcome {
        let started = Instant::now();
        while !done()? {
            if started.elapsed() > Duration::from_secs(30) {
                return Err("timed out".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// The error reply with `name`, as the daemon writes it.
    fn code(name: &str) -> String {
        format!(r#"{{"code":"{name}","ok":false}}"#)
    }

    /// A raw device: `line`, its device request, sent on a fresh connection, then the reply's
    /// ping answered by a clock `skew` microseconds off the daemon's; the connection and reply.
    fn device(
        root: &str,
        line: &str,
        skew: i64,
    ) -> Result<(BufReader<UnixStream>, Value), Box<dyn Error>> {
        let mut conn = greet(root)?;
        let reply: Value = serde_json::from_str(&say(&mut conn, line)?)?;
        let ping = reply["ping"].as_u64().ok_or("no ping")?;
        let answer = json!({"op": "pong", "ping": ping, "t": ping.saturating_add_signed(skew)});
        conn.get_mut().write_all(format!("{answer}\n").as_bytes())?;
        Ok((conn, reply))
    }

    /// A frame of `fill` bytes stamped `at` sent on `conn`, and its reply.
    fn frame(conn: &mut BufReader<UnixStream>, at: u64, fill: u8) -> Result<Value, Box<dyn Error>> {
        let line = json!({"op": "frame", "pcm": format!("{fill:02x}").repeat(640), "t": at});
        Ok(serde_json::from_str(&say(conn, &line.to_string())?)?)
    }

    /// The next `count` events' `e` on `feed`.
    fn take(feed: &mut BufReader<UnixStream>, count: usize) -> Result<Value, Box<dyn Error>> {
        Ok(Value::Array(
            iter::repeat_with(|| -> Result<Value, Box<dyn Error>> {
                Ok(serde_json::from_str::<Value>(&next(feed)?)?["e"].clone())
            })
            .take(count)
            .collect::<Result<_, _>>()?,
        ))
    }

    /// The daemon's life: the ready line, an absent root and `run/` made 0700, a socket and its pid, a second
    /// instance refused and logged, `status` and `version`, every error code, `id` echoed, quiet's
    /// idempotence, a failed append answered `io` with health `degraded` until the next append,
    /// then a SIGTERM drain under an open connection: `stopping` in its health and for any other
    /// request, the `stop` event, exit 0 once it closes, the pid file and socket gone, start,
    /// refusal, degradation and exit logged.
    #[test]
    fn life() -> Outcome {
        let (root, built) = (format!("{}/root", temp("life")?), version()?);
        let (mut daemon, ready) = serve(&root, &[])?;
        let pid = daemon.0.id();
        let since = ready["since"].as_str().unwrap_or_default().to_owned();
        let health = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "playing": {}, "proto": 1u8, "quiet": false, "seq": 1u8, "since": since, "state": "ready", "version": built, "window": 600u16});
        same(&ready.to_string(), &health.to_string())?;
        same(&since.len().to_string(), "24")?;
        for dir in [root.clone(), format!("{root}/run")] {
            same(&format!("{:o}", fs::metadata(dir)?.mode() & 0o777), "700")?;
        }
        same(
            &fs::read_to_string(format!("{root}/run/serve.pid"))?,
            &pid.to_string(),
        )?;
        same(
            &fs::metadata(format!("{root}/run/rift.sock"))?
                .file_type()
                .is_socket()
                .to_string(),
            "true",
        )?;
        same(
            &rift(&root, &[], &["serve"])?,
            &format!("1\nrift serve: already running (pid {pid}, by its pid file)\n"),
        )?;
        same(&rift(&root, &[], &["status"])?, &format!("0\n{health}\n"))?;
        same(&rift(&root, &[], &["version"])?, &format!("0\n{built}\n"))?;
        let hello = json!({"boot": 1u8, "id": 7u8, "ok": true, "pid": pid, "proto": 1u8, "since": since, "state": "ready", "version": built});
        same(
            &say(
                &mut dial(&root)?,
                r#"{"client":"test/1","id":7,"op":"hello","proto":1}"#,
            )?,
            &hello.to_string(),
        )?;
        // a refused first line is answered, then the connection closed
        for (first, reply) in [
            (
                "not json",
                r#"{"code":"bad_request","ok":false}"#.to_owned(),
            ),
            (
                r#"{"op":"health"}"#,
                r#"{"code":"bad_request","ok":false}"#.to_owned(),
            ),
            (
                r#"{"op":"hello","proto":1}"#,
                r#"{"code":"bad_request","ok":false}"#.to_owned(),
            ),
            (
                r#"{"client":"test/1","op":"hello","proto":2}"#,
                format!(r#"{{"code":"proto","ok":false,"proto":1,"version":"{built}"}}"#),
            ),
        ] {
            let mut conn = dial(&root)?;
            same(&say(&mut conn, first)?, &reply)?;
            same(&next(&mut conn)?, "")?;
        }
        let mut conn = greet(&root)?;
        for (line, reply) in [
            ("[]", r#"{"code":"bad_request","ok":false}"#),
            ("{}", r#"{"code":"bad_request","ok":false}"#),
            (
                r#"{"id":[1],"op":"health"}"#,
                r#"{"code":"bad_request","ok":false}"#,
            ),
            (
                r#"{"id":"a","op":"nope"}"#,
                r#"{"code":"unknown_op","id":"a","ok":false}"#,
            ),
            (HELLO, r#"{"code":"unknown_op","ok":false}"#),
            (r#"{"op":"quiet"}"#, r#"{"code":"bad_request","ok":false}"#),
            (
                r#"{"on":1,"op":"quiet"}"#,
                r#"{"code":"bad_request","ok":false}"#,
            ),
            (r#"{"op":"feed"}"#, r#"{"code":"bad_request","ok":false}"#),
            (
                r#"{"after":{"boot":1},"op":"feed"}"#,
                r#"{"code":"bad_request","ok":false}"#,
            ),
            (
                r#"{"id":2,"on":true,"op":"quiet"}"#,
                r#"{"changed":true,"id":2,"ok":true,"seq":2}"#,
            ),
            (
                r#"{"on":true,"op":"quiet"}"#,
                r#"{"changed":false,"ok":true,"seq":2}"#,
            ),
        ] {
            same(&say(&mut conn, line)?, reply)?;
        }
        let quieted = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "playing": {}, "proto": 1u8, "quiet": true, "seq": 2u8, "since": since, "state": "ready", "version": built, "window": 600u16});
        // a line of exactly 65 536 bytes is read; one byte more is too long, and closes
        let pad = |len: usize| format!(r#"{{"op":"health","pad":"{}"}}"#, "x".repeat(len - 24));
        same(&say(&mut conn, &pad(0x1_0000))?, &quieted.to_string())?;
        same(
            &say(&mut conn, &pad(0x1_0001))?,
            r#"{"code":"too_long","ok":false}"#,
        )?;
        same(&next(&mut conn)?, "")?;
        same(
            &rift(&root, &[], &["quiet", "off"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":3}\n",
        )?;
        // the log opens but refuses the write (a device that takes none): the event did not
        // happen, and nothing changed
        let (log, aside) = (
            format!("{root}/events.ndjson"),
            format!("{root}/held.ndjson"),
        );
        fs::rename(&log, &aside)?;
        symlink("/dev/urandom", &log)?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "1\n{\"code\":\"io\",\"ok\":false}\n",
        )?;
        let degraded = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "playing": {}, "proto": 1u8, "quiet": false, "seq": 3u8, "since": since, "state": "degraded", "version": built, "window": 600u16});
        same(&rift(&root, &[], &["status"])?, &format!("0\n{degraded}\n"))?;
        fs::rename(&aside, &log)?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":4}\n",
        )?;
        let healed: Value =
            serde_json::from_str(rift(&root, &[], &["status"])?.trim_start_matches("0\n"))?;
        same(&healed["state"].to_string(), r#""ready""#)?;
        let mut held = greet(&root)?;
        same(&signal(&daemon, "TERM")?, "")?;
        let begun = Instant::now();
        while !fs::read_to_string(&log)?.contains("\"stop\"") {
            if begun.elapsed() > Duration::from_secs(10) {
                return Err("no stop event".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
        let stopping: Value = serde_json::from_str(&say(&mut held, r#"{"op":"health"}"#)?)?;
        same(&stopping["state"].to_string(), r#""stopping""#)?;
        same(
            &say(&mut held, r#"{"on":true,"op":"quiet"}"#)?,
            r#"{"code":"stopping","ok":false}"#,
        )?;
        same(
            &say(&mut held, r#"{"op":"shutdown"}"#)?,
            r#"{"code":"stopping","ok":false}"#,
        )?;
        drop(held);
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        same(
            &positions(&events(&root)?),
            &format!(
                "1:1 {{\"kind\":\"boot\",\"pid\":{pid},\"version\":\"{built}\"}}\n1:2 {{\"kind\":\"quiet\",\"on\":true}}\n1:3 {{\"kind\":\"quiet\",\"on\":false}}\n1:4 {{\"kind\":\"quiet\",\"on\":true}}\n1:5 {{\"kind\":\"stop\",\"reason\":\"signal TERM\"}}"
            ),
        )?;
        same(
            &format!(
                "{} {}",
                fs::exists(format!("{root}/run/serve.pid"))?,
                fs::exists(format!("{root}/run/rift.sock"))?
            ),
            "false false",
        )?;
        let logged: Vec<Value> = fs::read_to_string(format!("{root}/run/serve.log"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        same(
            &logged
                .iter()
                .map(|line| {
                    let mut bare = line.clone();
                    drop(bare.as_object_mut().and_then(|map| map.remove("at")));
                    bare.to_string()
                })
                .collect::<Vec<_>>()
                .join("\n"),
            &format!(
                "{{\"event\":\"start\",\"pid\":{pid},\"version\":\"{built}\"}}\n{{\"event\":\"refused\",\"reason\":\"already running (pid {pid}, by its pid file)\"}}\n{{\"event\":\"degraded\",\"reason\":\"Operation not permitted (os error 1)\"}}\n{{\"event\":\"exit\",\"reason\":\"signal TERM\"}}"
            ),
        )
    }

    /// `kill -<signal>` to `daemon`, not waiting: `kill`'s stderr, empty when delivered.
    fn signal(daemon: &Daemon, name: &str) -> Result<String, Box<dyn Error>> {
        let out = bounded(
            Command::new("kill").args([format!("-{name}"), daemon.0.id().to_string()]),
            "kill",
        )?;
        Ok(String::from_utf8(out.stderr)?)
    }

    /// An append the disk fills partway through (a full HFS+ image of 4 KiB blocks, 10 bytes
    /// left in the log's last one) is answered `io` and cut back, the log byte for byte what it
    /// was; once there is room, the next append is a whole line after it.
    #[test]
    fn torn() -> Outcome {
        let dir = temp("torn")?;
        let (root, vol) = (format!("{dir}/root"), format!("{dir}/vol"));
        let image = format!("{dir}/disk.dmg");
        for args in [
            &[
                "create", "-size", "1m", "-fs", "HFS+", "-fsargs", "-b 4096", &image,
            ][..],
            &["attach", "-nobrowse", "-mountpoint", &vol, &image],
        ] {
            let out = bounded(Command::new("hdiutil").args(args), "hdiutil")?;
            same(&out.status.success().to_string(), "true")?;
        }
        let _volume = Volume(vol.clone());
        let (_daemon, _) = serve(&root, &[])?;
        let (log, disk) = (
            format!("{root}/events.ndjson"),
            format!("{vol}/events.ndjson"),
        );
        let mut kept = fs::read(&log)?;
        // the boot line, padded to 10 bytes short of its first block
        kept.resize(4086, b'\n');
        fs::write(&disk, &kept)?;
        fs::remove_file(&log)?;
        symlink(&disk, &log)?;
        let mut fill = File::create(format!("{vol}/fill"))?;
        for chunk in [0x1_0000, 0x1000] {
            let bytes = vec![0; chunk];
            while matches!(fill.write(&bytes), Ok(1..)) {}
        }
        drop(fill);
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "1\n{\"code\":\"io\",\"ok\":false}\n",
        )?;
        same(&(fs::read(&disk)? == kept).to_string(), "true")?;
        fs::remove_file(format!("{vol}/fill"))?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":2}\n",
        )?;
        let after = fs::read(&disk)?;
        let line: Value = serde_json::from_slice(after.strip_prefix(&*kept).ok_or("rewritten")?)?;
        same(&positions(&[line]), r#"1:2 {"kind":"quiet","on":true}"#)
    }

    /// SIGTERM, SIGINT and SIGHUP each drain to exit 0 and append `stop` with the signal's name,
    /// and so does the shutdown request; `boot` counts each start; a connection left open is cut
    /// once the drain budget is spent; the diagnostic log and launchd's `serve.err` rotate at
    /// their cap.
    #[test]
    fn stops() -> Outcome {
        let root = temp("stops")?;
        let envs = [("RIFT_DRAIN_MS", "200"), ("RIFT_LOG_CAP", "1")];
        fs::write(format!("{root}/serve.err"), "launchd's")?;
        for (boot, name) in ["TERM", "INT", "HUP"].into_iter().enumerate() {
            let (mut daemon, ready) = serve(&root, &envs)?;
            same(&ready["boot"].to_string(), &(boot + 1).to_string())?;
            let mut idle = greet(&root)?;
            let begun = Instant::now();
            same(&signal(&daemon, name)?, "")?;
            same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
            same(
                &(begun.elapsed() >= Duration::from_millis(200)).to_string(),
                "true",
            )?;
            same(&next(&mut idle)?, "")?;
            let last = events(&root)?.pop().ok_or("no events")?;
            same(
                &last["e"].to_string(),
                &format!(r#"{{"kind":"stop","reason":"signal {name}"}}"#),
            )?;
        }
        let (mut daemon, ready) = serve(&root, &envs)?;
        same(&ready["boot"].to_string(), "4")?;
        same(
            &say(&mut greet(&root)?, r#"{"id":"x","op":"shutdown"}"#)?,
            r#"{"id":"x","ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        let last = events(&root)?.pop().ok_or("no events")?;
        same(
            &last["e"].to_string(),
            r#"{"kind":"stop","reason":"shutdown request"}"#,
        )?;
        let tail = |file: &str| -> Result<String, Box<dyn Error>> {
            let line: Value =
                serde_json::from_str(fs::read_to_string(format!("{root}/run/{file}"))?.trim())?;
            Ok(format!("{} {}", line["event"], line["reason"]))
        };
        same(&tail("serve.log")?, r#""exit" "shutdown request""#)?;
        same(&tail("serve.log.1")?, r#""start" null"#)?;
        same(
            &format!(
                "{} {}",
                fs::read_to_string(format!("{root}/serve.err.1"))?,
                fs::metadata(format!("{root}/serve.err"))?.len()
            ),
            "launchd's 0",
        )
    }

    /// A stop signal during the boot (here, sent before the long log is read, the pid file not
    /// yet written) is held until the daemon is up, then drains like any other: exit 0, the
    /// `stop` event, the exit line.
    #[test]
    fn booting() -> Outcome {
        let root = temp("booting")?;
        let line = format!(
            "{}\n",
            json!({"boot": 1u8, "e": {"kind": "quiet", "on": false}, "seq": 1u8, "t": "2026-11-22T18:09:40.118Z"})
        );
        fs::write(format!("{root}/events.ndjson"), line.repeat(200_000))?;
        let mut daemon = Daemon(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .arg("serve")
                .stdout(Stdio::null())
                .spawn()?,
        );
        // frozen once the socket is bound, before the log is read: the TERM is pending mid-boot
        let begun = Instant::now();
        while !fs::exists(format!("{root}/run/serve.new"))? {
            if begun.elapsed() > Duration::from_secs(10) {
                return Err("never bound".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
        same(&signal(&daemon, "STOP")?, "")?;
        same(
            &fs::exists(format!("{root}/run/serve.pid"))?.to_string(),
            "false",
        )?;
        same(&signal(&daemon, "TERM")?, "")?;
        same(&signal(&daemon, "CONT")?, "")?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        same(
            &events(&root)?.pop().ok_or("no events")?["e"].to_string(),
            r#"{"kind":"stop","reason":"signal TERM"}"#,
        )?;
        same(
            &fs::read_to_string(format!("{root}/run/serve.log"))?
                .contains(r#""event":"exit","reason":"signal TERM""#)
                .to_string(),
            "true",
        )
    }

    /// A SIGKILL leaves a stale pid file and socket and, here, a torn last line; the next boot
    /// cuts the line, counts boot 2 and keeps quiet; feeds replay from null, resume from a
    /// position across the restart, reset on a position the log does not hold, go live, and end
    /// with `stop`; `rift feed` is a real client of the same; a third boot takes the last quiet.
    #[test]
    fn recover() -> Outcome {
        let root = temp("recover")?;
        let (mut daemon, _) = serve(&root, &[])?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":2}\n",
        )?;
        daemon.0.kill()?;
        let _: ExitStatus = daemon.0.wait()?;
        same(
            &format!(
                "{} {}",
                fs::exists(format!("{root}/run/serve.pid"))?,
                fs::exists(format!("{root}/run/rift.sock"))?
            ),
            "true true",
        )?;
        OpenOptions::new()
            .append(true)
            .open(format!("{root}/events.ndjson"))?
            .write_all(br#"{"boot":1,"e":{"ki"#)?;
        let (mut again, ready) = serve(&root, &[])?;
        same(
            &format!("{} {} {}", ready["boot"], ready["quiet"], ready["seq"]),
            "2 true 1",
        )?;
        let pid = ready["pid"].clone();
        let built = version()?;
        let boot2 = format!("2:1 {{\"kind\":\"boot\",\"pid\":{pid},\"version\":\"{built}\"}}");
        same(
            &positions(&events(&root)?),
            &format!(
                "1:1 {{\"kind\":\"boot\",\"pid\":{},\"version\":\"{built}\"}}\n1:2 {{\"kind\":\"quiet\",\"on\":true}}\n{boot2}",
                events(&root)?.first().ok_or("empty")?["e"]["pid"]
            ),
        )?;
        let mut feeds = Vec::new();
        for (after, reset, replay) in [
            ("null", false, boot2.clone()),
            (
                r#"{"boot":1,"seq":1}"#,
                false,
                format!("1:2 {{\"kind\":\"quiet\",\"on\":true}}\n{boot2}"),
            ),
            (r#"{"boot":9,"seq":9}"#, true, boot2.clone()),
            (r#"{"boot":2,"seq":1}"#, false, String::new()),
        ] {
            let mut conn = greet(&root)?;
            same(
                &say(&mut conn, &format!(r#"{{"after":{after},"op":"feed"}}"#))?,
                &format!(r#"{{"ok":true,"reset":{reset}}}"#),
            )?;
            feeds.push((conn, replay));
        }
        let cli = |after: &str| -> io::Result<Daemon> {
            Ok(Daemon(
                Command::new(env!("CARGO_BIN_EXE_rift"))
                    .env("RIFT_ROOT", &root)
                    .args(["feed", "--after", after])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?,
            ))
        };
        same(
            &rift(&root, &[], &["quiet", "off"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":2}\n",
        )?;
        // a CLI feed has subscribed once its first line arrives
        let (mut held, mut lost) = (cli("2:1")?, cli("9:9")?);
        let mut outs = Vec::new();
        for child in [&mut held, &mut lost] {
            let mut out = BufReader::new(child.0.stdout.take().ok_or("stdout")?);
            let mut first = String::new();
            let _: usize = out.read_line(&mut first)?;
            outs.push((first, out));
        }
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        let rest = "2:2 {\"kind\":\"quiet\",\"on\":false}\n2:3 {\"kind\":\"stop\",\"reason\":\"shutdown request\"}";
        for (mut conn, replay) in feeds {
            same(
                &positions(&drain(&mut conn)?),
                &[replay.as_str(), rest]
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n"),
            )?;
        }
        let mut printed = Vec::new();
        for (first, out) in outs {
            let lines: Vec<Value> = iter::once(first)
                .chain(out.lines().map_while(Result::ok))
                .map(|line| serde_json::from_str(&line))
                .collect::<Result<_, _>>()?;
            printed.push(positions(&lines));
        }
        same(
            &printed.join("\n--\n"),
            &format!("{rest}\n--\n{boot2}\n{rest}"),
        )?;
        let mut said = String::new();
        let _: usize =
            io::Read::read_to_string(&mut lost.0.stderr.take().ok_or("stderr")?, &mut said)?;
        same(
            &said,
            "rift feed: the log does not hold that position; from this boot's first event\n",
        )?;
        same(
            &format!(
                "{:?} {:?} {:?}",
                held.0.wait()?.code(),
                lost.0.wait()?.code(),
                again.0.wait()?.code()
            ),
            "Some(0) Some(0) Some(0)",
        )?;
        let (_last, third) = serve(&root, &[])?;
        same(&format!("{} {}", third["boot"], third["quiet"]), "3 false")
    }

    /// Running out of descriptors skips connections, never stops the daemon. A connection holds
    /// three, so of three consecutive fd limits one leaves none spare with dials still queued and
    /// `accept` itself fails (EMFILE); the limits climb until serve.log shows that `accept` line,
    /// and each serves on after.
    #[test]
    fn exhaust() -> Outcome {
        for limit in 28u8..=36 {
            let root = temp(&format!("exhaust{limit}"))?;
            let mut daemon = Daemon(
                Command::new("sh")
                    .args(["-c", &format!(r#"ulimit -n {limit}; exec "$0" serve"#)])
                    .arg(env!("CARGO_BIN_EXE_rift"))
                    .env("RIFT_ROOT", &root)
                    .stdout(Stdio::piped())
                    .spawn()?,
            );
            let mut ready = String::new();
            let _: usize =
                BufReader::new(daemon.0.stdout.take().ok_or("stdout")?).read_line(&mut ready)?;
            let held = iter::repeat_with(|| dial(&root))
                .take(100)
                .collect::<io::Result<Vec<_>>>()?;
            thread::sleep(Duration::from_millis(300));
            drop(held);
            thread::sleep(Duration::from_millis(300));
            same(
                &format!("{limit} {:?}", daemon.0.try_wait()?),
                &format!("{limit} None"),
            )?;
            // serving on: it answers ready once it has closed what the dials left, however long a
            // busy machine takes to
            until(|| {
                let mut conn = greet(&root).map_err(|cause| io::Error::other(cause.to_string()));
                Ok(conn.as_mut().is_ok_and(|open| {
                    say(open, r#"{"op":"health"}"#)
                        .is_ok_and(|line| line.contains(r#""state":"ready""#))
                }))
            })?;
            same(&signal(&daemon, "TERM")?, "")?;
            same(
                &format!("{limit} {:?}", daemon.0.wait()?.code()),
                &format!("{limit} Some(0)"),
            )?;
            if fs::read_to_string(format!("{root}/run/serve.log"))?.contains(r#""event":"accept""#)
            {
                return Ok(());
            }
        }
        Err("no fd limit in 28..=36 logged an accept failure".into())
    }

    /// A feed whose client hangs up is closed at once, not at the next event: twenty feeds opened
    /// and dropped in turn with no event between fit under a 30-descriptor limit.
    #[test]
    fn hangup() -> Outcome {
        let root = temp("hangup")?;
        let mut daemon = Daemon(
            Command::new("sh")
                .args(["-c", r#"ulimit -n 30; exec "$0" serve"#])
                .arg(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .stdout(Stdio::piped())
                .spawn()?,
        );
        let mut ready = String::new();
        let _: usize =
            BufReader::new(daemon.0.stdout.take().ok_or("stdout")?).read_line(&mut ready)?;
        for _ in 0u8..20 {
            let mut feed = greet(&root)?;
            same(
                &say(&mut feed, r#"{"after":null,"op":"feed"}"#)?,
                r#"{"ok":true,"reset":false}"#,
            )?;
            drop(next(&mut feed)?);
            drop(feed);
            thread::sleep(Duration::from_millis(50));
        }
        same(&signal(&daemon, "TERM")?, "")?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A feed subscribed while another connection toggles quiet sees every event once, in order:
    /// the replay meets the live events with no gap and no duplicate.
    #[test]
    fn seamless() -> Outcome {
        let root = temp("seamless")?;
        let (mut daemon, _) = serve(&root, &[])?;
        let (halfway, waited) = mpsc::channel();
        let mut toggling = greet(&root)?;
        // the feed subscribes a quarter of the way through the toggles; the scope waits out the rest
        let mut feed = thread::scope(|scope| -> Result<_, Box<dyn Error>> {
            drop(scope.spawn(move || {
                for (turn, on) in [true, false].into_iter().cycle().take(400).enumerate() {
                    if turn == 100 {
                        halfway.send(()).unwrap_or_default();
                    }
                    drop(say(
                        &mut toggling,
                        &format!(r#"{{"on":{on},"op":"quiet"}}"#),
                    ));
                }
            }));
            waited.recv()?;
            let mut feed = greet(&root)?;
            same(
                &say(&mut feed, r#"{"after":null,"op":"feed"}"#)?,
                r#"{"ok":true,"reset":false}"#,
            )?;
            Ok(feed)
        })?;
        same(&signal(&daemon, "TERM")?, "")?;
        let seqs: Vec<String> = drain(&mut feed)?
            .iter()
            .map(|event| event["seq"].to_string())
            .collect();
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        same(
            &seqs.join(","),
            &(1..=402)
                .map(|seq| seq.to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    /// A feed reads the log off the lock: with the log a fifo whose read never ends, a feed stuck
    /// replaying it leaves `status` answering; once the fifo ends, its reply carries its `id`.
    #[test]
    fn unlocked() -> Outcome {
        let root = temp("unlocked")?;
        let (mut daemon, _) = serve(&root, &[])?;
        let log = format!("{root}/events.ndjson");
        fs::rename(&log, format!("{log}.held"))?;
        let mut status = greet(&root)?;
        status
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(2)))?;
        same(
            &format!(
                "{}",
                bounded(Command::new("mkfifo").arg(&log), "mkfifo")?.status
            ),
            "exit status: 0",
        )?;
        let mut feed = greet(&root)?;
        feed.get_mut()
            .write_all(b"{\"after\":null,\"id\":7,\"op\":\"feed\"}\n")?;
        // opening the fifo to write returns once the feed has opened it to read
        let writer = OpenOptions::new().write(true).open(&log)?;
        let health: Value = serde_json::from_str(&say(&mut status, r#"{"op":"health"}"#)?)?;
        same(&health["ok"].to_string(), "true")?;
        drop(writer);
        same(&next(&mut feed)?, r#"{"id":7,"ok":true,"reset":false}"#)?;
        fs::remove_file(&log)?;
        fs::rename(format!("{log}.held"), &log)?;
        same(&signal(&daemon, "TERM")?, "")?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A feed client that stops reading is dropped once it is more than 1024 events behind; the
    /// daemon serves on, and the client resumes from its last position.
    #[test]
    fn slow() -> Outcome {
        let root = temp("slow")?;
        let (mut daemon, _) = serve(&root, &[])?;
        let mut feed = greet(&root)?;
        same(
            &say(&mut feed, r#"{"after":null,"op":"feed"}"#)?,
            r#"{"ok":true,"reset":false}"#,
        )?;
        let mut conn = greet(&root)?;
        for on in [true, false].into_iter().cycle().take(5000) {
            drop(say(&mut conn, &format!(r#"{{"on":{on},"op":"quiet"}}"#))?);
        }
        let got = drain(&mut feed)?;
        let last = got.last().ok_or("nothing fed")?;
        same(&(got.len() < 5001).to_string(), "true")?;
        let mut again = greet(&root)?;
        same(
            &say(
                &mut again,
                &format!(
                    r#"{{"after":{{"boot":1,"seq":{}}},"op":"feed"}}"#,
                    last["seq"]
                ),
            )?,
            r#"{"ok":true,"reset":false}"#,
        )?;
        same(&signal(&daemon, "TERM")?, "")?;
        let rest = drain(&mut again)?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        same(
            &format!("{} {}", rest.first().ok_or("no resume")?["seq"], rest.len()),
            &format!(
                "{} {}",
                last["seq"].as_u64().unwrap_or_default() + 1,
                5002 - last["seq"].as_u64().unwrap_or_default()
            ),
        )
    }

    /// The stream: refused requests; two devices whose clocks are three hours apart placed on the
    /// daemon's one timeline; a second device on a live track taken; closed ears refusing frames
    /// until opened, each device told; segments cut at their length, a gap a hole, a drop closing
    /// the open segment, each with its event and file; a listener fed each frame live; a range
    /// read with silence for the hole; a failed segment write answered `io`; closing the ears
    /// closing open segments; the window moving closed segments to the archive with an index line
    /// each, a range that has left it gone, then deleting them with no archive; ears, window and
    /// archive kept across a restart.
    #[test]
    fn stream() -> Outcome {
        let root = temp("stream")?;
        let archive = format!("{root}/archive");
        let envs = [("RIFT_DRAIN_MS", "100"), ("RIFT_SEGMENT_MS", "100")];
        let (mut daemon, ready) = serve(&root, &envs)?;
        same(
            &format!(
                "{} {} {} {}",
                ready["archive"], ready["devices"], ready["ears"], ready["window"]
            ),
            "null [] false 600",
        )?;
        let mut feed = greet(&root)?;
        same(
            &say(&mut feed, r#"{"after":null,"op":"feed"}"#)?,
            r#"{"ok":true,"reset":false}"#,
        )?;
        drop(take(&mut feed, 1)?);
        let mut conn = greet(&root)?;
        for (line, reply) in [
            (r#"{"kind":"nose","name":"a","op":"device"}"#, "bad_request"),
            (
                r#"{"kind":"ear","name":"../a","op":"device"}"#,
                "bad_request",
            ),
            (r#"{"op":"frame","pcm":"00","t":1}"#, "bad_request"),
            (r#"{"op":"pong","ping":1,"t":1}"#, "bad_request"),
            (r#"{"op":"listen"}"#, "bad_request"),
            (r#"{"op":"listen","track":"ears"}"#, "bad_request"),
            (
                r#"{"from":2,"op":"read","to":1,"track":"ear-a"}"#,
                "bad_request",
            ),
            (r#"{"from":1,"op":"read","to":2,"track":"ear-a"}"#, "gone"),
            (
                r#"{"archive":"here","op":"keep","window":1}"#,
                "bad_request",
            ),
            (r#"{"on":1,"op":"ears"}"#, "bad_request"),
        ] {
            same(&say(&mut conn, line)?, &code(reply))?;
        }
        // one clock an hour ahead of the daemon's, one two hours behind
        let (mut ear, hi) = device(
            &root,
            r#"{"kind":"ear","name":"a","op":"device"}"#,
            3_600_000_000,
        )?;
        let (mut mouth, mouth_hi) = device(
            &root,
            r#"{"kind":"mouth","name":"b","op":"device"}"#,
            -7_200_000_000,
        )?;
        same(
            &format!("{} {}", hi["ears"], hi["track"]),
            r#"false "ear-a""#,
        )?;
        same(
            &say(
                &mut greet(&root)?,
                r#"{"kind":"ear","name":"a","op":"device"}"#,
            )?,
            &code("taken"),
        )?;
        same(
            &say(&mut ear, r#"{"after":null,"op":"feed"}"#)?,
            &code("bad_request"),
        )?;
        let status: Value =
            serde_json::from_str(rift(&root, &[], &["status"])?.trim_start_matches("0\n"))?;
        same(&status["devices"].to_string(), r#"["ear-a","mouth-b"]"#)?;
        // each device stamps its frames on its own clock, from two seconds before it connected
        let ear_at = |index: u64| {
            hi["ping"].as_u64().unwrap_or_default() + 3_600_000_000 - 2_000_000 + index * 20_000
        };
        same(&frame(&mut ear, ear_at(0), 0)?.to_string(), &code("closed"))?;
        // a frame before its device's first pong has no place on the timeline
        let mut early = greet(&root)?;
        drop(say(
            &mut early,
            r#"{"kind":"ear","name":"early","op":"device"}"#,
        )?);
        same(&frame(&mut early, 1, 0)?.to_string(), &code("unsynced"))?;
        drop(early);
        same(
            &rift(&root, &[], &["ears", "on"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":2}\n",
        )?;
        same(
            &format!("{} {}", next(&mut ear)?, next(&mut mouth)?),
            r#"{"ears":true} {"ears":true}"#,
        )?;
        same(
            &take(&mut feed, 1)?.to_string(),
            r#"[{"kind":"ears","on":true}]"#,
        )?;
        for line in [
            r#"{"op":"frame","pcm":"00","t":1}"#.to_owned(),
            json!({"op": "frame", "pcm": "zz".repeat(640), "t": 1u8}).to_string(),
            // from_str_radix takes these; the wire is lowercase digits only
            json!({"op": "frame", "pcm": "AB".repeat(640), "t": 1u8}).to_string(),
            json!({"op": "frame", "pcm": "+0".repeat(640), "t": 1u8}).to_string(),
        ] {
            same(&say(&mut ear, &line)?, &code("bad_frame"))?;
        }
        same(
            &say(&mut ear, r#"{"op":"frame","pcm":"00"}"#)?,
            &code("bad_request"),
        )?;
        let mut listen = greet(&root)?;
        same(
            &say(&mut listen, r#"{"op":"listen","track":"mouth-b"}"#)?,
            r#"{"ok":true}"#,
        )?;
        // eight frames, a second of them missing, three more; then the device drops
        let mut placed = Vec::new();
        for index in (0..8).chain(58..61) {
            placed.push(frame(&mut ear, ear_at(index), u8::try_from(index)?)?["t"].clone());
        }
        drop(ear);
        let first = placed[0].as_u64().ok_or("unplaced")?;
        same(
            &(first.abs_diff(hi["ping"].as_u64().unwrap_or_default() - 2_000_000) < 1_000_000)
                .to_string(),
            "true",
        )?;
        same(
            &json!(placed).to_string(),
            &json!(
                (0..8)
                    .chain(58..61)
                    .map(|index| first + index * 20_000)
                    .collect::<Vec<_>>()
            )
            .to_string(),
        )?;
        let segment = |num: u8, samples: u32, start: u64| json!({"kind": "segment", "n": num, "path": format!("audio/1/ear-a/{num}.pcm"), "samples": samples, "start": start, "track": "ear-a"});
        same(
            &take(&mut feed, 5)?.to_string(),
            &json!([
                {"kind": "track", "start": first, "track": "ear-a"},
                segment(0, 1600, first),
                segment(1, 960, first + 100_000),
                {"kind": "hole", "samples": 16_000u16, "start": first + 160_000, "track": "ear-a"},
                segment(2, 960, first + 1_160_000),
            ])
            .to_string(),
        )?;
        let fills =
            |range: &[u8]| -> Vec<u8> { range.iter().flat_map(|&fill| [fill; 640]).collect() };
        for (num, range) in [
            (0u8, &[0, 1, 2, 3, 4][..]),
            (1, &[5, 6, 7]),
            (2, &[58, 59, 60]),
        ] {
            same(
                &(fs::read(format!("{root}/audio/1/ear-a/{num}.pcm"))? == fills(range)).to_string(),
                "true",
            )?;
        }
        // the mouth's frames land beside the ear's, its clock three hours from the ear's
        let mouth_at = mouth_hi["ping"].as_u64().unwrap_or_default() - 7_200_000_000 - 2_000_000;
        let mut heard = Vec::new();
        for index in 0..5 {
            let reply = frame(&mut mouth, mouth_at + index * 20_000, 9)?;
            heard.push(json!({"pcm": "09".repeat(640), "t": reply["t"]}).to_string());
        }
        let mouth_first = serde_json::from_str::<Value>(heard.first().ok_or("unheard")?)?["t"]
            .as_u64()
            .unwrap_or_default();
        same(
            &(mouth_first.abs_diff(first) < 1_000_000).to_string(),
            "true",
        )?;
        same(
            &iter::repeat_with(|| next(&mut listen))
                .take(5)
                .collect::<io::Result<Vec<_>>>()?
                .join("\n"),
            &heard.join("\n"),
        )?;
        same(
            &take(&mut feed, 2)?.to_string(),
            &json!([
                {"kind": "track", "start": mouth_first, "track": "mouth-b"},
                {"kind": "segment", "n": 0u8, "path": "audio/1/mouth-b/0.pcm", "samples": 1600u16, "start": mouth_first, "track": "mouth-b"},
            ])
            .to_string(),
        )?;
        // the whole of the ear's track: its frames, silence where the hole is
        let read = rift(
            &root,
            &[],
            &[
                "read",
                "ear-a",
                &first.to_string(),
                &(first + 1_220_000).to_string(),
            ],
        )?;
        same(
            &read,
            &format!(
                "0\n{}",
                (0u8..61).fold(String::new(), |lines, index| {
                    let fill = if (8..58).contains(&index) { 0 } else { index };
                    let line = json!({"pcm": format!("{fill:02x}").repeat(640), "t": first + u64::from(index) * 20_000});
                    format!("{lines}{line}\n")
                })
            ),
        )?;
        // half a frame in: every frame straddles two of the device's, one across two segments
        let mut halfway = greet(&root)?;
        same(
            &say(
                &mut halfway,
                &json!({"from": first + 10_000, "op": "read", "to": first + 110_000, "track": "ear-a"})
                    .to_string(),
            )?,
            r#"{"ok":true}"#,
        )?;
        same(
            &drain(&mut halfway)?
                .iter()
                .map(|line| line["pcm"].as_str().unwrap_or_default())
                .collect::<String>(),
            &fills(&[0, 1, 2, 3, 4, 5, 6, 7])
                .get(320..3520)
                .ok_or("short")?
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .concat(),
        )?;
        // a segment that cannot be written: every frame is refused and nothing is logged
        fs::write(format!("{root}/audio/1/ear-z"), "")?;
        let (mut broken, broken_hi) =
            device(&root, r#"{"kind":"ear","name":"z","op":"device"}"#, 0)?;
        for index in 0..2 {
            same(
                &frame(
                    &mut broken,
                    broken_hi["ping"].as_u64().unwrap_or_default() + index * 20_000,
                    1,
                )?
                .to_string(),
                &code("io"),
            )?;
        }
        same(
            &events(&root)?
                .iter()
                .filter(|event| event["e"]["track"] == "ear-z")
                .count()
                .to_string(),
            "0",
        )?;
        // a segment written whose track event cannot be logged is taken back
        let (log, aside) = (
            format!("{root}/events.ndjson"),
            format!("{root}/held.ndjson"),
        );
        fs::rename(&log, &aside)?;
        symlink("/dev/urandom", &log)?;
        let (mut unlogged, unlogged_hi) =
            device(&root, r#"{"kind":"ear","name":"y","op":"device"}"#, 0)?;
        same(
            &frame(
                &mut unlogged,
                unlogged_hi["ping"].as_u64().unwrap_or_default(),
                1,
            )?
            .to_string(),
            &code("io"),
        )?;
        // closing the ears unlogged changes nothing, and no device is told
        same(
            &rift(&root, &[], &["ears", "off"])?,
            &format!("1\n{}\n", code("io")),
        )?;
        same(
            &frame(
                &mut broken,
                broken_hi["ping"].as_u64().unwrap_or_default() + 40_000,
                1,
            )?
            .to_string(),
            &code("io"),
        )?;
        fs::rename(&aside, &log)?;
        same(
            &fs::exists(format!("{root}/audio/1/ear-y/0.pcm"))?.to_string(),
            "false",
        )?;
        let state = || -> Result<String, Box<dyn Error>> {
            let health: Value =
                serde_json::from_str(rift(&root, &[], &["status"])?.trim_start_matches("0\n"))?;
            Ok(health["state"].to_string())
        };
        same(&state()?, r#""degraded""#)?;
        // closing the ears closes the open segment, and its device is told
        let (mut open, open_hi) = device(&root, r#"{"kind":"ear","name":"c","op":"device"}"#, 0)?;
        let open_at = open_hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        for index in 0..2 {
            drop(frame(&mut open, open_at + index * 20_000, 3)?);
        }
        same(&state()?, r#""ready""#)?;
        // an open segment reads back too
        let opened = take(&mut feed, 1)?;
        same(&opened[0]["track"].to_string(), r#""ear-c""#)?;
        let open_first = opened[0]["start"].as_u64().unwrap_or_default();
        same(
            &rift(
                &root,
                &[],
                &[
                    "read",
                    "ear-c",
                    &open_first.to_string(),
                    &(open_first + 40_000).to_string(),
                ],
            )?,
            &format!(
                "0\n{}\n{}\n",
                json!({"pcm": "03".repeat(640), "t": open_first}),
                json!({"pcm": "03".repeat(640), "t": open_first + 20_000})
            ),
        )?;
        // a read into the future ends at now: no silence for time not yet heard
        let mut ahead = greet(&root)?;
        same(
            &say(
                &mut ahead,
                &json!({"from": open_first, "op": "read", "to": open_first + 10_000_000, "track": "ear-c"})
                    .to_string(),
            )?,
            r#"{"ok":true}"#,
        )?;
        let last = drain(&mut ahead)?
            .last()
            .and_then(|line| line["t"].as_u64())
            .ok_or("unread")?;
        same(
            &(last < u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?)
                .to_string(),
            "true",
        )?;
        // a segment whose event cannot be logged stays open, for the next close to log
        fs::rename(&log, &aside)?;
        symlink("/dev/urandom", &log)?;
        same(
            &rift(&root, &[], &["ears", "off"])?,
            &format!("1\n{}\n", code("io")),
        )?;
        fs::rename(&aside, &log)?;
        same(
            &rift(&root, &[], &["ears", "off"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":12}\n",
        )?;
        same(&next(&mut open)?, r#"{"ears":false}"#)?;
        same(
            &frame(&mut open, open_at + 40_000, 3)?.to_string(),
            &code("closed"),
        )?;
        let closing = take(&mut feed, 2)?;
        same(
            &format!(
                "{} {} {}",
                closing[0]["kind"], closing[0]["samples"], closing[1]
            ),
            r#""segment" 640 {"kind":"ears","on":false}"#,
        )?;
        // a window of nothing: every closed segment moves to the archive, in log order
        same(
            &rift(&root, &[], &["keep", "0", &archive])?,
            "0\n{\"ok\":true,\"seq\":13}\n",
        )?;
        until(|| Ok(!fs::exists(format!("{root}/audio/1/ear-c/0.pcm"))?))?;
        let indexed: Vec<Value> = fs::read_to_string(format!("{archive}/index.ndjson"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        same(
            &indexed
                .iter()
                .map(|event| event["e"]["path"].as_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(" "),
            "audio/1/ear-a/0.pcm audio/1/ear-a/1.pcm audio/1/ear-a/2.pcm audio/1/mouth-b/0.pcm audio/1/ear-c/0.pcm",
        )?;
        same(
            &format!(
                "{} {}",
                fs::read(format!("{archive}/audio/1/ear-a/0.pcm"))? == fills(&[0, 1, 2, 3, 4]),
                fs::exists(format!("{root}/audio/1/ear-a/0.pcm"))?
            ),
            "true false",
        )?;
        same(
            &rift(
                &root,
                &[],
                &[
                    "read",
                    "ear-a",
                    &first.to_string(),
                    &(first + 20_000).to_string(),
                ],
            )?,
            &format!("1\n{}\n", code("gone")),
        )?;
        // no archive: a segment leaving the window is deleted
        same(
            &rift(&root, &[], &["keep", "0"])?,
            "0\n{\"ok\":true,\"seq\":14}\n",
        )?;
        drop(rift(&root, &[], &["ears", "on"])?);
        same(&next(&mut open)?, r#"{"ears":true}"#)?;
        for index in 0..5 {
            drop(frame(&mut open, open_at + 200_000 + index * 20_000, 4)?);
        }
        until(|| Ok(!fs::exists(format!("{root}/audio/1/ear-c/1.pcm"))?))?;
        same(
            &format!(
                "{} {} {}",
                fs::exists(format!("{root}/audio/1/ear-c/1.pcm"))?,
                fs::exists(format!("{archive}/audio/1/ear-c/1.pcm"))?,
                fs::read_to_string(format!("{archive}/index.ndjson"))?
                    .lines()
                    .count()
            ),
            "false false 5",
        )?;
        same(
            &rift(&root, &[], &["keep", "5", &archive])?,
            "0\n{\"ok\":true,\"seq\":18}\n",
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        // drained, not killed, so its coverage is written
        let (mut restarted, again) = serve(&root, &envs)?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(
            &format!(
                "{} {} {} {:?}",
                again["archive"],
                again["ears"],
                again["window"],
                restarted.0.wait()?.code()
            ),
            &format!("\"{archive}\" true 5 Some(0)"),
        )
    }

    /// An idle daemon still keeps the window: with the ears closed and no device, a closed
    /// segment leaves `audio/` once it ages out.
    #[test]
    fn idle() -> Outcome {
        let root = temp("idle")?;
        let (mut daemon, _) = serve(&root, &[])?;
        drop(rift(&root, &[], &["keep", "1"])?);
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"i","op":"device"}"#, 0)?;
        drop(frame(&mut ear, hi["ping"].as_u64().unwrap_or_default(), 1)?);
        drop(rift(&root, &[], &["ears", "off"])?);
        drop(ear);
        let segment = format!("{root}/audio/1/ear-i/0.pcm");
        until(|| fs::exists(&segment))?;
        until(|| Ok(!fs::exists(&segment)?))?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A fast clock never runs a track ahead: a frame within half a frame of the track's end goes
    /// at the end, one half a frame or more before it is refused `ahead` and writes nothing.
    #[test]
    fn ahead() -> Outcome {
        let root = temp("ahead")?;
        let (mut daemon, _) = serve(&root, &[])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"f","op":"device"}"#, 0)?;
        let at = hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        let placed = [0, 25_000, 28_000, 35_000]
            .into_iter()
            .zip(0u8..)
            .map(|(step, fill)| frame(&mut ear, at + step, fill))
            .collect::<Result<Vec<_>, _>>()?;
        let first = placed[0]["t"].as_u64().ok_or("unplaced")?;
        same(
            &json!(placed).to_string(),
            &json!([
                {"ok": true, "t": first},
                {"ok": true, "t": first + 20_000},
                {"code": "ahead", "ok": false},
                {"ok": true, "t": first + 40_000},
            ])
            .to_string(),
        )?;
        same(
            &(fs::read(format!("{root}/audio/1/ear-f/0.pcm"))?
                == [0u8, 1, 3]
                    .iter()
                    .flat_map(|&fill| [fill; 640])
                    .collect::<Vec<_>>())
            .to_string(),
            "true",
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A late pong moves no frame: the offset moves only as far as a pong's span, from its ping to
    /// its reading, proves it wrong, as a device clock stepped a second ahead does.
    #[test]
    fn late() -> Outcome {
        let root = temp("late")?;
        let (mut daemon, _) = serve(&root, &[("RIFT_PING_MS", "0")])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"l","op":"device"}"#, 0)?;
        let at = hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        let mut placed = Vec::new();
        for (step, wait, skew) in [(0, 60, 0), (20_000, 0, 1_000_000), (1_040_000, 0, 0)] {
            let reply = frame(&mut ear, at + step, 1)?;
            let ping = reply["ping"].as_u64().ok_or("no ping")?;
            thread::sleep(Duration::from_millis(wait));
            let answer = json!({"op": "pong", "ping": ping, "t": ping + skew});
            ear.get_mut().write_all(format!("{answer}\n").as_bytes())?;
            placed.push(reply["t"].as_u64().ok_or("unplaced")?);
        }
        let first = placed.first().copied().unwrap_or_default();
        same(
            &format!(
                "{:?}",
                placed.iter().map(|stamp| stamp - first).collect::<Vec<_>>()
            ),
            "[0, 20000, 40000]",
        )?;
        same(
            &events(&root)?
                .iter()
                .any(|event| event["e"]["kind"] == "hole")
                .to_string(),
            "false",
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A kill mid-segment: the next boot cuts its torn tail to whole frames and closes it with its
    /// event, its start the track's; the range reads back; the boot after finds it closed.
    #[test]
    fn resume() -> Outcome {
        let root = temp("resume")?;
        let (mut daemon, _) = serve(&root, &[])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"k","op":"device"}"#, 0)?;
        let at = hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        let mut placed = Vec::new();
        for fill in 0u8..3 {
            placed.push(frame(&mut ear, at + u64::from(fill) * 20_000, fill)?["t"].clone());
        }
        daemon.0.kill()?;
        let _: ExitStatus = daemon.0.wait()?;
        let segment = format!("{root}/audio/1/ear-k/0.pcm");
        OpenOptions::new()
            .append(true)
            .open(&segment)?
            .write_all(&[9; 100])?;
        let (mut again, ready) = serve(&root, &[])?;
        same(&ready["ears"].to_string(), "true")?;
        let start = placed[0].as_u64().ok_or("unplaced")?;
        same(
            &events(&root)?.pop().ok_or("no events")?["e"].to_string(),
            &json!({"kind": "segment", "n": 0u8, "path": "audio/1/ear-k/0.pcm", "samples": 960u16, "start": start, "track": "ear-k"}).to_string(),
        )?;
        same(&fs::metadata(&segment)?.len().to_string(), "1920")?;
        let mut conn = greet(&root)?;
        same(
            &say(
                &mut conn,
                &json!({"from": start, "op": "read", "to": start + 60_000, "track": "ear-k"})
                    .to_string(),
            )?,
            r#"{"ok":true}"#,
        )?;
        same(
            &drain(&mut conn)?
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            &(0u8..3)
                .map(|fill| {
                    json!({"pcm": format!("{fill:02x}").repeat(640), "t": placed[usize::from(fill)]})
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", again.0.wait()?.code()), "Some(0)")?;
        // drained, not killed, so its coverage is written
        let (mut third, _) = serve(&root, &[])?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", third.0.wait()?.code()), "Some(0)")?;
        same(
            &events(&root)?
                .iter()
                .rev()
                .take(2)
                .map(|event| event["e"]["kind"].to_string())
                .collect::<Vec<_>>()
                .join(" "),
            r#""stop" "boot""#,
        )
    }

    /// A kill in a later segment: a torn segment after a closed one, or after a hole, starts where
    /// that one ended, at its own first frame, never at the track's start.
    #[test]
    fn seam() -> Outcome {
        let root = temp("seam")?;
        let (mut daemon, _) = serve(&root, &[("RIFT_SEGMENT_MS", "100")])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        // each device held to the kill: a drop would close its segment
        let (mut ears, mut torn) = (Vec::new(), Vec::new());
        for (name, steps) in [("h", &[0, 58, 59][..]), ("k", &[0, 1, 2, 3, 4, 5, 6])] {
            let (mut ear, hi) = device(
                &root,
                &json!({"kind": "ear", "name": name, "op": "device"}).to_string(),
                0,
            )?;
            let at = hi["ping"].as_u64().unwrap_or_default() - 2_000_000;
            let placed = steps
                .iter()
                .map(|&step| frame(&mut ear, at + step * 20_000, 1))
                .collect::<Result<Vec<_>, _>>()?;
            torn.push(json!({"kind": "segment", "n": 1u8, "path": format!("audio/1/ear-{name}/1.pcm"), "samples": 640u16, "start": placed[steps.len() - 2]["t"], "track": format!("ear-{name}")}));
            ears.push(ear);
        }
        daemon.0.kill()?;
        let _: ExitStatus = daemon.0.wait()?;
        let (mut again, _) = serve(&root, &[])?;
        let mut logged = events(&root)?;
        same(
            &json!(
                logged
                    .split_off(logged.len() - 2)
                    .iter()
                    .map(|event| &event["e"])
                    .collect::<Vec<_>>()
            )
            .to_string(),
            &json!(torn).to_string(),
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", again.0.wait()?.code()), "Some(0)")
    }

    /// A `*.pcm` under `audio/` that will not open for writing is left as it is: the daemon comes
    /// up, its boot the only event, rather than refusing each relaunch.
    #[test]
    fn unopenable() -> Outcome {
        let root = temp("unopenable")?;
        fs::create_dir_all(format!("{root}/audio/1/mic/0.pcm"))?;
        let (mut daemon, ready) = serve(&root, &[])?;
        same(&ready["boot"].to_string(), "1")?;
        same(
            &events(&root)?
                .iter()
                .map(|event| event["e"]["kind"].to_string())
                .collect::<Vec<_>>()
                .join(" "),
            r#""boot""#,
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// An archive whose disk stalls, a FIFO with no reader where a segment's copy goes: frames
    /// and `status` answer meanwhile, and the copy lands once the disk wakes.
    #[test]
    fn stall() -> Outcome {
        let root = temp("stall")?;
        let archive = format!("{root}/archive");
        let (mut daemon, _) = serve(&root, &[("RIFT_SEGMENT_MS", "20")])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        drop(rift(&root, &[], &["keep", "0", &archive])?);
        let stuck = format!("{archive}/audio/1/ear-s/0.pcm");
        fs::create_dir_all(format!("{archive}/audio/1/ear-s"))?;
        same(
            &bounded(Command::new("mkfifo").arg(&stuck), "mkfifo")?
                .status
                .success()
                .to_string(),
            "true",
        )?;
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"s","op":"device"}"#, 0)?;
        let at = hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        for index in 0..2 {
            same(
                &frame(&mut ear, at + index * 20_000, 1)?["ok"].to_string(),
                "true",
            )?;
        }
        same(
            &serde_json::from_str::<Value>(
                rift(&root, &[], &["status"])?.trim_start_matches("0\n"),
            )?["state"]
                .to_string(),
            r#""ready""#,
        )?;
        let woken = fs::read(&stuck)?;
        same(&woken.len().to_string(), "640")?;
        until(|| {
            Ok(fs::read_to_string(format!("{archive}/index.ndjson"))
                .is_ok_and(|index| index.lines().count() == 2))
        })?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// A frame that fills its segment while the log fails is still placed and answered; its
    /// segment stays open and is logged by the next frame's close, at its own start.
    #[test]
    fn unlogged() -> Outcome {
        let root = temp("unlogged")?;
        let (log, aside) = (format!("{root}/events.ndjson"), format!("{root}/aside"));
        let (mut daemon, _) = serve(&root, &[("RIFT_SEGMENT_MS", "20")])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ear, hi) = device(&root, r#"{"kind":"ear","name":"u","op":"device"}"#, 0)?;
        let at = hi["ping"].as_u64().unwrap_or_default() - 1_000_000;
        let mut placed = Vec::new();
        for index in 0u8..4 {
            if index == 1 {
                fs::rename(&log, &aside)?;
                symlink("/dev/urandom", &log)?;
            }
            let reply = frame(&mut ear, at + u64::from(index) * 20_000, index)?;
            if index == 1 {
                fs::remove_file(&log)?;
                fs::rename(&aside, &log)?;
            }
            same(&reply["ok"].to_string(), "true")?;
            placed.push(reply["t"].as_u64().ok_or("unplaced")?);
        }
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        same(
            &events(&root)?
                .iter()
                .filter(|event| event["e"]["kind"] == "segment")
                .map(|event| {
                    format!(
                        "{} {} {}",
                        event["e"]["n"], event["e"]["samples"], event["e"]["start"]
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            &format!(
                "0 320 {}, 1 640 {}, 2 320 {}",
                placed[0], placed[1], placed[3]
            ),
        )
    }

    /// `rift device`: refused while the ears are closed, each refusal printed; sent as fast as
    /// accepted, a ping answered on every frame, its frames contiguous segments ending by now, a
    /// short last frame dropped; paced in real time; a missing file; a refused device request; a
    /// live `rift listen` beside a device told the ears closed, then cut by the drain: `unknown`,
    /// exit 1, while the listener ends cleanly.
    #[test]
    fn play() -> Outcome {
        let root = temp("play")?;
        let envs = [
            ("RIFT_DRAIN_MS", "200"),
            ("RIFT_PING_MS", "0"),
            ("RIFT_SEGMENT_MS", "40"),
        ];
        let (mut daemon, _) = serve(&root, &envs)?;
        let mut feed = greet(&root)?;
        same(
            &say(&mut feed, r#"{"after":null,"op":"feed"}"#)?,
            r#"{"ok":true,"reset":false}"#,
        )?;
        drop(take(&mut feed, 1)?);
        let file = format!("{root}/five.pcm");
        fs::write(
            &file,
            (0u8..5)
                .flat_map(|fill| [fill; 640])
                .chain([7; 10])
                .collect::<Vec<_>>(),
        )?;
        let refused = rift(&root, &[], &["device", "ear", "cli", &file, "--fast"])?;
        same(
            &refused
                .lines()
                .filter(|&line| line != "0")
                .skip(1)
                .collect::<Vec<_>>()
                .join("\n"),
            &[
                code("closed"),
                code("closed"),
                code("closed"),
                code("closed"),
                code("closed"),
            ]
            .join("\n"),
        )?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let fast = rift(&root, &[], &["device", "ear", "cli", &file, "--fast"])?;
        same(
            &format!(
                "{} {}",
                fast.lines().count(),
                fast.contains(r#""track":"ear-cli""#)
            ),
            "2 true",
        )?;
        let logged = take(&mut feed, 5)?;
        let start = logged[1]["start"].as_u64().unwrap_or_default();
        same(
            &format!(
                "{} {} {} {}",
                logged[2]["samples"],
                logged[3]["samples"],
                logged[4]["samples"],
                [&logged[3], &logged[4]]
                    .iter()
                    .zip([start + 40_000, start + 80_000])
                    .all(|(event, at)| event["start"] == at)
            ),
            "640 640 320 true",
        )?;
        same(
            &(start + 100_000
                <= u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?)
            .to_string(),
            "true",
        )?;
        let begun = Instant::now();
        same(
            &rift(&root, &[], &["device", "mouth", "paced", &file])?
                .lines()
                .count()
                .to_string(),
            "2",
        )?;
        same(
            &(begun.elapsed() >= Duration::from_millis(80)).to_string(),
            "true",
        )?;
        // a mouth's `--mic` is a file it plays, never the microphone
        for absent in ["/nonexistent.pcm", "--mic"] {
            same(
                &rift(&root, &[], &["device", "mouth", "x", absent])?,
                "1\nrift device: No such file or directory (os error 2)\n",
            )?;
        }
        same(
            &rift(&root, &[], &["device", "nose", "x", &file])?,
            &format!("1\n{}\n", code("bad_request")),
        )?;
        // 20 s: still playing when the drain cuts it
        let long = format!("{root}/long.pcm");
        fs::write(&long, vec![1; 640_000])?;
        let spawn = |args: &[&str]| {
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .args(args)
                .stdout(Stdio::piped())
                .spawn()
                .map(Daemon)
        };
        let (mut listener, mut player) = (
            spawn(&["listen", "ear-long"])?,
            spawn(&["device", "ear", "long", &long])?,
        );
        let mut heard = String::new();
        let _: usize =
            BufReader::new(listener.0.stdout.take().ok_or("stdout")?).read_line(&mut heard)?;
        same(
            &serde_json::from_str::<Value>(&heard)?["pcm"].to_string(),
            &format!("\"{}\"", "01".repeat(640)),
        )?;
        drop(rift(&root, &[], &["ears", "off"])?);
        // told before the drain, which then cuts it whether its next write or read fails
        let mut said = BufReader::new(player.0.stdout.take().ok_or("stdout")?).lines();
        let before = said
            .by_ref()
            .map_while(Result::ok)
            .take_while(|line| line != r#"{"ears":false}"#)
            .count();
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(
            &format!(
                "{before} {} {:?} {:?} {:?}",
                said.map_while(Result::ok).last().unwrap_or_default(),
                player.0.wait()?.code(),
                listener.0.wait()?.code(),
                daemon.0.wait()?.code()
            ),
            &format!("1 {} Some(1) Some(0) Some(0)", code("unknown")),
        )
    }

    /// A frame is never placed after it arrived, however far ahead its device's clock; `rift device`
    /// stopped mid-file plays on from the clock, the pause a hole, never a burst behind it.
    #[test]
    fn pause() -> Outcome {
        let root = temp("pause")?;
        let (mut daemon, _) = serve(&root, &[("RIFT_DRAIN_MS", "200")])?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let (mut ahead, hi) = device(&root, r#"{"kind":"ear","name":"ahead","op":"device"}"#, 0)?;
        let placed = frame(
            &mut ahead,
            hi["ping"].as_u64().unwrap_or_default() + 3_600_000_000,
            1,
        )?;
        same(
            &(placed["t"].as_u64().unwrap_or(u64::MAX)
                <= u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?)
            .to_string(),
            "true",
        )?;
        drop(ahead);
        // 20 s: still playing when stopped, then cut by the drain
        let long = format!("{root}/long.pcm");
        fs::write(&long, vec![1; 640_000])?;
        let mut player = Daemon(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .args(["device", "ear", "paused", &long])
                .stdout(Stdio::null())
                .spawn()?,
        );
        let paused = || -> Result<Vec<Value>, Box<dyn Error>> {
            Ok(events(&root)?
                .into_iter()
                .map(|event| event["e"].clone())
                .filter(|event| event["track"] == "ear-paused")
                .collect())
        };
        until(|| Ok(paused().is_ok_and(|held| !held.is_empty())))?;
        same(&signal(&player, "STOP")?, "")?;
        thread::sleep(Duration::from_millis(400));
        same(&signal(&player, "CONT")?, "")?;
        until(|| {
            Ok(paused().is_ok_and(|held| {
                held.iter().any(|event| {
                    event["kind"] == "hole" && event["samples"].as_u64() >= Some(4_800)
                })
            }))
        })?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(
            &format!(
                "{:?} {:?}",
                player.0.wait()?.code(),
                daemon.0.wait()?.code()
            ),
            "Some(1) Some(0)",
        )
    }

    /// `serve` refuses a socket path over 103 bytes and a `run/` others can reach; a panic (the
    /// ready line into a closed stdout) is logged before the process dies.
    #[test]
    fn refuse() -> Outcome {
        let root = temp("refuse")?;
        let deep = format!("{root}/{}", "x".repeat(90));
        same(
            &rift(&deep, &[], &["serve"])?,
            &format!(
                "1\nrift serve: {deep}/run/rift.sock is over a unix socket's 103 bytes; shorten RIFT_ROOT\n"
            ),
        )?;
        let run = format!("{root}/run");
        fs::create_dir_all(&run)?;
        fs::set_permissions(&run, Permissions::from_mode(0o755))?;
        same(
            &rift(&root, &[], &["serve"])?,
            &format!("1\nrift serve: {run} must be this user's, mode 0700\n"),
        )?;
        same(
            &fs::read_to_string(format!("{run}/serve.log"))?
                .contains(&format!(
                    r#""event":"refused","reason":"{run} must be this user's, mode 0700""#
                ))
                .to_string(),
            "true",
        )?;
        fs::set_permissions(&run, Permissions::from_mode(0o700))?;
        // an unwritable pid file refuses before the boot event and the socket's rename
        fs::create_dir_all(format!("{run}/serve.pid"))?;
        same(
            &format!(
                "{} {} {:?}",
                rift(&root, &[], &["serve"])?,
                fs::exists(format!("{run}/rift.sock"))?,
                fs::read_to_string(format!("{root}/events.ndjson"))?
            ),
            "1\nrift serve: Is a directory (os error 21)\n false \"\"",
        )?;
        fs::remove_dir(format!("{run}/serve.pid"))?;
        let (reader, writer) = io::pipe()?;
        drop(reader);
        let out = Command::new(env!("CARGO_BIN_EXE_rift"))
            .env("RIFT_ROOT", &root)
            .arg("serve")
            .stdout(writer)
            .output()?;
        let log = fs::read_to_string(format!("{run}/serve.log"))?;
        same(
            &format!(
                "{:?} {} {}",
                out.status.code(),
                log.contains(r#","event":"panic","reason":"panicked at "#),
                log.contains("failed printing to stdout")
            ),
            "Some(101) true true",
        )
    }

    /// The clients: down without a daemon, usage on a bad line, the one-line protocol mismatch,
    /// another refused hello printed as it came, a version mismatch named beside the health, a
    /// refused request printed with exit 1, and `unknown` for a request sent but never answered.
    #[test]
    fn clients() -> Outcome {
        let (root, built) = (temp("clients")?, version()?);
        for args in [&["status"][..], &["quiet", "on"], &["feed"]] {
            same(
                &rift(&root, &[], args)?,
                "1\n{\"code\":\"down\",\"ok\":false}\n",
            )?;
        }
        let usage = "2\nusage: rift serve | status | quiet on|off | ears on|off | keep <window s> [<archive>] | feed [--after <boot>:<seq>] | listen <track> | read <track> <from us> <to us> | play <track> <rate> [<file.pcm>] | hush <track> | device ear|mouth <name> <file.pcm> [--fast] | device ear <name> --mic [<microphone>] | device mouth <name> --speaker | setup [--print | --remove] | upgrade [--check | --back | --retry] | version\n";
        // a play's file is opened before it connects; with none, stdin, here empty
        same(
            &rift(&root, &[], &["play", "mouth-a", "16000", "/nonexistent"])?,
            "1\nrift play: No such file or directory (os error 2)\n",
        )?;
        for args in [&["play", "mouth-a", "16000"][..], &["hush", "mouth-a"]] {
            same(
                &rift(&root, &[], args)?,
                "1\n{\"code\":\"down\",\"ok\":false}\n",
            )?;
        }
        for args in [
            &[][..],
            &["device", "ear", "a"],
            &["play", "mouth-a", "fast"],
            &["keep", "x"],
            &["quiet", "maybe"],
            &["read", "ear-a", "1", "x"],
            &["feed", "--after", "1"],
            &["feed", "--after", "a:1"],
            &["setup", "--now"],
            &["nope"],
        ] {
            same(&rift(&root, &[], args)?, usage)?;
        }
        fake(
            &root,
            &[r#"{"code":"proto","ok":false,"proto":2,"version":"9.9.9-new"}"#],
        )?;
        same(
            &rift(&root, &[], &["status"])?,
            &format!(
                "1\nrift status: the daemon speaks protocol 2 (9.9.9-new), this client protocol 1 ({built}); fix: rift setup, so both are one release\n"
            ),
        )?;
        fake(
            &root,
            &[
                r#"{"ok":true,"version":"0.0.0-old"}"#,
                r#"{"ok":true,"state":"ready"}"#,
            ],
        )?;
        same(
            &rift(&root, &[], &["status"])?,
            &format!(
                "0\n{{\"ok\":true,\"state\":\"ready\"}}\nrift status: the daemon is 0.0.0-old, this client {built}; fix: rift setup\n"
            ),
        )?;
        fake(&root, &[r#"{"code":"bad_request","ok":false}"#])?;
        same(
            &rift(&root, &[], &["status"])?,
            "1\n{\"code\":\"bad_request\",\"ok\":false}\n",
        )?;
        let greeted = format!(r#"{{"ok":true,"version":"{built}"}}"#);
        fake(&root, &[&greeted, r#"{"code":"stopping","ok":false}"#])?;
        same(
            &rift(&root, &[], &["feed"])?,
            "1\n{\"code\":\"stopping\",\"ok\":false}\n",
        )?;
        // a play opened, then the connection ends before its last line: an unknown outcome
        fake(&root, &[&greeted, r#"{"ok":true,"play":1,"seq":2}"#])?;
        same(
            &rift(&root, &[], &["play", "mouth-a", "16000"])?,
            "1\n{\"ok\":true,\"play\":1,\"seq\":2}\n{\"code\":\"unknown\",\"ok\":false}\n",
        )?;
        // the hello answered, the request read, then the connection ends with no reply
        fake(&root, &[&greeted])?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "1\n{\"code\":\"unknown\",\"ok\":false}\n",
        )
    }

    /// `line` sent on `conn`, nothing read.
    fn send(conn: &mut BufReader<UnixStream>, line: &str) -> io::Result<()> {
        conn.get_mut().write_all(format!("{line}\n").as_bytes())
    }

    /// The `e` of every event of `kind` (every kind: `""`) in the log's last `count`, compact, one a
    /// line, its `start` cut: a run's start lies off its report by the ping's half round trip.
    fn kinds(root: &str, kind: &str, count: usize) -> io::Result<String> {
        let held = events(root).map_err(|cause| io::Error::other(cause.to_string()))?;
        Ok(held
            .iter()
            .skip(held.len().saturating_sub(count))
            .filter(|event| kind.is_empty() || event["e"]["kind"] == kind)
            .map(|event| {
                let mut seen = event["e"].clone();
                drop(
                    seen.as_object_mut()
                        .and_then(|fields| fields.remove("start")),
                );
                seen.to_string()
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }

    /// Plays over raw connections, the suite as both giver and mouth (docs/protocol.html#plays):
    /// the refusals (`absent` with no mouth or a file-fed one, `busy`, `quiet`, the bad requests and
    /// frames); a give passed on as given and its reply withheld while the mouth holds
    /// `$RIFT_AHEAD_MS` untaken; runs logged as each ends; a hush answered once the mouth says
    /// `done`, the giver's last line then its end of stream; a play to its end, its mouth's segment
    /// closed at `done`, its frames naming the speaker; a giver gone (`left`), a mouth gone or full
    /// (`mouth`), a mouth that never answers cut after 1 s (`mouth`), quiet set (`quiet`), the
    /// drain (`stop`, before `stop`), and a kill (`lost`, the next boot's, its torn segment keeping
    /// the speaker), plays numbered across boots.
    #[test]
    fn plays() -> Outcome {
        let root = temp("plays")?;
        let (mut daemon, _) = serve(&root, &[("RIFT_AHEAD_MS", "100"), ("RIFT_DRAIN_MS", "400")])?;
        let mut probe = greet(&root)?;
        let play = r#"{"op":"play","rate":16000,"track":"mouth-desk"}"#;
        let zeros =
            |samples: usize| json!({"op": "give", "pcm": "0000".repeat(samples)}).to_string();
        let sound = |num: u8, from: u16, samples: u16| {
            json!({"from": from, "op": "sound", "play": num, "samples": samples, "t": 1_000_000u32})
                .to_string()
        };
        let hush = r#"{"op":"hush","track":"mouth-desk"}"#;
        for (line, reply) in [
            (
                r#"{"kind":"ear","name":"x","op":"device","plays":true}"#,
                code("bad_request"),
            ),
            (
                r#"{"op":"play","rate":16000,"track":"ear-desk"}"#,
                code("bad_request"),
            ),
            (
                r#"{"op":"play","rate":7999,"track":"mouth-desk"}"#,
                code("bad_request"),
            ),
            (play, code("absent")),
            (hush, r#"{"ok":true,"play":null}"#.to_owned()),
            (zeros(1).as_str(), code("bad_request")),
            (r#"{"op":"end"}"#, code("bad_request")),
            (sound(1, 0, 1).as_str(), code("bad_request")),
            (r#"{"op":"done","play":1}"#, code("bad_request")),
        ] {
            same(&say(&mut probe, line)?, &reply)?;
        }
        // a file-fed mouth plays nothing
        let (fed, fed_reply) = device(&root, r#"{"kind":"mouth","name":"desk","op":"device"}"#, 0)?;
        same(
            &format!("{} {}", fed_reply["plays"], say(&mut probe, play)?),
            &format!("null {}", code("absent")),
        )?;
        drop(fed);
        until(|| {
            let mut conn = greet(&root).map_err(|cause| io::Error::other(cause.to_string()))?;
            Ok(!say(&mut conn, r#"{"op":"health"}"#)?.contains("mouth-desk"))
        })?;
        // a playing mouth's report before its pong has no place yet; play and hush are not a device's
        let mut early = greet(&root)?;
        let early_reply: Value = serde_json::from_str(&say(
            &mut early,
            r#"{"kind":"mouth","name":"early","op":"device","plays":true}"#,
        )?)?;
        same(
            &format!(
                "{} {} {} {}",
                early_reply["plays"],
                say(&mut early, &sound(1, 0, 1))?,
                say(&mut early, play)?,
                say(&mut early, hush)?
            ),
            &format!(
                "true {} {} {}",
                code("unsynced"),
                code("bad_request"),
                code("bad_request")
            ),
        )?;
        drop(early);
        let mouth_line = r#"{"kind":"mouth","name":"desk","op":"device","plays":true}"#;
        let (mut desk, _) = device(&root, mouth_line, 0)?;
        drop(rift(&root, &[], &["ears", "on"])?);
        same(&next(&mut desk)?, r#"{"ears":true}"#)?;
        // play 1: given, paced, reported, hushed
        let mut giver = greet(&root)?;
        let first: Value = serde_json::from_str(&say(&mut giver, play)?)?;
        same(
            &format!("{} {}", first["play"], say(&mut greet(&root)?, play)?),
            &format!("1 {}", code("busy")),
        )?;
        for bad in ["abc", "ABCD", ""] {
            same(
                &say(&mut giver, &json!({"op": "give", "pcm": bad}).to_string())?,
                &code("bad_frame"),
            )?;
        }
        same(&say(&mut giver, &zeros(800))?, r#"{"given":800,"ok":true}"#)?;
        let passed = format!(
            r#"{{"pcm":"{}","play":1,"rate":16000}}"#,
            "0000".repeat(800)
        );
        same(&next(&mut desk)?, &passed)?;
        // 1600 untaken, the 100 ms the mouth may hold at 16 kHz: withheld until it reports
        send(&mut giver, &zeros(800))?;
        same(&next(&mut desk)?, &passed)?;
        giver
            .get_ref()
            .set_read_timeout(Some(Duration::from_millis(200)))?;
        same(&next(&mut giver).is_err().to_string(), "true")?;
        giver
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(10)))?;
        send(&mut desk, &sound(1, 0, 320))?;
        same(&next(&mut giver)?, r#"{"given":1600,"ok":true}"#)?;
        same(
            &say(&mut giver, r#"{"op":"health"}"#)?,
            &code("bad_request"),
        )?;
        // the run grows, then a new one starts and the first is logged; a report for another play
        // is no one's
        for line in [
            sound(1, 0, 640),
            sound(1, 1000, 100),
            sound(9, 0, 1),
            r#"{"op":"done","play":9}"#.to_owned(),
        ] {
            send(&mut desk, &line)?;
        }
        let health: Value = serde_json::from_str(&say(&mut probe, r#"{"op":"health"}"#)?)?;
        same(&health["playing"].to_string(), r#"{"mouth-desk":1}"#)?;
        send(&mut probe, hush)?;
        same(&next(&mut desk)?, r#"{"hush":1}"#)?;
        send(&mut desk, r#"{"op":"done","play":1}"#)?;
        let hushed: Value = serde_json::from_str(&next(&mut probe)?)?;
        same(
            &format!("{hushed} {} {}", next(&mut giver)?, next(&mut giver)?),
            &format!(
                r#"{{"dropped":500,"given":1600,"ok":true,"play":1,"played":1100,"reason":"hush","seq":{0},"track":"mouth-desk"}} {{"dropped":500,"given":1600,"play":1,"played":1100,"reason":"hush","seq":{0},"track":"mouth-desk"}} "#,
                hushed["seq"]
            ),
        )?;
        same(
            &format!(
                "{}\n{}",
                kinds(&root, "sound", usize::MAX)?,
                kinds(&root, "played", usize::MAX)?
            ),
            r#"{"from":0,"kind":"sound","play":1,"samples":640,"track":"mouth-desk"}
{"from":1000,"kind":"sound","play":1,"samples":100,"track":"mouth-desk"}
{"dropped":500,"given":1600,"kind":"played","play":1,"played":1100,"reason":"hush","track":"mouth-desk"}"#,
        )?;
        // a run's start: the report's on the daemon's clock, uncapped
        same(
            &events(&root)?
                .iter()
                .any(|event| {
                    event["e"]["kind"] == "sound" && event["e"]["start"].as_u64() >= Some(1_000_000)
                })
                .to_string(),
            "true",
        )?;
        // play 2 to its end, a frame naming the speaker in its segment, closed at `done`
        let mut second = greet(&root)?;
        drop(say(&mut second, play)?);
        same(
            &say(&mut second, &zeros(320))?,
            r#"{"given":320,"ok":true}"#,
        )?;
        drop(next(&mut desk)?);
        let framed = json!({"op": "frame", "pcm": "00".repeat(640), "speaker": "Desk Speakers", "t": now_us()});
        same(
            &say(&mut desk, &framed.to_string())?
                .contains(r#""ok":true"#)
                .to_string(),
            "true",
        )?;
        same(&say(&mut second, r#"{"op":"end"}"#)?, r#"{"ok":true}"#)?;
        same(
            &format!(
                "{} {}",
                next(&mut desk)?,
                say(&mut second, r#"{"op":"end"}"#)?
            ),
            &format!(r#"{{"end":2}} {}"#, code("bad_request")),
        )?;
        send(&mut desk, &sound(2, 0, 320))?;
        send(&mut desk, r#"{"op":"done","play":2}"#)?;
        same(
            &next(&mut second)?
                .contains(r#""dropped":0,"given":320,"play":2,"played":320,"reason":"end""#)
                .to_string(),
            "true",
        )?;
        same(
            &events(&root)?
                .iter()
                .filter(|event| {
                    event["e"]["track"] == "mouth-desk"
                        && matches!(event["e"]["kind"].as_str(), Some("track" | "segment"))
                })
                .map(|event| format!("{} {}", event["e"]["kind"], event["e"]["speaker"]))
                .collect::<Vec<_>>()
                .join(" "),
            r#""track" "Desk Speakers" "segment" "Desk Speakers""#,
        )?;
        // play 3: its giver gone before `end` hushes it
        let mut third = greet(&root)?;
        drop(say(&mut third, play)?);
        drop(third);
        same(&next(&mut desk)?, r#"{"hush":3}"#)?;
        send(&mut desk, r#"{"op":"done","play":3}"#)?;
        until(|| {
            Ok(kinds(&root, "played", usize::MAX)?
                .contains(r#""play":3,"played":0,"reason":"left""#))
        })?;
        // play 4: a mouth that never answers a hush is cut after 1 s
        let mut fourth = greet(&root)?;
        drop(say(&mut fourth, play)?);
        drop(say(&mut fourth, &zeros(1))?);
        drop(next(&mut desk)?);
        let begun = Instant::now();
        let cut: Value = serde_json::from_str(&say(&mut probe, hush)?)?;
        same(
            &format!(
                "{} {} {}",
                cut["reason"],
                begun.elapsed() >= Duration::from_secs(1),
                next(&mut desk)?
            ),
            r#""mouth" true {"hush":4}"#,
        )?;
        same(&next(&mut desk)?, "")?;
        same(
            &next(&mut fourth)?
                .contains(r#""reason":"mouth""#)
                .to_string(),
            "true",
        )?;
        // play 5: its giver gone after `end` leaves it playing; then the mouth gone mid-play
        let (gone, _) = device(&root, mouth_line, 0)?;
        let mut fifth = greet(&root)?;
        drop(say(&mut fifth, play)?);
        drop(say(&mut fifth, &zeros(1))?);
        same(&say(&mut fifth, r#"{"op":"end"}"#)?, r#"{"ok":true}"#)?;
        drop(fifth);
        same(
            &say(&mut greet(&root)?, r#"{"op":"health"}"#)?
                .contains(r#""playing":{"mouth-desk":5}"#)
                .to_string(),
            "true",
        )?;
        drop(gone);
        until(|| {
            Ok(kinds(&root, "played", usize::MAX)?
                .contains(r#""play":5,"played":0,"reason":"mouth""#))
        })?;
        // play 6: quiet on hushes it and refuses the next
        let (mut steady, _) = device(&root, mouth_line, 0)?;
        let mut sixth = greet(&root)?;
        drop(say(&mut sixth, play)?);
        drop(rift(&root, &[], &["quiet", "on"])?);
        same(&next(&mut steady)?, r#"{"hush":6}"#)?;
        send(&mut steady, r#"{"op":"done","play":6}"#)?;
        same(
            &next(&mut sixth)?
                .contains(r#""reason":"quiet""#)
                .to_string(),
            "true",
        )?;
        same(&say(&mut probe, play)?, &code("quiet"))?;
        drop(rift(&root, &[], &["quiet", "off"])?);
        // the drain: one mouth answers its hush (`stop`), one never does and is ended at its last
        // report once the budget is spent, both before `stop`
        let (mut mute, _) = device(
            &root,
            r#"{"kind":"mouth","name":"mute","op":"device","plays":true}"#,
            0,
        )?;
        let (mut seventh, mut eighth) = (greet(&root)?, greet(&root)?);
        drop(say(&mut seventh, play)?);
        drop(say(
            &mut eighth,
            r#"{"op":"play","rate":24000,"track":"mouth-mute"}"#,
        )?);
        send(
            &mut mute,
            r#"{"from":0,"op":"sound","play":8,"samples":48,"t":5}"#,
        )?;
        drop(say(&mut probe, r#"{"op":"health"}"#)?);
        same(&signal(&daemon, "TERM")?, "")?;
        same(&next(&mut steady)?, r#"{"hush":7}"#)?;
        send(&mut steady, r#"{"op":"done","play":7}"#)?;
        same(&next(&mut mute)?, r#"{"hush":8}"#)?;
        until(|| Ok(kinds(&root, "stop", usize::MAX)?.contains("stop")))?;
        same(
            &kinds(&root, "", 4)?,
            r#"{"dropped":0,"given":0,"kind":"played","play":7,"played":0,"reason":"stop","track":"mouth-desk"}
{"from":0,"kind":"sound","play":8,"samples":48,"track":"mouth-mute"}
{"dropped":0,"given":0,"kind":"played","play":8,"played":48,"reason":"stop","track":"mouth-mute"}
{"kind":"stop","reason":"signal TERM"}"#,
        )?;
        drop((
            steady, mute, seventh, eighth, giver, second, fourth, sixth, probe,
        ));
        // ended by itself, so its lines count
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        // a kill mid-play: the next boot ends it `lost`, its logged runs what it played, and its
        // torn segment keeps the speaker; the next play is numbered past it
        let (killed, _) = serve(&root, &[])?;
        let (mut torn, _) = device(&root, mouth_line, 0)?;
        let mut ninth = greet(&root)?;
        drop(say(&mut ninth, play)?);
        let tearing = json!({"op": "frame", "pcm": "00".repeat(640), "speaker": "Desk Speakers", "t": now_us()});
        drop(say(&mut torn, &tearing.to_string())?);
        for line in [sound(9, 0, 200), sound(9, 200, 10)] {
            send(&mut torn, &line)?;
        }
        until(|| Ok(kinds(&root, "sound", usize::MAX)?.contains(r#""play":9"#)))?;
        drop(killed);
        drop((torn, ninth));
        let (mut again, _) = serve(&root, &[])?;
        same(
            &format!(
                "{}\n{}",
                kinds(&root, "played", usize::MAX)?
                    .lines()
                    .last()
                    .unwrap_or_default(),
                events(&root)?
                    .iter()
                    .find(|event| event["boot"] == 3u8 && event["e"]["kind"] == "segment")
                    .map_or(Value::Null, |event| event["e"]["speaker"].clone())
            ),
            "{\"kind\":\"played\",\"play\":9,\"played\":200,\"reason\":\"lost\",\"track\":\"mouth-desk\"}\n\"Desk Speakers\"",
        )?;
        let (last_mouth, _) = device(&root, mouth_line, 0)?;
        same(
            &say(&mut greet(&root)?, play)?
                .contains(r#""play":10"#)
                .to_string(),
            "true",
        )?;
        drop(last_mouth);
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", again.0.wait()?.code()), "Some(0)")
    }

    /// The daemon's clock now, in microseconds since the epoch.
    fn now_us() -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
        )
        .unwrap_or_default()
    }

    /// `setup --print` prints both plists on hotswap's link, the ear's track named by a stub
    /// `scutil`, the daemon's alone with no `scutil`, and writes nothing; an install refuses a host
    /// name unset, which `--remove` never asks for; an ear disabled is written, not loaded; a track
    /// already held (`held`) has the ear loaded unwaited; without `launchctl` nothing is
    /// written; against a stub `launchctl` (`launchctl.log` its calls and, at each bootstrap, the
    /// root's mode when it is 0700; `loaded/` its jobs, each started as its plist's program; a
    /// bootout letting go one `print` later) over a HOME hotswap already laid out, it boots the
    /// ear out before the daemon and loads it once the daemon is ready, ready once health lists
    /// it, an ear that never connects (`deaf`) reloaded once then named, exit 1, the daemon
    /// serving on; it retires an earlier install (another
    /// label running the link's `serve`), moving its root here while this one is absent and
    /// leaving it otherwise, has hotswap link `~/.local/bin/rift` and load the upgrade agent,
    /// makes the root 0700 before the agent loads, bootstraps a new plist, kickstarts the same one
    /// loaded (its respawn held back a second, as by `ThrottleInterval`), reloads a changed one or
    /// a re-signed release, returns only once the loaded daemon answers health ready, reloads once
    /// a job none answers for past `$RIFT_RELOAD_MS` (`refuse`: a kickstart launchd refuses to
    /// spawn), fails on a failed bootstrap, a job still loaded past `$RIFT_BOOTOUT_MS` or none
    /// ready past `$RIFT_READY_MS` after that reload; `upgrade`
    /// swaps under the manifest, green once the swapped daemon answers with the new version;
    /// `--remove` has hotswap forget the layout and the upgrade agent, then boots the daemon and
    /// any earlier install out and removes their plists and the link.
    #[test]
    fn setup() -> Outcome {
        let root = temp("setup")?;
        let _started = Started(root.clone());
        let (home, bin) = (format!("{root}/home"), format!("{root}/bin"));
        let file = format!("{home}/Library/LaunchAgents/dev.gigantic.rift.plist");
        let exe = format!("{home}/.local/bin/rift");
        // the ear's program: the signed release at one path every release shares
        let fixed = format!("{home}/.local/libexec/rift/rift");
        let plist = |data: &str| {
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>EnvironmentVariables</key>
  <dict><key>RIFT_ROOT</key><string>{data}</string></dict>
  <key>ExitTimeOut</key>
  <integer>3</integer>
  <key>KeepAlive</key>
  <dict><key>SuccessfulExit</key><false/></dict>
  <key>Label</key>
  <string>dev.gigantic.rift</string>
  <key>ProgramArguments</key>
  <array><string>{exe}</string><string>serve</string></array>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardErrorPath</key>
  <string>{data}/serve.err</string>
  <key>ThrottleInterval</key>
  <integer>10</integer>
</dict>
</plist>
"#
            )
        };
        // the standing ear's: its track this Mac's local host name, lowercased
        let ear = |data: &str| {
            plist(data)
                .replace("<dict><key>SuccessfulExit</key><false/></dict>", "<true/>")
                .replace("rift</string>\n", "rift.ear</string>\n")
                .replace(
                    "<string>serve</string>",
                    "<string>device</string><string>ear</string><string>test-mac</string><string>--mic</string>",
                )
                .replace("serve.err", "ear.err")
                .replace(&exe, &fixed)
        };
        // the standing mouth's, beside it on the same copy
        // the standing mouth's, on a copy of its own beside the ear's
        let mouthcopy = format!("{home}/.local/libexec/rift/mouth");
        let mouthed = |data: &str| {
            ear(data)
                .replace("rift.ear</string>\n", "rift.mouth</string>\n")
                .replace("<string>ear</string>", "<string>mouth</string>")
                .replace("--mic", "--speaker")
                .replace("ear.err", "mouth.err")
                .replace(&fixed, &mouthcopy)
        };
        let standing = |data: &str| format!("{}{}", ear(data), mouthed(data));
        // scutil's stub: the name, or with `fail-scutil` none set
        let host = format!("{root}/host");
        fs::create_dir_all(&host)?;
        fs::write(
            format!("{host}/scutil"),
            format!(
                "#!/bin/sh\n[ -f {root}/fail-scutil ] && {{ echo 'LocalHostName: not set'; exit 1; }}\necho Test-Mac\n"
            ),
        )?;
        fs::set_permissions(format!("{host}/scutil"), Permissions::from_mode(0o755))?;
        let named = format!("{host}:/usr/bin:/bin");
        let data = format!("{root}/data");
        let run = |path: &str, envs: &[(&str, &str)], args: &[&str]| {
            rift(
                &data,
                &[
                    &[
                        ("HOME", home.as_str()),
                        ("HOTSWAP_TEST_HOME", home.as_str()),
                        ("PATH", path),
                    ],
                    envs,
                ]
                .concat(),
                &[&["setup"], args].concat(),
            )
        };
        // with no scutil (off macOS) the daemon's alone, as there is no ear
        same(
            &run("/usr/bin:/bin", &[], &["--print"])?,
            &format!("0\n{}", plist(&data)),
        )?;
        same(
            &run(&named, &[], &["--print"])?,
            &format!("0\n{}{}", plist(&data), standing(&data)),
        )?;
        // no RIFT_ROOT: the default under HOME
        let out = bounded(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("HOME", &home)
                .env("PATH", &named)
                .env_remove("RIFT_ROOT")
                .args(["setup", "--print"]),
            "rift setup --print",
        )?;
        let state = format!("{home}/.local/state/rift");
        same(
            &String::from_utf8(out.stdout)?,
            &format!("{}{}", plist(&state), standing(&state)),
        )?;
        // a relative RIFT_ROOT: made absolute against where setup ran, as launchd runs from /
        let relative = bounded(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .current_dir(&root)
                .env("HOME", &home)
                .env("PATH", &named)
                .env("RIFT_ROOT", "data")
                .args(["setup", "--print"]),
            "rift setup --print",
        )?;
        same(
            &String::from_utf8(relative.stdout)?,
            &format!("{}{}", plist(&data), standing(&data)),
        )?;
        same(
            &run("/usr/bin", &[], &[])?,
            "1\nrift setup: launchctl print: No such file or directory (os error 2) (setup installs launchd agents, macOS only; --print prints their plists)\n",
        )?;
        same(&fs::exists(&home)?.to_string(), "false")?;
        fs::create_dir_all(&bin)?;
        fs::create_dir_all(format!("{root}/loaded"))?;
        fs::write(
            format!("{bin}/launchctl"),
            format!(
                r#"#!/bin/sh
printf '%s\n' "$*" >> {root}/launchctl.log
job=${{2##*/}}
start() {{ [ -f {root}/dead ] && return; case $1 in *.ear.plist) [ -f {root}/deaf ] && return;; *.mouth.plist) [ -f {root}/mute ] && return;; esac; RIFT_ROOT=$(sed -n 's|.*<key>RIFT_ROOT</key><string>\(.*\)</string>.*|\1|p' $1) RIFT_MIC={root}/fake RIFT_SPEAKER={root}/fake RIFT_POLL_MS=100 {serve} $(sed -n 's|.*<array><string>[^<]*</string>\(.*\)</array>|\1|p' $1 | sed 's|<string>||g; s|</string>| |g') >/dev/null 2>&1 & echo $! > {root}/loaded/$(basename $1 .plist); }}
stop() {{ kill $1 2>/dev/null || return 0; while kill -0 $1 2>/dev/null; do sleep 0.02; done; }}
hold() {{ until RIFT_ROOT=$1 {serve} status | grep -q ready; do sleep 0.05; done; RIFT_ROOT=$1 RIFT_MIC={root}/fake {serve} device ear test-mac --mic >/dev/null 2>&1 & echo $! > {root}/holder; until RIFT_ROOT=$1 {serve} status | grep -q ear-test-mac; do sleep 0.05; done; }}
case $1 in
print) [ -f {root}/loaded/$job.going ] && {{ rm {root}/loaded/$job.going; exit 0; }}; [ -f {root}/loaded/$job ] || exit 113;;
bootout) stop $(cat {root}/loaded/$job); [ -f {root}/stuck ] || mv {root}/loaded/$job {root}/loaded/$job.going;;
bootstrap) case $3 in *.upgrade.plist) touch {root}/loaded/$(basename $3 .plist); exit;; esac; held=$(sed -n 's|.*<key>RIFT_ROOT</key><string>\(.*\)</string>.*|\1|p' $3); [ -n "$(find "$held" -maxdepth 0 -perm 700)" ] && echo root 0700 >> {root}/launchctl.log; [ -f {root}/fail ] && {{ echo 'Bootstrap failed: 5: Input/output error' >&2; exit 5; }}; start $3; case $3 in *rift.plist) [ ! -f {root}/held ] || hold "$held";; esac;;
print-disabled) [ -f {root}/disabled ] && sed 's/.*/	"&" => disabled/' {root}/disabled; exit 0;;
kickstart) stop $(cat {root}/loaded/${{3##*/}}); [ -f {root}/refuse ] && exit; {{ sleep 1; start {file}; }} >/dev/null 2>&1 &;;
esac
"#,
                serve = env!("CARGO_BIN_EXE_rift")
            ),
        )?;
        fs::write(
            format!("{bin}/curl"),
            "#!/bin/sh\nprintf https://github.com/jgeschwendt/rift/releases/tag/v1\n",
        )?;
        // the signing tools, each call logged to sign.log by its first word: `fail-<word>` fails it,
        // `locked` stands for a locked keychain, which only an unlock lifts and codesign then
        // refuses, `hang` holds codesign as a prompt at the screen would; an unlock refuses any
        // password but the password file's, its trailing newline cut; the keychain file holds
        // its identity, an import's SHA-1 of its path, and `signature` the requirement codesign
        // last signed to
        fs::write(
            format!("{bin}/security"),
            format!(
                r#"#!/bin/sh
echo "security $1" >> {root}/sign.log
[ -f {root}/fail-$1 ] && {{ echo "$1 failed" >&2; exit 1; }}
case $1 in
create-keychain) : > "$4";;
find-identity) [ -s "$4" ] && printf '  1) %s "rift" (CSSMERR_TP_NOT_TRUSTED)\n     1 identities found\n' "$(cat "$4")";;
import) printf %s "$4" | shasum | cut -c1-40 > "$4";;
unlock-keychain) [ "$3" = "$(cat "${{4%db}}pass")" ] || {{ echo 'The user name or passphrase you entered is not correct.' >&2; exit 51; }}; rm -f {root}/locked;;
esac
exit 0
"#
            ),
        )?;
        fs::write(
            format!("{bin}/openssl"),
            format!(
                "#!/bin/sh\necho \"openssl $1\" >> {root}/sign.log\n[ -f {root}/fail-openssl ] && {{ echo \"$1 failed\" >&2; exit 1; }}\nexit 0\n"
            ),
        )?;
        fs::write(
            format!("{bin}/codesign"),
            format!(
                r#"#!/bin/sh
echo "codesign $1 ${{8:-$3}}" >> {root}/sign.log
[ -f {root}/hang ] && exec sleep 10
case $1 in
-v) [ "$(cat {root}/signature 2>/dev/null)" = "$2" ];;
-f) [ -f {root}/locked ] && {{ echo errSecInternalComponent >&2; exit 1; }}; [ -f {root}/fail-codesign ] && {{ echo "$8: not a Mach-O file" >&2; exit 1; }}; echo "-R=identifier \"$7\" and certificate leaf = H\"$3\"" > {root}/signature;;
esac
"#
            ),
        )?;
        for tool in ["codesign", "curl", "launchctl", "openssl", "security"] {
            fs::set_permissions(format!("{bin}/{tool}"), Permissions::from_mode(0o755))?;
        }
        // hotswap's layout, its version 1 a copy of this build: setup signs it in place
        let layout = format!("{home}/.local/share/hotswap/rift");
        let lay = || -> Outcome {
            fs::create_dir_all(format!("{layout}/1"))?;
            let _: u64 = fs::copy(env!("CARGO_BIN_EXE_rift"), format!("{layout}/1/rift"))?;
            Ok(symlink("1", format!("{layout}/current"))?)
        };
        lay()?;
        let release = format!("{layout}/1/rift");
        let linked = format!("linked {exe} \u{2192} {release}\n");
        let installed = format!("installed {release} as {fixed}\n");
        let signs = || -> Result<String, Box<dyn Error>> {
            let log = fs::read_to_string(format!("{root}/sign.log")).unwrap_or_default();
            drop(fs::remove_file(format!("{root}/sign.log")));
            Ok(log)
        };
        let stubbed = format!("{bin}:{named}");
        let gui = format!(
            "gui/{}",
            String::from_utf8(bounded(Command::new("id").arg("-u"), "id")?.stdout)?.trim()
        );
        let job = format!("{gui}/dev.gigantic.rift");
        let agents = format!("{home}/Library/LaunchAgents");
        let upgrade = format!("{agents}/dev.gigantic.rift.upgrade.plist");
        let tended = format!(
            "print {gui}/dev.gigantic.rift.upgrade\nprint-disabled {gui}\nbootstrap {gui} {upgrade}\n"
        );
        // the ear's and the mouth's jobs: their plists written beside the daemon's; each booted out
        // before the daemon is touched (printed `muted`, their calls `deafen`, or only `unheard`
        // when not loaded); each loaded once the daemon is up (printed `heard`, their calls `hear`)
        let earfile = format!("{agents}/dev.gigantic.rift.ear.plist");
        let earjob = format!("{gui}/dev.gigantic.rift.ear");
        let mouthfile = format!("{agents}/dev.gigantic.rift.mouth.plist");
        let mouthjob = format!("{gui}/dev.gigantic.rift.mouth");
        // the mouth's plist is written only as it is loaded, the daemon and the ear good, its
        // copy placed first when it is not the release's (`placing` the first time)
        let wrote = format!("wrote {file}\nwrote {earfile}\n");
        let (earboot, mouthboot) = (
            format!("launchctl bootstrap {gui} {earfile}\n"),
            format!("wrote {mouthfile}\nlaunchctl bootstrap {gui} {mouthfile}\n"),
        );
        let placing = format!("installed {release} as {mouthcopy}\n");
        let (muted, heard, placed) = (
            format!("launchctl bootout {earjob}\nlaunchctl bootout {mouthjob}\n"),
            format!("{earboot}{mouthboot}"),
            format!("{earboot}{placing}{mouthboot}"),
        );
        let absent = |agent: &str| format!("print {agent}\nprint {agent}\n");
        let off = |agent: &str| format!("print {agent}\nbootout {agent}\n{}", absent(agent));
        let on = |agent: &str, sheet: &str| {
            format!("{}bootstrap {gui} {sheet}\nroot 0700\n", absent(agent))
        };
        let unheard = format!("{}{}", absent(&earjob), absent(&mouthjob));
        let deafen = format!("{}{}", off(&earjob), off(&mouthjob));
        let mouthon = on(&mouthjob, &mouthfile);
        let hear = format!("print-disabled {gui}\n{}{mouthon}", on(&earjob, &earfile));
        let calls = || -> Result<String, Box<dyn Error>> {
            let log = fs::read_to_string(format!("{root}/launchctl.log"))?;
            fs::remove_file(format!("{root}/launchctl.log"))?;
            Ok(log)
        };
        // an earlier install, loaded, its root 0744 as launchd made it; and another program's agent
        let (old, earlier) = (
            format!("{root}/old"),
            format!("{agents}/dev.example.old.plist"),
        );
        let former = format!("{gui}/dev.example.old");
        let plant = |held: &str| -> Outcome {
            fs::create_dir_all(held)?;
            fs::create_dir_all(&agents)?;
            fs::write(
                &earlier,
                plist(held).replace("dev.gigantic.rift", "dev.example.old"),
            )?;
            Ok(fs::write(format!("{held}/events.ndjson"), "the log\n")?)
        };
        plant(&old)?;
        fs::set_permissions(&old, Permissions::from_mode(0o744))?;
        fs::write(format!("{root}/loaded/dev.example.old"), "")?;
        let other = format!("{agents}/com.example.other.plist");
        fs::write(&other, plist(&old).replace(&exe, "/usr/bin/true"))?;
        // the signing a run does: in a fresh keychain an identity made and the release signed;
        // after it, the unlock, the identity found and the signature checked
        let id = |at: &str| -> Result<String, Box<dyn Error>> {
            Ok(fs::read_to_string(format!("{at}/rift.keychain-db"))?
                .trim()
                .to_owned())
        };
        let made = |at: &str, hash: &str| {
            format!(
                "wrote {at}/rift.keychain-pass\nmade {at}/rift.keychain-db\nmade the code-signing identity {hash} in {at}/rift.keychain-db\nsigned {release} as dev.gigantic.rift by {hash}\n"
            )
        };
        let steady = format!(
            "security unlock-keychain\nsecurity find-identity\nsecurity set-key-partition-list\ncodesign -v {release}\n"
        );
        let fresh = format!(
            "security create-keychain\nsecurity set-keychain-settings\nsecurity unlock-keychain\nsecurity find-identity\nopenssl req\nopenssl pkcs12\nsecurity import\nsecurity find-identity\nsecurity set-key-partition-list\ncodesign -v {release}\ncodesign -f {release}.new\n"
        );
        let first = format!("{}{}{}", run(&stubbed, &[], &[])?, calls()?, signs()?);
        same(
            &first,
            &format!(
                "0\nlaunchctl bootout {former}\nmoved {old} to {data}\nremoved {earlier}\n{linked}{}{installed}{wrote}launchctl bootstrap {gui} {file}\n{placed}print {job}\nprint {former}\nbootout {former}\nprint {former}\nprint {former}\n{tended}{unheard}print {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n{hear}{fresh}",
                made(&data, &id(&data)?)
            ),
        )?;
        same(
            &format!(
                "{:o} {} {}",
                fs::metadata(format!("{data}/rift.keychain-pass"))?.mode() & 0o777,
                fs::read_to_string(format!("{data}/rift.keychain-pass"))?.len(),
                fs::exists(format!("{release}.new"))?
            ),
            "600 32 false",
        )?;
        same(
            &format!(
                "{} {} {} {}",
                fs::read_to_string(format!("{data}/events.ndjson"))?
                    .lines()
                    .next()
                    .unwrap_or_default(),
                fs::exists(&old)?,
                fs::exists(&earlier)?,
                fs::exists(&other)?
            ),
            "the log false false true",
        )?;
        // the ear's copy: the release byte for byte, its staging file gone; its inode tells a
        // rerun that left it alone from a release that replaced it
        let copied = || -> Result<(String, u64), Box<dyn Error>> {
            Ok((
                format!(
                    "{} {}",
                    fs::read(&fixed)? == fs::read(&release)?,
                    fs::exists(format!("{fixed}.new"))?
                ),
                fs::metadata(&fixed)?.ino(),
            ))
        };
        let (copy, inode) = copied()?;
        same(&copy, "true false")?;
        same(
            &format!(
                "{}{}{}",
                fs::read_to_string(&file)?,
                fs::read_to_string(&earfile)?,
                fs::exists(format!("{file}.new"))? || fs::exists(format!("{earfile}.new"))?
            ),
            &format!("{}{}false", plist(&data), ear(&data)),
        )?;
        // a second run, its password file ending in a hand-made newline, remakes nothing and
        // signs nothing
        let secret = format!("{data}/rift.keychain-pass");
        fs::write(&secret, format!("{}\n", fs::read_to_string(&secret)?))?;
        same(
            &format!("{}{}{}", run(&stubbed, &[], &[])?, calls()?, signs()?),
            &format!(
                "0\n{linked}{wrote}{muted}launchctl kickstart -k {job}\n{heard}print {job}\n{tended}{deafen}kickstart -k {job}\n{hear}{steady}"
            ),
        )?;
        // the swap returns once the respawn, held back as by ThrottleInterval, answers ready,
        // the standing ear among its devices, holding no microphone while the ears are off
        let health = || -> Result<Value, Box<dyn Error>> {
            Ok(serde_json::from_str(&say(
                &mut greet(&data)?,
                r#"{"op":"health"}"#,
            )?)?)
        };
        // the rerun left the ear's copy as it was
        same(
            &format!("{:?}", copied()?),
            &format!("(\"true false\", {inode})"),
        )?;
        let now = health()?;
        same(
            &format!("{} {} {}", now["state"], now["devices"], now["ears"]),
            r#""ready" ["ear-test-mac","mouth-test-mac"] false"#,
        )?;
        same(
            &fs::read_link(&exe)?.display().to_string(),
            &format!("{layout}/current/rift"),
        )?;
        // a kickstart launchd refuses to spawn: loaded afresh past $RIFT_RELOAD_MS, then ready;
        // an ear that never connects (`deaf`) loaded afresh too, then named, the daemon serving
        // on and the mouth not touched
        for marker in ["deaf", "refuse"] {
            fs::write(format!("{root}/{marker}"), "")?;
        }
        same(
            &format!(
                "{}{}{}",
                run(
                    &stubbed,
                    &[("RIFT_READY_MS", "3000"), ("RIFT_RELOAD_MS", "0")],
                    &[]
                )?,
                calls()?,
                signs()?
            ),
            &format!(
                "1\n{linked}{wrote}{muted}launchctl kickstart -k {job}\ndev.gigantic.rift not ready 0 ms after its load; loading it again\nlaunchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\n{earboot}dev.gigantic.rift.ear not ready 0 ms after its load; loading it again\n{earboot}rift setup: dev.gigantic.rift.ear not ready 3000 ms after its load; see {data}/ear.err\nprint {job}\n{tended}{deafen}kickstart -k {job}\nprint {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\nprint-disabled {gui}\n{0}{0}{steady}",
                on(&earjob, &earfile),
            ),
        )?;
        same(&health()?["state"].to_string(), "\"ready\"")?;
        for marker in ["deaf", "refuse"] {
            fs::remove_file(format!("{root}/{marker}"))?;
        }
        // a mouth that never connects (`mute`), loaded afresh, then booted out and its plist and
        // copy taken away before setup fails: nothing of the mouth is left
        fs::write(format!("{root}/mute"), "")?;
        same(
            &format!(
                "{}{}{}",
                run(&stubbed, &[("RIFT_READY_MS", "1000")], &[])?,
                calls()?,
                signs()?
            ),
            &format!(
                "1\n{linked}{wrote}launchctl kickstart -k {job}\n{heard}dev.gigantic.rift.mouth not ready 15000 ms after its load; loading it again\nlaunchctl bootstrap {gui} {mouthfile}\nremoved {mouthfile}\nremoved {mouthcopy}\nrift setup: dev.gigantic.rift.mouth not ready 1000 ms after its load; see {data}/mouth.err\nprint {job}\n{tended}{unheard}kickstart -k {job}\n{hear}{mouthon}{0}{steady}",
                absent(&mouthjob)
            ),
        )?;
        fs::remove_file(format!("{root}/mute"))?;
        same(
            &format!("{} {}", fs::exists(&mouthfile)?, fs::exists(&mouthcopy)?),
            "false false",
        )?;
        // the swap: setup through the link, its kickstart, then the smoke against the new daemon
        same(
            &format!(
                "{}{}{}",
                rift(
                    &data,
                    &[
                        ("HOME", home.as_str()),
                        ("HOTSWAP_TEST_HOME", home.as_str()),
                        ("PATH", stubbed.as_str())
                    ],
                    &["upgrade"]
                )?,
                calls()?,
                signs()?
            ),
            &format!(
                "0\nrift 1: green, 2 smoke probes\nprint {job}\n{tended}{}{}kickstart -k {job}\n{hear}{steady}",
                off(&earjob),
                absent(&mouthjob)
            ),
        )?;
        same(
            &rift(&data, &[("HOME", home.as_str())], &["upgrade", "--x"])?,
            "2\nusage: rift upgrade [--check | --back | --retry]\n",
        )?;
        // a rebuilt release (its bytes changed, its signature gone) under a locked keychain:
        // unlocked, signed, copied over the ear's, then loaded afresh rather than kickstarted;
        // and a hand-run ear already holding the track (`held`) once the new daemon is up: the
        // standing ear loaded unwaited, to take the track over once that one leaves
        let hash = id(&data)?;
        fs::remove_file(format!("{root}/signature"))?;
        OpenOptions::new()
            .append(true)
            .open(&release)?
            .write_all(b"rebuilt")?;
        for marker in ["held", "locked"] {
            fs::write(format!("{root}/{marker}"), "")?;
        }
        same(
            &format!(
                "{}{}{} {}",
                run(&stubbed, &[], &[])?,
                calls()?,
                signs()?,
                fs::exists(format!("{root}/locked"))?
            ),
            &format!(
                "0\n{linked}signed {release} as dev.gigantic.rift by {hash}\n{installed}{wrote}{muted}launchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\n{earboot}ear-test-mac is held by another device; dev.gigantic.rift.ear takes it over once it leaves\n{placing}{mouthboot}print {job}\n{tended}{deafen}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\nprint-disabled {gui}\nbootstrap {gui} {earfile}\nroot 0700\n{mouthon}{steady}codesign -f {release}.new\n false"
            ),
        )?;
        let recopied = copied()?;
        same(
            &format!("{} {}", recopied.0, recopied.1 != inode),
            "true false true",
        )?;
        fs::remove_file(format!("{root}/held"))?;
        let holder = fs::read_to_string(format!("{root}/holder"))?;
        same(
            &String::from_utf8(bounded(Command::new("kill").arg(holder.trim()), "kill")?.stderr)?,
            "",
        )?;
        until(|| {
            Ok(health().is_ok_and(|listing| {
                listing["devices"]
                    .as_array()
                    .is_some_and(|held| held.len() == 2)
            }) && !bounded(Command::new("kill").args(["-0", holder.trim()]), "kill")
                .is_ok_and(|alive| alive.status.success()))
        })?;
        // gone, so the guard never signals a pid reused since
        fs::remove_file(format!("{root}/holder"))?;
        // each failure is one line, exit 1, before the daemon is touched; a failed signing
        // leaves no copy behind
        let failed = |marker: &str, envs: &[(&str, &str)]| -> Result<String, Box<dyn Error>> {
            fs::write(format!("{root}/{marker}"), "")?;
            let said = format!("{}{}{}", run(&stubbed, envs, &[])?, calls()?, signs()?);
            fs::remove_file(format!("{root}/{marker}"))?;
            Ok(said)
        };
        same(
            &failed("fail-unlock-keychain", &[])?,
            &format!(
                "1\n{linked}rift setup: security unlock-keychain: unlock-keychain failed\nprint {job}\n{tended}security unlock-keychain\n"
            ),
        )?;
        // a Mac whose local host name is unset: refused before anything is written
        same(
            &failed("fail-scutil", &[])?,
            &format!(
                "1\nrift setup: scutil --get LocalHostName gave \"LocalHostName: not set\", no track name (1 to 64 letters, digits, - or _)\nprint {job}\n"
            ),
        )?;
        // a tool not on PATH, which holds only launchctl and what it and setup call before it
        let bare = format!("{root}/bare");
        fs::create_dir_all(&bare)?;
        symlink(format!("{bin}/launchctl"), format!("{bare}/launchctl"))?;
        symlink(format!("{host}/scutil"), format!("{bare}/scutil"))?;
        for tool in ["id", "touch"] {
            symlink(format!("/usr/bin/{tool}"), format!("{bare}/{tool}"))?;
        }
        same(
            &format!("{}{}{}", run(&bare, &[], &[])?, calls()?, signs()?),
            &format!(
                "1\n{linked}rift setup: security unlock-keychain: No such file or directory (os error 2)\nprint {job}\n{tended}"
            ),
        )?;
        same(
            &failed("hang", &[("RIFT_SIGN_MS", "2000")])?,
            &format!(
                "1\n{linked}rift setup: codesign -v: no answer in 2000 ms; is it asking at the screen?\nprint {job}\n{tended}{steady}"
            ),
        )?;
        fs::remove_file(format!("{root}/signature"))?;
        same(
            &format!(
                "{} {}",
                failed("fail-codesign", &[])?,
                fs::exists(format!("{release}.new"))?
            ),
            &format!(
                "1\n{linked}rift setup: codesign -f: {release}.new: not a Mach-O file\nprint {job}\n{tended}{steady}codesign -f {release}.new\n false"
            ),
        )?;
        // in a fresh root: the keychain, the certificate, its import
        let blank = format!("{root}/blank");
        let anew = |marker: &str| -> Result<String, Box<dyn Error>> {
            fs::write(format!("{root}/{marker}"), "")?;
            let said = rift(
                &blank,
                &[
                    ("HOME", home.as_str()),
                    ("HOTSWAP_TEST_HOME", home.as_str()),
                    ("PATH", stubbed.as_str()),
                ],
                &["setup"],
            )?;
            fs::remove_file(format!("{root}/{marker}"))?;
            Ok(format!("{said}{}{}", calls()?, signs()?))
        };
        same(
            &anew("fail-create-keychain")?,
            &format!(
                "1\n{linked}wrote {blank}/rift.keychain-pass\nrift setup: security create-keychain: create-keychain failed\nprint {job}\n{tended}security create-keychain\n"
            ),
        )?;
        same(
            &format!(
                "{} {}",
                anew("fail-openssl")?,
                fs::exists(format!("{blank}/identity.new"))?
            ),
            &format!(
                "1\n{linked}made {blank}/rift.keychain-db\nrift setup: sh -c: req failed\nprint {job}\n{tended}security create-keychain\nsecurity set-keychain-settings\nsecurity unlock-keychain\nsecurity find-identity\nopenssl req\n false"
            ),
        )?;
        same(
            &anew("fail-import")?,
            &format!(
                "1\n{linked}rift setup: sh -c: import failed\nprint {job}\n{tended}security unlock-keychain\nsecurity find-identity\nopenssl req\nopenssl pkcs12\nsecurity import\n"
            ),
        )?;
        // an identity imported without its partition list gets one the next run, before codesign
        let partial = anew("fail-set-key-partition-list")?;
        let minted = id(&blank)?;
        same(
            &format!("{partial}{}", anew("fail-codesign")?),
            &format!(
                "1\n{linked}made the code-signing identity {minted} in {blank}/rift.keychain-db\nrift setup: security set-key-partition-list: set-key-partition-list failed\nprint {job}\n{tended}security unlock-keychain\nsecurity find-identity\nopenssl req\nopenssl pkcs12\nsecurity import\nsecurity find-identity\nsecurity set-key-partition-list\n1\n{linked}rift setup: codesign -f: {release}.new: not a Mach-O file\nprint {job}\n{tended}{steady}codesign -f {release}.new\n"
            ),
        )?;
        // a changed plist: booted out, waited out, bootstrapped; its absent root made first
        let moved = format!("{root}/moved");
        let rerun = |args: &[&str], envs: &[(&str, &str)]| {
            rift(
                &moved,
                &[
                    &[
                        ("HOME", home.as_str()),
                        ("HOTSWAP_TEST_HOME", home.as_str()),
                        ("PATH", stubbed.as_str()),
                    ],
                    envs,
                ]
                .concat(),
                &[&["setup"], args].concat(),
            )
        };
        // its own keychain and identity, so the release is signed again; the ear disabled in
        // launchd, its plist written and never loaded
        fs::write(
            format!("{root}/disabled"),
            "dev.gigantic.rift.ear\ndev.gigantic.rift.mouth\n",
        )?;
        let moving = format!("{}{}{}", rerun(&[], &[])?, calls()?, signs()?);
        fs::remove_file(format!("{root}/disabled"))?;
        same(
            &moving,
            &format!(
                "0\n{linked}{}{wrote}{muted}launchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\ndev.gigantic.rift.ear is disabled, so written and not loaded; launchctl enable {earjob} stands it\nwrote {mouthfile}\ndev.gigantic.rift.mouth is disabled, so written and not loaded; launchctl enable {mouthjob} stands it\nprint {job}\n{tended}{deafen}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\nprint-disabled {gui}\n{fresh}",
                made(&moved, &id(&moved)?)
            ),
        )?;
        same(&fs::read_to_string(&earfile)?, &ear(&moved))?;
        // an earlier install, unloaded, beside a root that exists: retired, its root left alone
        plant(&old)?;
        fs::write(format!("{root}/fail"), "")?;
        // data's identity signs the release once more
        same(
            &format!("{}{}{}", run(&stubbed, &[], &[])?, calls()?, signs()?),
            &format!(
                "1\nremoved {earlier}\n{linked}signed {release} as dev.gigantic.rift by {hash}\n{wrote}launchctl bootout {job}\nrift setup: launchctl bootstrap {gui} {file}: Bootstrap failed: 5: Input/output error\nprint {job}\nprint {former}\nprint {former}\n{tended}{unheard}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n{steady}codesign -f {release}.new\n"
            ),
        )?;
        same(
            &fs::exists(format!("{old}/events.ndjson"))?.to_string(),
            "true",
        )?;
        fs::remove_file(format!("{root}/fail"))?;
        fs::write(format!("{root}/loaded/dev.gigantic.rift"), "")?;
        fs::write(format!("{root}/stuck"), "")?;
        let resigned = format!("signed {release} as dev.gigantic.rift by {}\n", id(&moved)?);
        same(
            &format!(
                "{}{}{}",
                rerun(&[], &[("RIFT_BOOTOUT_MS", "0")])?,
                calls()?,
                signs()?
            ),
            &format!(
                "1\n{linked}{resigned}{wrote}launchctl bootout {job}\nrift setup: dev.gigantic.rift still loaded 0 ms after its bootout\nprint {job}\n{tended}{unheard}print {job}\nbootout {job}\nprint {job}\n{steady}codesign -f {release}.new\n"
            ),
        )?;
        fs::remove_file(format!("{root}/stuck"))?;
        // a load that never answers ready is loaded afresh past $RIFT_RELOAD_MS, then fails past
        // $RIFT_READY_MS
        fs::write(format!("{root}/dead"), "")?;
        same(
            &format!(
                "{}{}{}",
                run(
                    &stubbed,
                    &[("RIFT_READY_MS", "0"), ("RIFT_RELOAD_MS", "0")],
                    &[]
                )?,
                calls()?,
                signs()?
            ),
            &format!(
                "1\n{linked}signed {release} as dev.gigantic.rift by {hash}\n{wrote}launchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\ndev.gigantic.rift not ready 0 ms after its load; loading it again\nlaunchctl bootstrap {gui} {file}\nrift setup: dev.gigantic.rift not ready 0 ms after its load; see {data}/serve.err\nprint {job}\n{tended}{unheard}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n{steady}codesign -f {release}.new\n"
            ),
        )?;
        fs::remove_file(format!("{root}/dead"))?;
        for loaded in [
            "dev.gigantic.rift",
            "dev.gigantic.rift.ear",
            "dev.gigantic.rift.mouth",
        ] {
            fs::write(format!("{root}/loaded/{loaded}"), "")?;
        }
        plant(&old)?;
        // the keychain and its password go as files, the root stays; no host name is asked for
        fs::write(format!("{root}/fail-scutil"), "")?;
        same(
            &format!("{}{}{}", rerun(&["--remove"], &[])?, calls()?, signs()?),
            &format!(
                "0\nremoved {earlier}\nlaunchctl bootout {earjob}\nremoved {earfile}\nlaunchctl bootout {mouthjob}\nremoved {mouthfile}\nlaunchctl bootout {job}\nremoved {file}\nremoved {exe}\nremoved {fixed}\nremoved {mouthcopy}\nremoved {moved}/rift.keychain-db\nremoved {moved}/rift.keychain-pass\nprint {gui}/dev.gigantic.rift.upgrade\nprint {former}\nprint {former}\n{deafen}print {job}\nbootout {job}\nprint {job}\nprint {job}\n"
            ),
        )?;
        same(
            &format!("{}{}", rerun(&["--remove"], &[])?, calls()?),
            &format!(
                "0\nprint {gui}/dev.gigantic.rift.upgrade\n{unheard}print {job}\nprint {job}\n"
            ),
        )?;
        same(
            &format!(
                "{} {} {} {} {} {}",
                fs::exists(&moved)?,
                fs::exists(&upgrade)?,
                fs::exists(&layout)?,
                fs::exists(&earfile)?,
                fs::exists(&mouthfile)?,
                fs::exists(format!("{home}/.local/libexec/rift"))?
            ),
            "true false false false false false",
        )
    }

    /// `device ear <name> --mic` over the fake inputs of `$RIFT_MIC`: nothing opened while the ears
    /// are off; with them on, the default input's channel 0 at 16 kHz (a 48 kHz stereo input whose
    /// other channel is full scale and whose swing at its Nyquist rate the filter takes out), each
    /// event naming its microphone; the default changed, the segment closes and a hole opens the
    /// next microphone's; ears off closes the microphone. A second device pinned to an absent
    /// input says so and waits, records it once it appears and stops when it goes. A torn segment
    /// keeps its microphone; a frame whose `mic` is not a string is refused. Pongs leave on the
    /// reader's thread, so listings slower than a ping leave no hole; a daemon frozen past a poll
    /// holds up the frames, which go on from the sample count, no hole. An open waited on, a refused
    /// open and exact zeros are each said once and the device kept: the waited open returns when
    /// granted, the others are retried, a poll doubled per failure, until heard. A device outlives
    /// its daemon: it finds it down, waits, and connects again once it is back; SIGTERM, SIGINT
    /// and SIGHUP each end one at once, exit 0, one stderr line, the daemon down or not; a refusal
    /// (`taken`, another protocol) is said once on stderr, the attempts backing off; `ear.err` is
    /// capped at an attempt. A
    /// Bluetooth default is followed only while there is no built-in input, then left for the
    /// built-in, each reason said once; a device pinned to it hears it.
    #[cfg(target_os = "macos")]
    #[test]
    fn mic() -> Outcome {
        let root = temp("mic")?;
        let fake = format!("{root}/fake");
        fs::create_dir_all(format!("{fake}/inputs"))?;
        fs::write(format!("{fake}/inputs/Desk Mic"), "48000 2 0.25 0.5")?;
        fs::write(format!("{fake}/inputs/Hall Mic"), "44100 1 -0.5 0")?;
        fs::write(format!("{fake}/default"), "Desk Mic\n")?;
        fs::write(format!("{fake}/slow"), "50")?;
        // the open and zero windows far past any stall a starved machine gives a device: only
        // an open that truly waits, or an input that truly gives zeros, is named
        let envs = [
            ("RIFT_LOG_CAP", "1"),
            ("RIFT_MIC", fake.as_str()),
            ("RIFT_OPEN_MS", "3000"),
            ("RIFT_POLL_MS", "100"),
            ("RIFT_ZERO_MS", "3000"),
        ];
        let (daemon, _) = serve(
            &root,
            &[("RIFT_PING_MS", "300"), ("RIFT_SEGMENT_MS", "200")],
        )?;
        let spawn_with = |at: &str, poll: &str, args: &[&str]| -> io::Result<Daemon> {
            Ok(Daemon(
                Command::new(env!("CARGO_BIN_EXE_rift"))
                    .env("RIFT_ROOT", at)
                    .envs(envs)
                    .env("RIFT_POLL_MS", poll)
                    .args(args)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?,
            ))
        };
        // a device that hears an input judges it silent only a second long, far past a starved
        // machine's stall; one that never hears one polls fast, so its backoff is quick to see
        let spawn = |args: &[&str]| spawn_with(&root, "1000", args);
        // a stop signal ends a device at once, exit 0, saying so, the daemon down or not
        let stop = |mut device: Daemon,
                    mut errs: BufReader<ChildStderr>,
                    name: &str|
         -> Result<String, Box<dyn Error>> {
            same(&signal(&device, name)?, "")?;
            let mut rest = String::new();
            let _: usize = io::Read::read_to_string(&mut errs, &mut rest)?;
            Ok(format!("{:?} {rest}", device.0.wait()?.code()))
        };
        let log = || fs::read_to_string(format!("{fake}/log")).unwrap_or_default();
        // launchd's stderr for the standing ear, past the cap: emptied into `.1` at an attempt
        fs::write(format!("{root}/ear.err"), "old")?;
        let stream = |track: &str| -> Result<Vec<Value>, Box<dyn Error>> {
            Ok(events(&root)?
                .into_iter()
                .map(|event| event["e"].clone())
                .filter(|event| event["track"] == track)
                .collect())
        };
        let mut desk = spawn(&["device", "ear", "desk", "--mic"])?;
        thread::sleep(Duration::from_millis(300));
        same(&log(), "")?;
        drop(rift(&root, &[], &["ears", "on"])?);
        let mut heard = greet(&root)?;
        same(
            &say(&mut heard, r#"{"op":"listen","track":"ear-desk"}"#)?,
            r#"{"ok":true}"#,
        )?;
        let frame: Value = serde_json::from_str(
            &iter::repeat_with(|| next(&mut heard))
                .nth(9)
                .ok_or("frame")??,
        )?;
        let pcm = frame["pcm"].as_str().unwrap_or_default();
        same(
            &(0..pcm.len())
                .step_by(4)
                .map(|at| {
                    let bytes = pcm.get(at..at + 4).unwrap_or_default();
                    i16::from_le_bytes([
                        u8::from_str_radix(bytes.get(..2).unwrap_or_default(), 16).unwrap_or(0),
                        u8::from_str_radix(bytes.get(2..).unwrap_or_default(), 16).unwrap_or(0),
                    ])
                })
                .all(|sample| (8191..=8193).contains(&sample))
                .to_string(),
            "true",
        )?;
        drop(heard);
        // a second of it, three pings past the reply's: no hole
        until(|| {
            Ok(stream("ear-desk").is_ok_and(|held| {
                held.iter()
                    .filter(|event| event["kind"] == "segment")
                    .count()
                    >= 5
            }))
        })?;
        let opening = stream("ear-desk")?;
        same(
            &format!(
                "{} {}",
                opening.iter().any(|event| event["kind"] == "hole"),
                opening[1]["samples"]
            ),
            "false 3200",
        )?;
        same(
            &format!(
                "{} {}",
                fs::read_to_string(format!("{root}/ear.err.1"))?,
                fs::metadata(format!("{root}/ear.err"))?.len()
            ),
            "old 0",
        )?;
        // a second device asking for a held track: refused `taken` each attempt on stdout, said
        // once on stderr, the attempts spaced a poll doubled per refusal
        let mut twin = spawn_with(&root, "100", &["device", "ear", "desk", "--mic"])?;
        let mut echoed = BufReader::new(twin.0.stdout.take().ok_or("stdout")?);
        same(
            &format!("{} {}", next(&mut echoed)?, next(&mut echoed)?),
            &format!("{} {}", code("taken"), code("taken")),
        )?;
        let twinned = BufReader::new(twin.0.stderr.take().ok_or("stderr")?);
        same(
            &stop(twin, twinned, "TERM")?,
            &format!(
                "Some(0) rift device: refused {}\nrift device: stopped by SIGTERM\n",
                code("taken")
            ),
        )?;
        // a daemon of another protocol: said once, the attempts backing off the same way
        let other = format!("{root}/other");
        fs::create_dir_all(format!("{other}/run"))?;
        let (listener, (stamp, met)) = (
            UnixListener::bind(format!("{other}/run/rift.sock"))?,
            mpsc::channel(),
        );
        drop(thread::spawn(move || {
            for dialed in listener.incoming().flatten() {
                let mut conn = BufReader::new(dialed);
                drop(next(&mut conn));
                let reply = r#"{"code":"proto","ok":false,"proto":2,"version":"9-x"}"#;
                drop(conn.get_mut().write_all(format!("{reply}\n").as_bytes()));
                stamp.send(Instant::now()).unwrap_or_default();
            }
        }));
        let mut alien = spawn_with(&other, "100", &["device", "ear", "alien", "--mic"])?;
        // a poll doubled, then doubled again: the third attempt six polls after the first
        let (once, thrice) = (met.recv()?, met.iter().nth(1).ok_or("no third")?);
        same(
            &(thrice.duration_since(once) >= Duration::from_millis(600)).to_string(),
            "true",
        )?;
        let strange = BufReader::new(alien.0.stderr.take().ok_or("stderr")?);
        same(
            &stop(alien, strange, "TERM")?,
            &format!(
                "Some(0) rift device: the daemon speaks protocol 2 (9-x), this client protocol 1 ({}); fix: rift setup, so both are one release\nrift device: stopped by SIGTERM\n",
                version()?
            ),
        )?;
        // the daemon frozen for 300 ms, three polls: the device's writes wait, its microphone
        // stays open, and the frames held up go on from the sample count, no hole and none ahead
        same(&signal(&daemon, "STOP")?, "")?;
        thread::sleep(Duration::from_millis(300));
        same(&signal(&daemon, "CONT")?, "")?;
        let frozen = opening.len();
        until(|| Ok(stream("ear-desk").is_ok_and(|held| held.len() >= frozen + 5)))?;
        let kinds: Vec<Value> = stream("ear-desk")?
            .iter()
            .map(|event| event["kind"].clone())
            .collect();
        same(
            &json!(kinds).to_string(),
            &json!(
                iter::once("track")
                    .chain(iter::repeat_n("segment", kinds.len() - 1))
                    .collect::<Vec<_>>()
            )
            .to_string(),
        )?;
        // a second device pinned to an input not yet there
        let mut spare = spawn(&["device", "ear", "spare", "--mic", "Spare Mic"])?;
        let (mut mouthcopy, mut said) = (
            BufReader::new(spare.0.stderr.take().ok_or("stderr")?),
            String::new(),
        );
        let _: usize = mouthcopy.read_line(&mut said)?;
        same(
            &said,
            "rift device: waiting for \"Spare Mic\"; the inputs present: Desk Mic, Hall Mic\n",
        )?;
        fs::write(format!("{fake}/inputs/Spare Mic"), "16000 1 0.5 0")?;
        until(|| {
            Ok(stream("ear-spare").is_ok_and(|held| {
                held.iter()
                    .any(|event| event["kind"] == "segment" && event["mic"] == "Spare Mic")
            }))
        })?;
        fs::write(format!("{fake}/default"), "Hall Mic\n")?;
        fs::remove_file(format!("{fake}/inputs/Spare Mic"))?;
        until(|| {
            Ok(stream("ear-desk").is_ok_and(|held| {
                held.iter()
                    .any(|event| event["kind"] == "segment" && event["mic"] == "Hall Mic")
            }) && log().contains("close Spare Mic"))
        })?;
        // one microphone per segment: the change closes Desk Mic's and a hole opens Hall Mic's
        let desk_events = stream("ear-desk")?;
        let change = desk_events
            .iter()
            .position(|event| event["mic"] == "Hall Mic")
            .ok_or("no change")?;
        same(
            &format!(
                "{} {} {}",
                desk_events[0]["kind"],
                desk_events
                    .get(change - 1)
                    .map_or(Value::Null, |event| json!([event["kind"], event["mic"]])),
                json!([desk_events[change]["kind"], desk_events[change]["mic"]])
            ),
            r#""track" ["segment","Desk Mic"] ["hole","Hall Mic"]"#,
        )?;
        drop(rift(&root, &[], &["ears", "off"])?);
        until(|| Ok(log().contains("close Hall Mic")))?;
        // each microphone asked for, opened and closed, none left open; one a starved machine
        // left silent a poll long is opened afresh, so counted by kind, not by line
        let mut opened: Vec<&str> = log().leak().lines().collect();
        opened.sort_unstable();
        let shut = ["Desk Mic", "Hall Mic", "Spare Mic"].iter().all(|mic| {
            opened
                .iter()
                .filter(|line| **line == format!("open {mic}"))
                .count()
                == opened
                    .iter()
                    .filter(|line| **line == format!("close {mic}"))
                    .count()
        });
        opened.dedup();
        same(
            &format!("{} {shut}", opened.join("\n")),
            "ask Desk Mic\nask Hall Mic\nask Spare Mic\nclose Desk Mic\nclose Hall Mic\nclose Spare Mic\nopen Desk Mic\nopen Hall Mic\nopen Spare Mic true",
        )?;
        // the ears on again, a segment torn by a kill keeps its microphone
        drop(rift(&root, &[], &["ears", "on"])?);
        until(|| Ok(log().matches("open Hall Mic").count() == 2))?;
        // killed frozen, a segment seen open there: left running, it may close first
        until(|| {
            drop(signal(&daemon, "STOP"));
            let closed = stream("ear-desk").map_or(0, |held| {
                held.iter()
                    .filter(|event| event["kind"] == "segment")
                    .count()
            });
            let open = fs::read_dir(format!("{root}/audio/1/ear-desk"))?.count() > closed;
            if !open {
                drop(signal(&daemon, "CONT"));
            }
            Ok(open)
        })?;
        drop(daemon);
        // the daemon gone, the ear waits a poll, finds it down and waits on; a frame in flight
        // as the ears close is refused `closed`, as the protocol has it
        let mut out = BufReader::new(desk.0.stdout.take().ok_or("stdout")?);
        let printed: Vec<String> = ((&mut out).lines().map_while(Result::ok))
            .skip(1)
            .filter(|line| *line != code("closed"))
            .take_while(|line| *line != code("down"))
            .collect();
        same(
            &printed.join(" "),
            &format!(
                "{{\"ears\":true}} {{\"ears\":false}} {{\"ears\":true}} {}",
                code("unknown")
            ),
        )?;
        same(
            &stop(spare, mouthcopy, "INT")?,
            "Some(0) rift device: stopped by SIGINT\n",
        )?;
        let (mut again, _) = serve(&root, &[("RIFT_DRAIN_MS", "200")])?;
        // once it is up, the ear connects again
        drop(
            ((&mut out).lines().map_while(Result::ok))
                .find(|line| line.contains(r#""track":"ear-desk""#))
                .ok_or("no reconnect")?,
        );
        same(
            &events(&root)?
                .iter()
                .find(|event| event["boot"] == 2u8 && event["e"]["kind"] == "segment")
                .map_or(Value::Null, |event| event["e"]["mic"].clone())
                .to_string(),
            "\"Hall Mic\"",
        )?;
        let (mut raw, _) = device(&root, r#"{"kind":"ear","name":"raw","op":"device"}"#, 0)?;
        let line = json!({"mic": 5u8, "op": "frame", "pcm": "00".repeat(640), "t": 1u8});
        same(&say(&mut raw, &line.to_string())?, &code("bad_frame"))?;
        drop(raw);
        // each failure one stderr line naming it, the device kept: an open waited on past
        // `$RIFT_OPEN_MS`, a refused one and exact zeros retried a poll doubled per failure
        let grant = "allow rift in System Settings \u{203a} Privacy & Security \u{203a} Microphone";
        fs::write(format!("{fake}/inputs/Stuck"), "block")?;
        fs::write(format!("{fake}/inputs/Broken"), "fail")?;
        fs::write(format!("{fake}/inputs/Silent"), "48000 1 0 0")?;
        let (begun, mut failing) = (Instant::now(), Vec::new());
        for (name, ending) in [
            ("Broken", ": refused".to_owned()),
            (
                "Silent",
                format!(
                    " gave no signal (nothing, or only exact zeros) after it opened: the microphone is not granted, is muted, or is a closed lid's; {grant}"
                ),
            ),
            (
                "Stuck",
                format!(" has not opened in 3000 ms: the microphone is not granted yet; {grant}"),
            ),
        ] {
            // the refused one hears nothing, so its doubled retries are seen at a fast poll
            let poll = if name == "Broken" { "100" } else { "1000" };
            let mut failed = spawn_with(
                &root,
                poll,
                &["device", "ear", &name.to_lowercase(), "--mic", name],
            )?;
            let (mut errs, mut first) = (
                BufReader::new(failed.0.stderr.take().ok_or("stderr")?),
                String::new(),
            );
            let _: usize = errs.read_line(&mut first)?;
            same(&first, &format!("rift device: \"{name}\"{ending}\n"))?;
            failing.push((failed, errs));
        }
        until(|| {
            Ok(log().matches("ask Broken").count() >= 3
                && log().matches("close Silent").count() >= 2)
        })?;
        // the third refusal waited a poll, then two: never sooner
        same(
            &format!(
                "{} {} {}",
                begun.elapsed() >= Duration::from_millis(300),
                stream("ear-silent")?.len(),
                stream("ear-stuck")?.len()
            ),
            "true 0 0",
        )?;
        // the ears off and on while the open waits: asked again, said once
        drop(rift(&root, &[], &["ears", "off"])?);
        drop(rift(&root, &[], &["ears", "on"])?);
        until(|| Ok(log().matches("ask Stuck").count() == 2))?;
        // the grant given, that open returns; the input heard, recording starts
        fs::write(format!("{fake}/inputs/Stuck"), "16000 1 0.5 0")?;
        fs::write(format!("{fake}/inputs/Silent"), "48000 1 0.5 0")?;
        until(|| {
            Ok(["ear-silent", "ear-stuck"].iter().all(|track| {
                stream(track)
                    .is_ok_and(|seen| seen.first().is_some_and(|event| event["kind"] == "track"))
            }))
        })?;
        // the open that returned is the second asked, never one asked anew; an input a starved
        // machine leaves silent past its poll may be asked again after, which proves nothing here
        let asked = log();
        let returned = asked.find("open Stuck").ok_or("never opened")?;
        same(
            &asked
                .get(..returned)
                .unwrap_or_default()
                .matches("ask Stuck")
                .count()
                .to_string(),
            "2",
        )?;
        // following the default while there is none: it says so and waits
        fs::remove_file(format!("{fake}/default"))?;
        let mut lost = spawn(&["device", "ear", "lost", "--mic"])?;
        let (mut astray, mut waiting) = (
            BufReader::new(lost.0.stderr.take().ok_or("stderr")?),
            String::new(),
        );
        let _: usize = astray.read_line(&mut waiting)?;
        same(
            &waiting,
            "rift device: waiting for a default input; the inputs present: Broken, Desk Mic, Hall Mic, Silent, Stuck\n",
        )?;
        // a Bluetooth default: followed while the Mac has no built-in input, then, once it has,
        // left for the built-in, a hole opening its segments; a device pinned to it still hears it
        let recorded = |track: &str, mic: &str| -> io::Result<bool> {
            Ok(stream(track).is_ok_and(|held| held.iter().any(|event| event["mic"] == mic)))
        };
        fs::write(format!("{fake}/inputs/Headset"), "16000 1 0.5 0 bluetooth")?;
        fs::write(format!("{fake}/default"), "Headset\n")?;
        until(|| Ok(recorded("ear-desk", "Headset")? && recorded("ear-lost", "Headset")?))?;
        fs::write(
            format!("{fake}/inputs/Built-in Mic"),
            "16000 1 0.25 0 builtin",
        )?;
        let mut phone = spawn(&["device", "ear", "phone", "--mic", "Headset"])?;
        until(|| {
            Ok(recorded("ear-desk", "Built-in Mic")?
                && recorded("ear-lost", "Built-in Mic")?
                && recorded("ear-phone", "Headset")?)
        })?;
        same(
            &stream("ear-lost")?
                .iter()
                .find(|event| event["mic"] == "Built-in Mic")
                .map_or(Value::Null, |event| event["kind"].clone())
                .to_string(),
            "\"hole\"",
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        // the devices wait on with the daemon down, until a stop signal ends each, so their runs
        // count toward coverage; nothing said twice, the rule's reasons once each
        same(&format!("{:?}", again.0.wait()?.code()), "Some(0)")?;
        let ruled = "rift device: \"Headset\" is Bluetooth and this Mac has no built-in input; hearing it, in call mode while the ears are on\nrift device: \"Headset\" is Bluetooth, which an open microphone puts in call mode; hearing \"Built-in Mic\" instead\n";
        let mut errs = [&mut desk, &mut phone]
            .map(|device| device.0.stderr.take().map(BufReader::new))
            .into_iter();
        let ending = [
            (desk, errs.next().flatten().ok_or("stderr")?, ruled, "TERM"),
            (lost, astray, ruled, "HUP"),
            (phone, errs.next().flatten().ok_or("stderr")?, "", "TERM"),
        ];
        for (device, rest, told, name) in ending.into_iter().chain(
            failing
                .into_iter()
                .map(|(device, rest)| (device, rest, "", "TERM")),
        ) {
            same(
                &stop(device, rest, name)?,
                &format!("Some(0) {told}rift device: stopped by SIG{name}\n"),
            )?;
        }
        Ok(())
    }

    /// An output that takes nothing (`take` 0) while the mouth holds a play: past
    /// `$RIFT_STALL_MS` with no report the daemon ends it `mouth`, its giver, its reply withheld,
    /// answered; the mouth told to drop it, its output taking no sample of it after. A giver that
    /// pauses past the stall time owes nothing, and its play plays on to its end.
    #[cfg(target_os = "macos")]
    #[test]
    fn stuck() -> Outcome {
        let root = temp("stuck")?;
        let fake = format!("{root}/fake");
        fs::create_dir_all(format!("{fake}/outputs"))?;
        fs::write(format!("{fake}/outputs/Desk"), "48000 1 0")?;
        fs::write(format!("{fake}/default"), "Desk\n")?;
        fs::write(format!("{fake}/take"), "0")?;
        // two seconds: room for a starved mouth's next report, short enough to wait out
        let (mut daemon, _) = serve(&root, &[("RIFT_STALL_MS", "2000")])?;
        let mut mouth = Daemon(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .env("RIFT_SPEAKER", &fake)
                .env("RIFT_POLL_MS", "100")
                .args(["device", "mouth", "desk", "--speaker"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        );
        until(|| {
            let mut conn = greet(&root).map_err(|cause| io::Error::other(cause.to_string()))?;
            Ok(say(&mut conn, r#"{"op":"health"}"#)?.contains("mouth-desk"))
        })?;
        let (_, stalled) = joined(giver(&root, "mouth-desk", 16_000, tone(16_000, 1.0)))?;
        same(
            &format!(
                "{} {} {}",
                stalled["reason"], stalled["played"], stalled["given"]
            ),
            r#""mouth" 0 8192"#,
        )?;
        // the mouth, told to drop the play, closes its output, which never took a sample of it;
        // only then is the output freed
        until(|| {
            Ok(fs::read_to_string(format!("{fake}/log"))
                .is_ok_and(|log| log.contains("close Desk")))
        })?;
        fs::remove_file(format!("{fake}/take"))?;
        same(
            &taken(&fake, "Desk")
                .iter()
                .all(|&sample| sample == 0)
                .to_string(),
            "true",
        )?;
        // a giver that pauses longer than the stall time, its mouth run dry and holding only the
        // interpolator's reach untaken, owes nothing: the play plays on to its end
        let mut paused = greet(&root)?;
        drop(say(
            &mut paused,
            r#"{"op":"play","rate":16000,"track":"mouth-desk"}"#,
        )?);
        drop(say(&mut paused, &give(&tone(16_000, 0.1)))?);
        // the time must pass for a stall to be possible: the stall time and a sweep's second over
        thread::sleep(Duration::from_millis(3500));
        drop(say(&mut paused, &give(&tone(16_000, 0.1)))?);
        drop(say(&mut paused, r#"{"op":"end"}"#)?);
        let resumed = next(&mut paused)?;
        same(
            &format!(
                "{} {resumed}",
                resumed
                    .contains(r#""dropped":0,"given":3200,"play":2,"played":3200,"reason":"end""#)
            ),
            &format!("true {resumed}"),
        )?;
        same(&signal(&mouth, "TERM")?, "")?;
        same(&format!("{:?}", mouth.0.wait()?.code()), "Some(0)")?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
    }

    /// `seconds` of a 1 kHz tone at half scale, 16-bit samples at `rate`.
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::float_arithmetic,
        reason = "a test signal"
    )]
    fn tone(rate: u32, seconds: f64) -> Vec<i16> {
        (0..(f64::from(rate) * seconds) as u32)
            .map(|at| {
                let phase = 2000.0f64 * PI * f64::from(at) / f64::from(rate);
                (phase.sin() * 16384.0f64).round() as i16
            })
            .collect()
    }

    /// A give of `sound`, its samples as lowercase hex.
    fn give(sound: &[i16]) -> String {
        let pcm = (sound.iter().flat_map(|sample| sample.to_le_bytes())).fold(
            String::new(),
            |mut text, byte| {
                write!(text, "{byte:02x}").unwrap_or_default();
                text
            },
        );
        json!({"op": "give", "pcm": pcm}).to_string()
    }

    /// A giver on its own thread: a play of `sound` at `rate` for `track`, given 8192 samples at a
    /// time, then `end`; its play's reply and last line.
    fn giver(
        root: &str,
        track: &str,
        rate: u32,
        sound: Vec<i16>,
    ) -> thread::JoinHandle<Result<(Value, Value), String>> {
        let (at, mouth) = (root.to_owned(), track.to_owned());
        thread::spawn(move || {
            let run = || -> Result<(Value, Value), Box<dyn Error>> {
                let mut conn = greet(&at)?;
                let asked = json!({"op": "play", "rate": rate, "track": mouth}).to_string();
                let reply: Value = serde_json::from_str(&say(&mut conn, &asked)?)?;
                conn.get_ref()
                    .set_read_timeout(Some(Duration::from_secs(50)))?;
                // each give, then `end`, answered `ok` until the play's last line, which has none
                let lines = sound.chunks(8192).map(give);
                for line in lines.chain([r#"{"op":"end"}"#.to_owned()]) {
                    // a play ended under it: the write may fail, its last line is still there
                    drop(send(&mut conn, &line));
                    let answer: Value = serde_json::from_str(&next(&mut conn)?)?;
                    if answer.get("ok").is_none() {
                        return Ok((reply, answer));
                    }
                }
                Ok((reply, serde_json::from_str(&next(&mut conn)?)?))
            };
            run().map_err(|cause| cause.to_string())
        })
    }

    /// A giver's reply and last line, once it has ended.
    fn joined(
        giving: thread::JoinHandle<Result<(Value, Value), String>>,
    ) -> Result<(Value, Value), Box<dyn Error>> {
        Ok(giving.join().map_err(|_panic| "the giver panicked")??)
    }

    /// The samples an output of `$RIFT_SPEAKER` at `fake` took, channel 0.
    fn taken(fake: &str, output: &str) -> Vec<i16> {
        fs::read(format!("{fake}/heard/{output}"))
            .unwrap_or_default()
            .chunks_exact(2)
            .map(|pair| {
                let byte = |at: usize| pair.get(at).copied().unwrap_or_default();
                i16::from_le_bytes([byte(0), byte(1)])
            })
            .collect()
    }

    /// A tone's frequency by its rising zero crossings and its level by its RMS, over `sound`'s
    /// samples at `rate` that are not silence: `"<Hz> <RMS>"`, rounded to tens and hundreds.
    #[expect(
        clippy::as_conversions,
        clippy::cast_precision_loss,
        clippy::float_arithmetic,
        clippy::single_call_fn,
        reason = "a test signal's measure, apart from the test it serves"
    )]
    fn measure(sound: &[i16], rate: u32) -> String {
        // silence (8 zeros or more in a row) left out, where a busy machine ran the mouth dry
        let (mut heard, mut zeros) = (Vec::new(), 0usize);
        for &sample in sound {
            if sample == 0 {
                zeros = zeros.saturating_add(1);
            } else {
                heard.extend(iter::repeat_n(0.0f64, if zeros < 8 { zeros } else { 0 }));
                heard.push(f64::from(sample));
                zeros = 0;
            }
        }
        let span = heard.as_slice();
        let rising = span
            .windows(2)
            .filter(|pair| matches!(pair, [low, high] if *low < 0.0f64 && *high >= 0.0f64))
            .count();
        let rms = (span.iter().map(|sample| sample * sample).sum::<f64>()
            / span.len().max(1) as f64)
            .sqrt();
        let hz = rising as f64 * f64::from(rate) / span.len().max(1) as f64;
        format!(
            "{} {}",
            (hz / 10.0f64).round() * 10.0f64,
            (rms / 100.0f64).round() * 100.0f64
        )
    }

    /// `device mouth <name> --speaker` over the fake outputs of `$RIFT_SPEAKER`: a play given at
    /// 16, 24 and 48 kHz sounds on a 48 and a 44.1 kHz output as the same tone at the same level, run
    /// to its end, one run, its track's frames naming the output and its segment closed at `done`;
    /// a hush at a set `take` plays exactly what the output took and drops the rest, with no further
    /// callback; a mouth run dry splits the runs and holes the track; the default changed mid-play
    /// reopens on the new output, keeping what is queued; with no output it says so and waits, the
    /// giver paced until one appears (past `$RIFT_STALL_MS` the stall rule would end the play); a
    /// refused open is said and retried; an output failing mid-play is reopened; the output lingers after a play, then
    /// closes; an older daemon is said once and waited out; the daemon gone, it drops its play and
    /// connects again; a stop signal ends it, exit 0.
    #[cfg(target_os = "macos")]
    #[test]
    fn mouth() -> Outcome {
        let root = temp("mouth")?;
        let fake = format!("{root}/fake");
        fs::create_dir_all(format!("{fake}/outputs"))?;
        fs::write(format!("{fake}/outputs/Desk"), "48000 2 5")?;
        fs::write(format!("{fake}/outputs/Hall"), "44100 1 0")?;
        fs::write(format!("{fake}/default"), "Desk\n")?;
        let (mut daemon, _) = serve(&root, &[("RIFT_SEGMENT_MS", "60000")])?;
        let spawn = |at: &str| -> io::Result<Daemon> {
            Ok(Daemon(
                Command::new(env!("CARGO_BIN_EXE_rift"))
                    .env("RIFT_ROOT", at)
                    .env("RIFT_SPEAKER", &fake)
                    .env("RIFT_POLL_MS", "100")
                    .env("RIFT_LINGER_MS", "300")
                    .args(["device", "mouth", "desk", "--speaker"])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?,
            ))
        };
        let mut desk = spawn(&root)?;
        let mut errs = BufReader::new(desk.0.stderr.take().ok_or("stderr")?);
        let mut said = |line: &str| -> Outcome {
            let mut got = String::new();
            let _: usize = errs.read_line(&mut got)?;
            same(&got, &format!("rift device: {line}\n"))
        };
        let log = || fs::read_to_string(format!("{fake}/log")).unwrap_or_default();
        let listed = |track: &str| -> io::Result<bool> {
            let mut conn = greet(&root).map_err(|cause| io::Error::other(cause.to_string()))?;
            Ok(say(&mut conn, r#"{"op":"health"}"#)?.contains(track))
        };
        until(|| listed("mouth-desk"))?;
        drop(rift(&root, &[], &["ears", "on"])?);
        // to its end: the same tone at each rate, on each output
        // the default set, then every output closed: it has seen the default, which would
        // otherwise be followed mid-play
        let closed = || Ok(log().matches("open ").count() == log().matches("close ").count());
        for (rate, output) in [(16_000u32, "Desk"), (24_000, "Hall"), (48_000, "Hall")] {
            fs::write(format!("{fake}/default"), format!("{output}\n"))?;
            until(closed)?;
            let before = taken(&fake, output).len();
            let (reply, last) = joined(giver(&root, "mouth-desk", rate, tone(rate, 0.5)))?;
            let given = rate.checked_div(2).unwrap_or_default();
            same(
                &format!("{} {}", reply["ok"], last),
                &format!(
                    r#"true {{"dropped":0,"given":{given},"play":{},"played":{given},"reason":"end","seq":{},"track":"mouth-desk"}}"#,
                    reply["play"], last["seq"]
                ),
            )?;
            let out_rate = if output == "Desk" { 48_000 } else { 44_100 };
            same(
                &measure(
                    taken(&fake, output).get(before..).unwrap_or_default(),
                    out_rate,
                ),
                "1000 11600",
            )?;
        }
        // every play's runs contiguous from 0, together what played: a busy machine may split a
        // run where the mouth ran dry, never lose or repeat a sample
        let contiguous = || -> Result<bool, Box<dyn Error>> {
            let held = events(&root)?;
            Ok(held
                .iter()
                .filter(|event| event["e"]["kind"] == "played")
                .all(|ended| {
                    let runs: Vec<&Value> = held
                        .iter()
                        .map(|event| &event["e"])
                        .filter(|event| {
                            event["kind"] == "sound" && event["play"] == ended["e"]["play"]
                        })
                        .collect();
                    let mut at = 0u64;
                    runs.iter().all(|run| {
                        let fits = run["from"] == at;
                        at = at.saturating_add(run["samples"].as_u64().unwrap_or_default());
                        fits
                    }) && ended["e"]["played"] == at
                }))
        };
        same(&contiguous()?.to_string(), "true")?;
        // the track's segments name their output, Desk's then Hall's, each closed at `done`
        let mut speakers: Vec<Value> = events(&root)?
            .into_iter()
            .map(|event| event["e"].clone())
            .filter(|event| event["kind"] == "segment")
            .map(|segment| segment["speaker"].clone())
            .collect();
        speakers.dedup();
        same(&json!(speakers).to_string(), r#"["Desk","Hall"]"#)?;
        // `rift play` gives a file, its odd last byte dropped: its reply, then its last line,
        // exit 0 at its end; a stop signal ends it, exit 1, the play hushed (`left`)
        let file = format!("{root}/tone.pcm");
        let mut bytes: Vec<u8> = (tone(16_000, 0.2).iter())
            .flat_map(|sample| sample.to_le_bytes())
            .collect();
        bytes.push(1);
        fs::write(&file, &bytes)?;
        let played = rift(&root, &[], &["play", "mouth-desk", "16000", &file])?;
        same(
            &format!(
                "{} {}",
                played.starts_with("0\n{\"ok\":true,\"play\":"),
                played.contains(r#""dropped":0,"given":3200,"#)
            ),
            "true true",
        )?;
        // a play given nothing ends at once, at its `end` or its hush, the mouth never told of it
        same(
            &rift(&root, &[], &["play", "mouth-desk", "16000", "/dev/null"])?
                .contains(r#""dropped":0,"given":0,"play":"#)
                .to_string(),
            "true",
        )?;
        let mut empty = greet(&root)?;
        drop(say(
            &mut empty,
            r#"{"op":"play","rate":16000,"track":"mouth-desk"}"#,
        )?);
        let quelled = rift(&root, &[], &["hush", "mouth-desk"])?;
        same(
            &format!(
                "{} {}",
                quelled.contains(r#""given":0,"ok":true"#)
                    && quelled.contains(r#""reason":"hush""#),
                next(&mut empty)?.contains(r#""reason":"hush""#)
            ),
            "true true",
        )?;
        let lasting: Vec<u8> = (tone(16_000, 5.0).iter())
            .flat_map(|sample| sample.to_le_bytes())
            .collect();
        fs::write(&file, lasting)?;
        let mut stopped = Daemon(
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .args(["play", "mouth-desk", "16000", &file])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?,
        );
        until(|| listed(r#""playing":{"mouth-desk""#))?;
        same(&signal(&stopped, "INT")?, "")?;
        let mut stop_line = String::new();
        let _: usize =
            BufReader::new(stopped.0.stderr.take().ok_or("stderr")?).read_line(&mut stop_line)?;
        same(
            &format!("{:?} {stop_line}", stopped.0.wait()?.code()),
            "Some(1) rift play: stopped by SIGINT\n",
        )?;
        until(|| {
            Ok(kinds(&root, "played", usize::MAX)?
                .lines()
                .last()
                .is_some_and(|line| line.contains(r#""reason":"left""#)))
        })?;
        // a hush once Hall has taken 100 ms (4410 samples at 44.1 kHz) and no more, of a play of
        // one level so its first sample taken shows: `played` is the given samples the output's
        // last position taken reached, its count from the fake's own record; the give after the
        // hush is refused and never counts; freed, the output takes only silence
        until(closed)?;
        let all = || {
            taken(&fake, "Desk")
                .len()
                .saturating_add(taken(&fake, "Hall").len())
        };
        let (budget, before) = (all().saturating_add(4410), taken(&fake, "Hall").len());
        fs::write(format!("{fake}/take"), budget.to_string())?;
        let hushing = giver(&root, "mouth-desk", 16_000, vec![8192i16; 16_000]);
        until(|| Ok(all() == budget))?;
        let hushed: Value = serde_json::from_str(
            rift(&root, &[], &["hush", "mouth-desk"])?
                .trim_start_matches("0\n")
                .trim_end(),
        )?;
        fs::remove_file(format!("{fake}/take"))?;
        let (_, last) = joined(hushing)?;
        let held = taken(&fake, "Hall");
        let window = held
            .get(before..before.saturating_add(4410))
            .unwrap_or_default();
        let lead = window
            .iter()
            .position(|&sample| sample != 0)
            .unwrap_or_default();
        // the positions the mouth read, stepped as it steps them
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the interpolator's positions, as the mouth steps them"
        )]
        let reached = ((1..4410usize.saturating_sub(lead))
            .fold(0.0f64, |at, _| at + 16_000.0f64 / 44_100.0f64)
            .floor() as u64)
            .saturating_add(1);
        same(
            &format!(
                "{} {} {} {}",
                hushed["played"] == reached,
                hushed["reason"],
                last["given"],
                hushed["dropped"]
                    == last["given"]
                        .as_u64()
                        .unwrap_or_default()
                        .saturating_sub(reached)
            ),
            r#"true "hush" 8192 true"#,
        )?;
        until(closed)?;
        same(
            &taken(&fake, "Hall")
                .get(before.saturating_add(4410)..)
                .unwrap_or_default()
                .iter()
                .all(|&sample| sample == 0)
                .to_string(),
            "true",
        )?;
        same(&contiguous()?.to_string(), "true")?;
        // a giver that stalls: the mouth runs dry, its runs split, its track holes
        until(closed)?;
        let (start, holes) = (
            taken(&fake, "Hall").len(),
            kinds(&root, "hole", usize::MAX)?.lines().count(),
        );
        let mut slow = greet(&root)?;
        drop(say(
            &mut slow,
            r#"{"op":"play","rate":16000,"track":"mouth-desk"}"#,
        )?);
        drop(say(&mut slow, &give(&tone(16_000, 0.1)))?);
        // 100 ms played, then 60 ms dry, at 44.1 kHz
        until(|| Ok(taken(&fake, "Hall").len() >= start.saturating_add(7056)))?;
        drop(say(&mut slow, &give(&tone(16_000, 0.1)))?);
        drop(say(&mut slow, r#"{"op":"end"}"#)?);
        let stalled: Value = serde_json::from_str(&next(&mut slow)?)?;
        let runs = kinds(&root, "sound", usize::MAX)?;
        same(
            &format!(
                "{} {}",
                stalled["played"],
                runs.matches(&format!(r#""play":{}"#, stalled["play"]))
                    .count()
                    >= 2
            ),
            "3200 true",
        )?;
        same(&contiguous()?.to_string(), "true")?;
        same(
            &(kinds(&root, "hole", usize::MAX)?.lines().count() >= holes.saturating_add(2))
                .to_string(),
            "true",
        )?;
        // the default changed mid-play, Hall held at 200 ms by `take` until Desk has opened:
        // reopened on the new output, what is queued kept
        until(closed)?;
        let (held_at, opens) = (
            all().saturating_add(8820),
            log().matches("open Desk").count(),
        );
        fs::write(format!("{fake}/take"), held_at.to_string())?;
        let moving = giver(&root, "mouth-desk", 16_000, tone(16_000, 1.0));
        until(|| Ok(all() == held_at))?;
        fs::write(format!("{fake}/default"), "Desk\n")?;
        until(|| Ok(log().matches("open Desk").count() > opens))?;
        fs::remove_file(format!("{fake}/take"))?;
        let (_, moved) = joined(moving)?;
        same(
            &format!("{} {}", moved["played"], moved["reason"]),
            r#"16000 "end""#,
        )?;
        same(&contiguous()?.to_string(), "true")?;
        same(
            &kinds(&root, "hole", usize::MAX)?
                .contains(r#""speaker":"Desk""#)
                .to_string(),
            "true",
        )?;
        // no output at all: said, the giver paced until one appears, well inside the stall time; a
        // refused open said and
        // retried
        until(closed)?;
        fs::remove_file(format!("{fake}/default"))?;
        let waiting = giver(&root, "mouth-desk", 16_000, tone(16_000, 0.2));
        said("waiting for a default output; the outputs present: Desk, Hall")?;
        fs::write(format!("{fake}/outputs/Broken"), "fail")?;
        fs::write(format!("{fake}/default"), "Broken\n")?;
        said(r#""Broken": refused"#)?;
        fs::write(format!("{fake}/default"), "Desk\n")?;
        let (_, waited) = joined(waiting)?;
        same(&waited["reason"].to_string(), r#""end""#)?;
        // an output failing mid-play while it stays listed: said, closed, its open refused while
        // it fails, then reopened, the play playing on to its end, every sample counted
        until(closed)?;
        let opened = taken(&fake, "Desk").len();
        let failing = giver(&root, "mouth-desk", 16_000, tone(16_000, 1.0));
        until(|| Ok(taken(&fake, "Desk").len() >= opened.saturating_add(4800)))?;
        fs::write(format!("{fake}/outputs/Desk"), "fail")?;
        said(r#""Desk" failed; opening it again"#)?;
        said(r#""Desk": refused"#)?;
        fs::write(format!("{fake}/outputs/Desk"), "48000 2 5")?;
        let (_, recovered) = joined(failing)?;
        same(
            &format!("{} {}", recovered["played"], recovered["reason"]),
            r#"16000 "end""#,
        )?;
        same(&contiguous()?.to_string(), "true")?;
        // it closes once its linger is out; how long that takes is the machine's to stretch
        until(closed)?;
        // the ears closed: it plays, and sends no frames
        drop(rift(&root, &[], &["ears", "off"])?);
        let frames = events(&root)?.len();
        let (_, deaf) = joined(giver(&root, "mouth-desk", 16_000, tone(16_000, 0.1)))?;
        let since = events(&root)?.len().saturating_sub(frames);
        same(
            &format!(
                "{} {}",
                deaf["reason"],
                kinds(&root, "", since)?.contains("segment")
            ),
            r#""end" false"#,
        )?;
        // the daemon drained mid-play: hushed (`stop`); the mouth drops its play and, the daemon
        // back, connects again
        let draining = giver(&root, "mouth-desk", 16_000, tone(16_000, 2.0));
        until(|| listed(r#""playing":{"mouth-desk""#))?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        drop(draining.join());
        same(
            &kinds(&root, "played", usize::MAX)?
                .lines()
                .last()
                .unwrap_or_default()
                .contains(r#""reason":"stop""#)
                .to_string(),
            "true",
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")?;
        let (mut again, _) = serve(&root, &[])?;
        until(|| listed("mouth-desk"))?;
        // an older daemon, which plays nothing: said once, waited out as a refusal
        let other = format!("{root}/other");
        let hello = format!(
            r#"{{"boot":1,"ok":true,"pid":1,"proto":1,"since":"x","state":"ready","version":"{}"}}"#,
            version()?
        );
        self::fake(
            &other,
            &[
                &hello,
                r#"{"ears":false,"ok":true,"ping":1,"track":"mouth-desk"}"#,
            ],
        )?;
        let mut older = spawn(&other)?;
        let mut printed = BufReader::new(older.0.stdout.take().ok_or("stdout")?);
        same(
            &next(&mut printed)?,
            r#"{"ears":false,"ok":true,"ping":1,"track":"mouth-desk"}"#,
        )?;
        let older_errs = BufReader::new(older.0.stderr.take().ok_or("stderr")?);
        same(&signal(&older, "TERM")?, "")?;
        same(
            &format!(
                "{:?} {}",
                older.0.wait()?.code(),
                older_errs
                    .lines()
                    .map_while(Result::ok)
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
            &format!(
                "Some(0) rift device: the daemon {} plays nothing; fix: rift setup\nrift device: stopped by SIGTERM",
                version()?
            ),
        )?;
        // a stop signal ends it, exit 0
        same(&signal(&desk, "TERM")?, "")?;
        said("stopped by SIGTERM")?;
        same(&format!("{:?}", desk.0.wait()?.code()), "Some(0)")?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", again.0.wait()?.code()), "Some(0)")
    }
}

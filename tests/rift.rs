//! End to end: the built `rift` over temp roots, against docs/protocol.html and the daemon rules;
//! a daemon a test starts is drained by it, or killed when its guard drops.

// the binary's dependencies, unused by the suite
use hotswap as _;
use signal_hook as _;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use std::env;
    use std::error::Error;
    use std::fs::{self, File, OpenOptions, Permissions};
    use std::io::{self, BufRead as _, BufReader, Write as _};
    use std::iter;
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _, symlink};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::process::{Child, Command, ExitStatus, Stdio, id};
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
            drop(
                Command::new("hdiutil")
                    .args(["detach", "-force", &self.0])
                    .output(),
            );
        }
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
        let out = Command::new(env!("CARGO_BIN_EXE_rift"))
            .env("RIFT_ROOT", root)
            .envs(envs.iter().copied())
            .args(args)
            .stdin(Stdio::null())
            .output()?;
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
    fn next(conn: &mut BufReader<UnixStream>) -> io::Result<String> {
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

    /// `done` asked every 10 ms until it holds, 10 s at most: the sweep moves segments off the
    /// request path.
    fn until<F: Fn() -> io::Result<bool>>(done: F) -> Outcome {
        let started = Instant::now();
        while !done()? {
            if started.elapsed() > Duration::from_secs(10) {
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
        let health = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "proto": 1u8, "quiet": false, "seq": 1u8, "since": since, "state": "ready", "version": built, "window": 600u16});
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
        let quieted = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "proto": 1u8, "quiet": true, "seq": 2u8, "since": since, "state": "ready", "version": built, "window": 600u16});
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
        let degraded = json!({"archive": null, "boot": 1u8, "devices": [], "ears": false, "ok": true, "pid": pid, "proto": 1u8, "quiet": false, "seq": 3u8, "since": since, "state": "degraded", "version": built, "window": 600u16});
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
        let out = Command::new("kill")
            .args([format!("-{name}"), daemon.0.id().to_string()])
            .output()?;
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
            let out = Command::new("hdiutil").args(args).output()?;
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
                fs::exists(format!("{root}/serve.err"))?
            ),
            "launchd's false",
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
        let cli = |after: &str| {
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .args(["feed", "--after", after])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
        };
        same(
            &rift(&root, &[], &["quiet", "off"])?,
            "0\n{\"changed\":true,\"ok\":true,\"seq\":2}\n",
        )?;
        // a CLI feed has subscribed once its first line arrives
        let (mut held, mut lost) = (cli("2:1")?, cli("9:9")?);
        let mut outs = Vec::new();
        for child in [&mut held, &mut lost] {
            let mut out = BufReader::new(child.stdout.take().ok_or("stdout")?);
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
            io::Read::read_to_string(&mut lost.stderr.take().ok_or("stderr")?, &mut said)?;
        same(
            &said,
            "rift feed: the log does not hold that position; from this boot's first event\n",
        )?;
        same(
            &format!(
                "{:?} {:?} {:?}",
                held.wait()?.code(),
                lost.wait()?.code(),
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
            let health: Value =
                serde_json::from_str(&say(&mut greet(&root)?, r#"{"op":"health"}"#)?)?;
            same(
                &format!("{limit} {}", health["state"]),
                &format!("{limit} \"ready\""),
            )?;
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
            &format!("{}", Command::new("mkfifo").arg(&log).status()?),
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
            &Command::new("mkfifo")
                .arg(&stuck)
                .status()?
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
        same(
            &rift(&root, &[], &["device", "ear", "x", "/nonexistent.pcm"])?,
            "1\nrift device: No such file or directory (os error 2)\n",
        )?;
        same(
            &rift(&root, &[], &["device", "nose", "x", &file])?,
            &format!("1\n{}\n", code("bad_request")),
        )?;
        let long = format!("{root}/long.pcm");
        fs::write(&long, vec![1; 64_000])?;
        let spawn = |args: &[&str]| {
            Command::new(env!("CARGO_BIN_EXE_rift"))
                .env("RIFT_ROOT", &root)
                .args(args)
                .stdout(Stdio::piped())
                .spawn()
        };
        let (mut listener, mut player) = (
            spawn(&["listen", "ear-long"])?,
            spawn(&["device", "ear", "long", &long])?,
        );
        let mut heard = String::new();
        let _: usize =
            BufReader::new(listener.stdout.take().ok_or("stdout")?).read_line(&mut heard)?;
        same(
            &serde_json::from_str::<Value>(&heard)?["pcm"].to_string(),
            &format!("\"{}\"", "01".repeat(640)),
        )?;
        drop(rift(&root, &[], &["ears", "off"])?);
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        let mut said = String::new();
        let _: usize =
            io::Read::read_to_string(&mut player.stdout.take().ok_or("stdout")?, &mut said)?;
        let lines: Vec<&str> = said.lines().collect();
        same(
            &format!(
                "{} {} {:?} {:?} {:?}",
                lines.get(1).unwrap_or(&""),
                lines.last().unwrap_or(&""),
                player.wait()?.code(),
                listener.wait()?.code(),
                daemon.0.wait()?.code()
            ),
            &format!(
                "{{\"ears\":false}} {} Some(1) Some(0) Some(0)",
                code("unknown")
            ),
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
        let long = format!("{root}/long.pcm");
        fs::write(&long, vec![1; 64_000])?;
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
        same(&format!("{:?}", player.0.wait()?.code()), "Some(0)")?;
        same(
            &paused()?
                .iter()
                .any(|event| event["kind"] == "hole" && event["samples"].as_u64() >= Some(4_800))
                .to_string(),
            "true",
        )?;
        same(
            &say(&mut greet(&root)?, r#"{"op":"shutdown"}"#)?,
            r#"{"ok":true}"#,
        )?;
        same(&format!("{:?}", daemon.0.wait()?.code()), "Some(0)")
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
        let usage = "2\nusage: rift serve | status | quiet on|off | ears on|off | keep <window s> [<archive>] | feed [--after <boot>:<seq>] | listen <track> | read <track> <from us> <to us> | device ear|mouth <name> <file.pcm> [--fast] | setup [--print | --remove] | upgrade [--check | --back | --retry] | version\n";
        for args in [
            &[][..],
            &["device", "ear", "a"],
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
        // the hello answered, the request read, then the connection ends with no reply
        fake(&root, &[&greeted])?;
        same(
            &rift(&root, &[], &["quiet", "on"])?,
            "1\n{\"code\":\"unknown\",\"ok\":false}\n",
        )
    }

    /// `setup --print` prints the plist on hotswap's link and writes nothing; without `launchctl`
    /// nothing is written; against a stub `launchctl` (`launchctl.log` its calls and, at each
    /// bootstrap, the root's mode when it is 0700; `loaded/` its jobs; a bootout letting go one
    /// `print` later) over a HOME hotswap already laid out, it retires an earlier install (another
    /// label running the link's `serve`), moving its root here while this one is absent and
    /// leaving it otherwise, has hotswap link `~/.local/bin/rift` and load the upgrade agent,
    /// makes the root 0700 before the agent loads, bootstraps a new plist, kickstarts the same one
    /// loaded (its respawn held back a second, as by `ThrottleInterval`), reloads a changed one,
    /// returns only once the loaded daemon answers health ready, fails on a failed bootstrap, a
    /// job still loaded past `$RIFT_BOOTOUT_MS` or none ready past `$RIFT_READY_MS`; `upgrade`
    /// swaps under the manifest, green once the swapped daemon answers with the new version;
    /// `--remove` has hotswap forget the layout and the upgrade agent, then boots the daemon and
    /// any earlier install out and removes their plists and the link.
    #[test]
    fn setup() -> Outcome {
        let root = temp("setup")?;
        let (home, bin) = (format!("{root}/home"), format!("{root}/bin"));
        let file = format!("{home}/Library/LaunchAgents/dev.gigantic.rift.plist");
        let exe = format!("{home}/.local/bin/rift");
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
        same(
            &run("/usr/bin:/bin", &[], &["--print"])?,
            &format!("0\n{}", plist(&data)),
        )?;
        // no RIFT_ROOT: the default under HOME
        let out = Command::new(env!("CARGO_BIN_EXE_rift"))
            .env("HOME", &home)
            .env_remove("RIFT_ROOT")
            .args(["setup", "--print"])
            .output()?;
        same(
            &String::from_utf8(out.stdout)?,
            &plist(&format!("{home}/.local/state/rift")),
        )?;
        // a relative RIFT_ROOT: made absolute against where setup ran, as launchd runs from /
        let relative = Command::new(env!("CARGO_BIN_EXE_rift"))
            .current_dir(&root)
            .env("HOME", &home)
            .env("RIFT_ROOT", "data")
            .args(["setup", "--print"])
            .output()?;
        same(&String::from_utf8(relative.stdout)?, &plist(&data))?;
        same(
            &run("/usr/bin", &[], &[])?,
            "1\nrift setup: launchctl: No such file or directory (os error 2) (setup installs a launchd agent, macOS only; --print prints the plist)\n",
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
start() {{ [ -f {root}/dead ] && return; RIFT_ROOT=$(sed -n 's|.*<key>RIFT_ROOT</key><string>\(.*\)</string>.*|\1|p' $1) {serve} serve >/dev/null 2>&1 & echo $! > {root}/loaded/$(basename $1 .plist); }}
case $1 in
print) [ -f {root}/loaded/$job.going ] && {{ rm {root}/loaded/$job.going; exit 0; }}; [ -f {root}/loaded/$job ] || exit 113;;
bootout) kill $(cat {root}/loaded/$job) 2>/dev/null; [ -f {root}/stuck ] || mv {root}/loaded/$job {root}/loaded/$job.going;;
bootstrap) case $3 in *.upgrade.plist) touch {root}/loaded/$(basename $3 .plist); exit;; esac; held=$(sed -n 's|.*<key>RIFT_ROOT</key><string>\(.*\)</string>.*|\1|p' $3); [ -n "$(find "$held" -maxdepth 0 -perm 700)" ] && echo root 0700 >> {root}/launchctl.log; [ -f {root}/fail ] && {{ echo 'Bootstrap failed: 5: Input/output error' >&2; exit 5; }}; start $3;;
kickstart) kill $(cat {root}/loaded/${{3##*/}}); {{ sleep 1; start {file}; }} >/dev/null 2>&1 &;;
esac
"#,
                serve = env!("CARGO_BIN_EXE_rift")
            ),
        )?;
        fs::write(
            format!("{bin}/curl"),
            "#!/bin/sh\nprintf https://github.com/jgeschwendt/rift/releases/tag/v1\n",
        )?;
        for tool in ["curl", "launchctl"] {
            fs::set_permissions(format!("{bin}/{tool}"), Permissions::from_mode(0o755))?;
        }
        // hotswap's layout, its version 1 this build
        let layout = format!("{home}/.local/share/hotswap/rift");
        let lay = || -> Outcome {
            fs::create_dir_all(format!("{layout}/1"))?;
            symlink(env!("CARGO_BIN_EXE_rift"), format!("{layout}/1/rift"))?;
            Ok(symlink("1", format!("{layout}/current"))?)
        };
        lay()?;
        let linked = format!(
            "linked {exe} \u{2192} {}\n",
            fs::canonicalize(env!("CARGO_BIN_EXE_rift"))?.display()
        );
        let stubbed = format!("{bin}:/usr/bin:/bin");
        let gui = format!(
            "gui/{}",
            String::from_utf8(Command::new("id").arg("-u").output()?.stdout)?.trim()
        );
        let job = format!("{gui}/dev.gigantic.rift");
        let agents = format!("{home}/Library/LaunchAgents");
        let upgrade = format!("{agents}/dev.gigantic.rift.upgrade.plist");
        let tended = format!(
            "print {gui}/dev.gigantic.rift.upgrade\nprint-disabled {gui}\nbootstrap {gui} {upgrade}\n"
        );
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
        same(
            &format!("{}{}", run(&stubbed, &[], &[])?, calls()?),
            &format!(
                "0\nlaunchctl bootout {former}\nmoved {old} to {data}\nremoved {earlier}\n{linked}wrote {file}\nlaunchctl bootstrap {gui} {file}\nprint {job}\nprint {former}\nbootout {former}\nprint {former}\nprint {former}\n{tended}print {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n"
            ),
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
        same(&fs::read_to_string(&file)?, &plist(&data))?;
        same(&fs::exists(format!("{file}.new"))?.to_string(), "false")?;
        same(
            &format!("{}{}", run(&stubbed, &[], &[])?, calls()?),
            &format!(
                "0\n{linked}wrote {file}\nlaunchctl kickstart -k {job}\nprint {job}\n{tended}kickstart -k {job}\n"
            ),
        )?;
        // the swap returns once the respawn, held back as by ThrottleInterval, answers ready
        let health: Value = serde_json::from_str(&say(&mut greet(&data)?, r#"{"op":"health"}"#)?)?;
        same(&health["state"].to_string(), "\"ready\"")?;
        same(
            &fs::read_link(&exe)?.display().to_string(),
            &format!("{layout}/current/rift"),
        )?;
        // the swap: setup through the link, its kickstart, then the smoke against the new daemon
        same(
            &format!(
                "{}{}",
                rift(
                    &data,
                    &[
                        ("HOME", home.as_str()),
                        ("HOTSWAP_TEST_HOME", home.as_str()),
                        ("PATH", stubbed.as_str())
                    ],
                    &["upgrade"]
                )?,
                calls()?
            ),
            &format!("0\nrift 1: green, 2 smoke probes\nprint {job}\n{tended}kickstart -k {job}\n"),
        )?;
        same(
            &rift(&data, &[("HOME", home.as_str())], &["upgrade", "--x"])?,
            "2\nusage: rift upgrade [--check | --back | --retry]\n",
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
        same(
            &format!("{}{}", rerun(&[], &[])?, calls()?),
            &format!(
                "0\n{linked}wrote {file}\nlaunchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\nprint {job}\n{tended}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n"
            ),
        )?;
        // an earlier install, unloaded, beside a root that exists: retired, its root left alone
        plant(&old)?;
        fs::write(format!("{root}/fail"), "")?;
        same(
            &format!("{}{}", run(&stubbed, &[], &[])?, calls()?),
            &format!(
                "1\nremoved {earlier}\n{linked}wrote {file}\nlaunchctl bootout {job}\nrift setup: launchctl bootstrap {gui} {file}: Bootstrap failed: 5: Input/output error\nprint {job}\nprint {former}\nprint {former}\n{tended}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n"
            ),
        )?;
        same(
            &fs::exists(format!("{old}/events.ndjson"))?.to_string(),
            "true",
        )?;
        fs::remove_file(format!("{root}/fail"))?;
        fs::write(format!("{root}/loaded/dev.gigantic.rift"), "")?;
        fs::write(format!("{root}/stuck"), "")?;
        same(
            &format!("{}{}", rerun(&[], &[("RIFT_BOOTOUT_MS", "0")])?, calls()?),
            &format!(
                "1\n{linked}wrote {file}\nlaunchctl bootout {job}\nrift setup: dev.gigantic.rift still loaded 0 ms after its bootout\nprint {job}\n{tended}print {job}\nbootout {job}\nprint {job}\n"
            ),
        )?;
        fs::remove_file(format!("{root}/stuck"))?;
        // a load that never answers ready fails past $RIFT_READY_MS
        fs::write(format!("{root}/dead"), "")?;
        same(
            &format!(
                "{}{}",
                run(&stubbed, &[("RIFT_READY_MS", "0")], &[])?,
                calls()?
            ),
            &format!(
                "1\n{linked}wrote {file}\nlaunchctl bootout {job}\nlaunchctl bootstrap {gui} {file}\nrift setup: dev.gigantic.rift not ready 0 ms after its load; see {data}/serve.err\nprint {job}\n{tended}print {job}\nbootout {job}\nprint {job}\nprint {job}\nbootstrap {gui} {file}\nroot 0700\n"
            ),
        )?;
        fs::remove_file(format!("{root}/dead"))?;
        fs::write(format!("{root}/loaded/dev.gigantic.rift"), "")?;
        plant(&old)?;
        same(
            &format!("{}{}", rerun(&["--remove"], &[])?, calls()?),
            &format!(
                "0\nremoved {earlier}\nlaunchctl bootout {job}\nremoved {file}\nremoved {exe}\nprint {gui}/dev.gigantic.rift.upgrade\nprint {former}\nprint {former}\nprint {job}\nbootout {job}\nprint {job}\nprint {job}\n"
            ),
        )?;
        same(
            &format!("{}{}", rerun(&["--remove"], &[])?, calls()?),
            &format!("0\nprint {gui}/dev.gigantic.rift.upgrade\nprint {job}\nprint {job}\n"),
        )?;
        same(
            &format!("{} {}", fs::exists(&upgrade)?, fs::exists(&layout)?),
            "false false",
        )
    }
}

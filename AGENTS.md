# rift

A launchd-owned daemon that keeps one append-only event log and serves it over a unix socket, and
carries an audio stream from the devices that connect to it onto one clock, kept on disk for a
window, then archived or deleted. No microphone yet: the one device is `rift device`, a file player.
The design is `docs/index.html`, the wire contract `docs/protocol.html`; the stream is in neither
yet, only under Contract below.

## Commands

```sh
mise run check     # the gate: fmt, clippy -D warnings, the end-to-end suite at 100 % line coverage
mise run cover     # the suite under llvm-cov alone, naming any line it misses
mise run dist      # this host's release archive, the next YYYY.M.N stamped, to dist/ (unsigned)
mise run install   # a dev build to ~/.local/bin/rift-next, beside the release; setup never wires it
```

The gate runs in hk's pre-push (`hk install --mise` once per clone) and in CI. `Cargo.toml`'s
`[lints]` tables are kept by hand, held by no gate; each `allow` carries its reason.

## Map

| path | what |
| --- | --- |
| `.github/workflows/release.yaml` | a GitHub Release per merge to main, `YYYY.M.N`, `aarch64-apple-darwin` and `x86_64-unknown-linux-gnu`, signed by packslip |
| `src/main.rs` | the whole binary: `serve`, the clients `status`, `quiet`, `ears`, `keep`, `feed`, `listen`, `read`, the file-fed `device`, then `setup`, `upgrade`, `version` |
| `build.rs` | the build identity: `$RIFT_VERSION` (the release workflow's), else the crate version; then the short commit, `-dirty` over uncommitted work, `unknown` outside git |
| `tests/rift.rs` | the end-to-end suite: the built binary over temp roots via `RIFT_ROOT`, stub `launchctl` and `curl` planted on `PATH` and a temp `HOME` (also `HOTSWAP_TEST_HOME`) hotswap's layout is laid out in for `setup` and `upgrade`; a `Daemon` guard kills every `serve` it starts, so a boot whose lines must count toward coverage is drained by `shutdown` instead: a killed process writes no profile |
| `docs/index.html` | the design page: what rift is today and what it will be |
| `docs/protocol.html` | the wire contract, protocol 1 |

## Environment

| var | effect |
| --- | --- |
| `RIFT_BOOTOUT_MS` | `setup`'s wait for launchd to let go of a job after a bootout, default 30000 |
| `RIFT_DRAIN_MS` | the drain's budget for open connections before they are cut, default 1000 (launchd's `ExitTimeOut` is 3 s) |
| `RIFT_LOG_CAP` | the size at which `serve.log` and `serve.err` each rotate to `.1`, default 1 MiB |
| `RIFT_PING_MS` | how long after a device's last ping a frame's reply carries the next, default 10000 |
| `RIFT_READY_MS` | `setup`'s wait for the loaded daemon to answer health ready, default 30000 |
| `RIFT_ROOT` | the state root, default `~/.local/state/rift`; the socket path under it must fit 103 bytes |
| `RIFT_SEGMENT_MS` | a segment's length, in whole 20 ms frames, at least one, default 10000 |

## State

Under `<root>` (0700): `events.ndjson`, the only durable state beside the audio;
`audio/<boot>/<track>/<n>.pcm`, each a segment's raw frames appended as they arrive, the closed
ones still in the window and each track's open one; `serve.err`, launchd's stderr; and `run/`
(0700): `rift.sock`, `serve.lock` (the `flock` that admits one instance), `serve.pid` (a hint),
`serve.log` (start, refused, accept, degraded, exit and panic lines). Under an archive directory:
`audio/<boot>/<track>/<n>.pcm` for every segment that has left the window, and `index.ndjson`, its
`segment` event as logged, one line each; nothing deletes there. The agent is
`~/Library/LaunchAgents/dev.gigantic.rift.plist`, its program `~/.local/bin/rift serve`: hotswap's
link through `~/.local/share/hotswap/rift/current`, never the executable that ran setup. hotswap
owns `dev.gigantic.rift.upgrade` (`rift upgrade` every 900 s) and its record under
`~/.local/state/hotswap/`.

## Contract

### `rift serve`

The daemon, a foreground process under launchd. Start: signals armed first, so a stop during the
boot drains; a socket path over 103 bytes, a `run/` not this user's or reachable by group or other,
or a held lock is one stderr line, a `refused` log line and exit 1. Otherwise it creates the root
and `run/` 0700 when absent, holds an exclusive `flock` on `run/serve.lock` for life, cuts a torn
last line off the log and derives from it `boot`, `quiet`, `ears`, the window and archive, and
the closed segments still under `audio/`, binds `run/serve.new`, writes `run/serve.pid`, appends
the `boot` event, closes out the segments a kill left open (below), renames the socket over
`run/rift.sock`, logs its start and prints its health line on stdout: ready.

The socket speaks `docs/protocol.html` (a hello, then `health`, `quiet`, `feed` and `shutdown`)
and the stream's ops below, all protocol 1: new ops, events, fields and codes are additive there.
Errors are typed codes. SIGTERM, SIGINT and SIGHUP take the shutdown path: stop accepting, close
the open segments, append `stop`, end the feeds, let open connections ask `health` inside
`$RIFT_DRAIN_MS`, cut them, remove `serve.pid` and the socket while the pid file still holds its
pid, log the exit, exit 0. A failed append answers `io`, changes nothing and reads `degraded`
until an append succeeds.

### The stream

Time is the daemon's clock, microseconds since the epoch, the one timeline. A frame is 20 ms of
16 kHz mono 16-bit little-endian PCM, 640 bytes, sent as 1280 lowercase hex digits: the socket
stays one JSON line per message, and hex needs no dependency. A track is `<kind>-<name>`, kind
`ear` or `mouth` (what that device played, as played), name 1–64 of `[A-Za-z0-9_-]`.

| op | request | reply |
| --- | --- | --- |
| `device` | `{"kind":"ear","name":"mac","op":"device"}` | `{"ears":true,"ok":true,"ping":<t>,"track":"ear-mac"}`; the connection is that track's device |
| `ears` | `{"on":true,"op":"ears"}` | as `quiet`: `{"changed":true,"ok":true,"seq":n}` |
| `frame` | `{"op":"frame","pcm":"<hex>","t":<device time>}`, a device's | `{"ok":true,"t":<its time on the track>}` |
| `keep` | `{"archive":"/abs/dir" or null,"op":"keep","window":600}` | `{"ok":true,"seq":n}`, then a sweep |
| `listen` | `{"op":"listen","track":"ear-mac"}` | `{"ok":true}`, then each frame placed on it, `{"pcm":"<hex>","t":<t>}`, live as a feed is |
| `pong` | `{"op":"pong","ping":<t>,"t":<device time>}`, a device's | none |
| `read` | `{"from":<t>,"op":"read","to":<t>,"track":"ear-mac"}`, `from` < `to` | `{"ok":true}`, then the range's frames from `from`, 20 ms apart, silence where nothing is held, `to` capped at now; then end of stream |

Codes: `bad_frame` (not 640 bytes of hex), `closed` (a frame while the ears are closed; not
taken), `gone` (a `read` from before now less the window), `taken` (a device for a track with one
live); a mistyped field, an unknown kind or name, `frame` or `pong` off a device, `device`, `feed`,
`listen` or `read` on one: `bad_request`. Health adds `archive`, `devices` (the live tracks),
`ears` and `window`; the ears default closed, the window 600 s, no archive.

The clock: any reply carrying `ping` (the device reply, then a frame's once `$RIFT_PING_MS` has
passed) is answered by a pong with the device's clock; the daemon takes its time at the ping and
at the pong's arrival, and places each later frame at its `t` plus the midpoint less the device's
time (until a first pong, at its `t`). Placement: a track's first frame this boot starts it
(`track` event); a frame half a frame or more past the track's end closes the open segment and is
a `hole` from that end, never written; any other frame goes at the end, so the track has no
jitter. Frames append to `audio/<boot>/<track>/<n>.pcm` as they arrive, each before its reply and
its listeners; a segment closes (its `segment` event) once `$RIFT_SEGMENT_MS` long, at a hole, as
its device leaves, as the ears close and in the drain. The ears closing tells every device
`{"ears":false}`, opening `{"ears":true}`.

| kind | fields | when |
| --- | --- | --- |
| `ears` | `on` | the ears opened or closed |
| `hole` | `samples`, `start`, `track` | a gap in a track's frames this boot, as the frame after it arrives |
| `keep` | `archive`, `window` | the window or archive set |
| `segment` | `n`, `path` (under the root), `samples`, `start`, `track` | a segment closed |
| `track` | `start`, `track` | a track's first frame this boot |

The sweep, at each segment's close, each `keep` and each boot: a closed segment that ended more
than the window ago is copied into the archive at its `path`, its event as logged appended to the
archive's `index.ndjson`, then removed from `audio/`; with no archive it is deleted. One that fails
stays for the next sweep. Boot: a `.pcm` under `audio/` with no `segment` event is the open segment
of a killed boot, cut to whole frames and closed with its event, its start the end of its track's
last `track`, `hole` or `segment` event in that boot.

### `rift status`, `rift quiet on|off`, `rift ears on|off`, `rift keep <window s> [<archive>]`, `rift feed [--after <boot>:<seq>]`, `rift listen <track>`, `rift read <track> <from us> <to us>`

Clients of the socket. Each prints the daemon's reply (a feed, a listen and a read, in its place,
each line after it until the stream ends), exit 0 on `"ok":true`; `keep`'s archive resolves against the
working directory. No connection, or none past hello: `{"code":"down","ok":false}`;
a request sent and never answered: `{"code":"unknown","ok":false}`; both exit 1. A daemon of
another protocol is one stderr line naming both and the fix (`rift setup`), exit 1; another
version, the same line beside the reply.

### `rift device ear|mouth <name> <file.pcm> [--fast]`

A device, its own process: the file read first (unreadable: `rift device: <cause>`, exit 1), then
connected as the track, its reply printed, every ping answered. It sends the file's whole frames
(a short tail dropped), stamped from now and paced in real time, or with `--fast` as fast as each
is answered, stamped as if captured ending now. Notices and refusals print as they come; exit 0
once every frame is answered, `{"code":"unknown","ok":false}` and exit 1 if the connection ends
first. A refused device request prints as any client's does.

### `rift setup`

The plist for `dev.gigantic.rift`: `ProgramArguments` `~/.local/bin/rift serve`,
`EnvironmentVariables` the root it ran with, `KeepAlive` on an unclean exit only (a drain's exit 0,
as after `shutdown`, stays down), `RunAtLoad`, `ThrottleInterval` 10,
`ExitTimeOut` 3, `StandardErrorPath` `<root>/serve.err`. `setup --print` prints it and writes
nothing. `setup`, `launchctl` resolved from `PATH` (none, as on Linux: one line, exit 1, before
anything is written), `gui/<id -u>` the domain, each call that changes something printed:

1. an earlier install, any other agent in `~/Library/LaunchAgents` whose program is `~/.local/bin/rift`
   `serve`, is booted out; its root moves to this one while this one does not exist (`moved <old>
   to <new>`); its plist is removed (since 2026-11-22: an earlier build ran under another label and
   root);
2. `hotswap::ensure("rift", manifest)`: a mise install of a newer version than the active one moved
   into `~/.local/share/hotswap/rift/<version>/`, else, with none active, the latest release
   installed by packslip under the manifest's signer; `~/.local/bin/rift` linked through its
   `current` (`linked <link> → <its target>`); the upgrade agent, hotswap's alone. Its failure:
   `rift setup: <hotswap's line>`, exit 1;
3. the root is made 0700 before the agent loads, since launchd would otherwise create it 0744 as
   `serve.err`'s parent;
4. the plist by temp + rename (`wrote <path>`);
5. the same plist already loaded: `kickstart -k`, the swap; else booted out when loaded, then
   `bootstrap`;
6. health polled every 100 ms until a daemon answers ready (after a swap, not the one it
   replaced), at most `$RIFT_READY_MS`: `<label> not ready <ms> ms after its load; see
   <root>/serve.err`, exit 1. launchd holds a respawn until 10 s after the last launch, so a swap
   soon after a load waits that out.

After each bootout `launchctl print` is polled every 100 ms until it fails, at most
`$RIFT_BOOTOUT_MS`: `<label> still loaded <ms> ms after its bootout`, exit 1. `setup --remove`
first has hotswap forget the layout, its record and the upgrade agent (refused while a swap runs),
then boots out every earlier install and `dev.gigantic.rift`, removes their plists and
`~/.local/bin/rift`, and leaves the root alone. Idempotent.

### `rift upgrade [--check | --back | --retry]`

`hotswap::upgrade("rift", manifest, args)`, its code the exit code (0 green or settled, 1 red, 2
unable to try; another argument `usage: rift upgrade [--check | --back | --retry]`, 2). The
manifest: repo `jgeschwendt/rift`, bin `rift`, setup `rift setup`, smoke `rift version` then
`/bin/sh -c` polling `rift status` once a second, 30 tries, until its health's `version` is `rift
version`'s; no agents to kickstart and no drain step, since setup's own `kickstart -k` is the swap
and its SIGTERM the daemon's drain. The signer is `ps1_fby5zwdz6ddvk4frxkhs4mmpny`,
what `packslip pin` prints for a release's `packslip.sigstore.json`; `v2026.11.0` was cut before
the pin, so an install of it cannot upgrade.

### `rift version`

Stdout `<release>-<build>`, the hello's and health's `version`; exit 0. `<release>` is the release
workflow's `YYYY.M.N` (`$RIFT_VERSION`, which `build.rs` stamps), a checkout's build the crate
version.

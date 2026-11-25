# rift

A launchd-owned daemon: one append-only event log served over a unix socket, and an audio stream
its devices feed onto one clock. The design, paths, environment and command line are
`docs/index.html`; the wire contract, the stream's included, is `docs/protocol.html`. This file
holds what those pages do not: the commands, the map and how each verb behaves inside.

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

## Contract

### `rift serve`

The daemon, a foreground process under launchd. Start: signals armed first, so a stop during the
boot drains; a socket path over 103 bytes, a `run/` not this user's or reachable by group or other,
or a held lock is one stderr line, a `refused` log line and exit 1. Otherwise it creates the root
and `run/` 0700 when absent, holds an exclusive `flock` on `run/serve.lock` for life, cuts a torn
last line off the log and derives its state from it, binds `run/serve.new`, writes
`run/serve.pid`, appends the `boot` event, closes out the segments a kill left open, renames the
socket over `run/rift.sock`, logs its start and prints its health line on stdout: ready.

SIGTERM, SIGINT and SIGHUP take the shutdown path: stop accepting, close the open segments, append
`stop`, end the feeds, let open connections ask `health` inside `$RIFT_DRAIN_MS`, cut them, remove
`serve.pid` and the socket while the pid file still holds its pid, log the exit, exit 0.

### The clients and `rift device`

Each prints the daemon's reply (a feed, a listen and a read, in its place, each line after it until
the stream ends), exit 0 on `"ok":true`; `down` and `unknown` exit 1. A daemon of another protocol is
one stderr line naming both and the fix (`rift setup`), exit 1; another version, the same line
beside the reply. `rift device`, as `docs/index.html#cli` has it, answers the reply's ping before its
first frame, so the daemon never refuses it `unsynced`.

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

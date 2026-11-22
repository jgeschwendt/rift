# rift

A local daemon that keeps one append-only event log and serves it, live, over a unix socket.

## Install

```sh
mise use -g packslip:jgeschwendt/rift   # the latest release, verified by packslip
rift setup                              # into hotswap's layout, the ~/.local/bin/rift link, the launchd agent
rift upgrade                            # a newer release swapped in; the upgrade agent runs it every 900 s
```

From a checkout, `mise run install` builds `~/.local/bin/rift-next`, a dev build beside the release.

# Invariants

| claim | node | anchor |
| --- | --- | --- |
| one instance, by an exclusive flock on run/serve.lock opened append, never create, so a second instance cannot even truncate the lock file | / | ※ one-instance-by-lock |
| the socket lives in a run/ directory the daemon creates 0700 and checks is this user's and closed to group and other, else it refuses and removes the socket | / | ※ socket-in-0700 |
| the daemon arms SIGHUP, SIGINT and SIGTERM before anything else in its boot, so a stop during the boot waits for the drain and never kills | / | ※ stop-signals-first |

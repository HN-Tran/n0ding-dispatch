# Operations

Dispatch stores definitions, runs and append-only events in SQLite WAL. Stop the service or use SQLite's online backup API; never copy only the main database while its WAL is active. Restore while stopped, keep the replaced files until validation succeeds, and run `n0ding-dispatch doctor` after restart.

Default binding is loopback. A non-loopback bind requires `--auth-token` or `N0DING_DISPATCH_AUTH_TOKEN`; place TLS in front of it. Upgrade by stopping, backing up, replacing the binary and restarting. Active runs are marked interrupted and require explicit reconciliation rather than silent continuation.

Replay exports may contain prompts, messages and tool output even after credential redaction, so apply the same access and retention policy as the source run.

## Container data directory

The distroless image runs as UID/GID `65532` and ships `/data` owned by that
identity. The default database is `/data/dispatch.db`; Compose mounts its named
volume at that path.

CI first runs `scripts/container-layout-smoke.sh` without a `/data` mount to
prove SQLite works on the raw container writable layer. It then runs a distinct
compose-like named-volume deployment with a read-only root filesystem, `/tmp` tmpfs, and
`no-new-privileges`. Each run must stay healthy, execute the authenticated
deterministic fixture, expose its expected interrupted projection, and create a
non-empty SQLite database. This is packaging evidence, not a durability or
backup/restore drill.

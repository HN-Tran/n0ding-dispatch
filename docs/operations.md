# Operations

Dispatch stores definitions, runs and append-only events in SQLite WAL. Stop the service or use SQLite's online backup API; never copy only the main database while its WAL is active. Restore while stopped, keep the replaced files until validation succeeds, and run `n0ding-dispatch doctor` after restart.

Default binding is loopback. A non-loopback bind requires `--auth-token` or `N0DING_DISPATCH_AUTH_TOKEN`; place TLS in front of it. Upgrade by stopping, backing up, replacing the binary and restarting. Active runs are marked interrupted and require explicit reconciliation rather than silent continuation.

Replay exports may contain prompts, messages and tool output even after credential redaction, so apply the same access and retention policy as the source run.

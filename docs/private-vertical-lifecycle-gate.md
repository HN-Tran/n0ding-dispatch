# Hermetic private lifecycle gate

`TestPrivateVerticalLifecycleGate` runs Dispatch against an in-process loopback
HTTP worker using the existing OpenClaw-compatible adapter contract. It proves a
two-task dependency remains blocked until the first accepted task has a checked
terminal result, then releases the second task. Dispatch is restarted with the
same SQLite database before checking the second result. The final LIVE
projection must equal checksum-verified offline replay after cursor
normalization.

The same gate records an idempotent fenced control and separately proves an HTTP
worker that accepts a request and drops the connection becomes `outcome_unknown`
across restart, blocks retry, and requires typed evidence before `not_applied`
reconciliation. Evidence carries run, task, command event, fence, disposition,
and observation under a canonical SHA-256 digest; mismatched or stale bindings
fail closed. The digest proves integrity and binding, not the truth of an
operator's external observation. It uses no external host, network secret,
or intelligent routing. It does not establish exactly-once side effects,
multi-node availability, or production readiness.

```bash
go test ./internal/httpapi -run '^TestPrivateVerticalLifecycleGate$' -v
```

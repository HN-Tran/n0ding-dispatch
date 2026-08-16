# n0ding Dispatch

Local-first observability and control center for routing work to external agent runtimes with durable evidence and safe operator intervention.

> Private v0.1 development branch. Not published, stable, or production-ready.

```bash
go build -o n0ding-dispatch ./cmd/n0ding-dispatch
./n0ding-dispatch serve --db dispatch.db
```

Open <http://127.0.0.1:8080>. Dispatch is independent and does not require n0ding Cache or n0ding Bench.

CLI commands emit JSON and use stable exit codes (`0` success, `2` usage, `3` transport, `4` rejected):

```text
n0ding-dispatch init
n0ding-dispatch serve
n0ding-dispatch run --id RUN --catalog CATALOG --dag DAG
n0ding-dispatch runs
n0ding-dispatch control --run RUN [--task TASK] pause|resume|cancel|retry|reassign|emergency-stop
n0ding-dispatch approve --run RUN --digest DIGEST --decision grant|deny
n0ding-dispatch reconcile --run RUN --idempotency-key KEY --result RESULT
n0ding-dispatch export --run RUN
n0ding-dispatch doctor
```

Private v0.1 focuses on deterministic routing, a real OpenClaw adapter, a reproducible HTTP fixture adapter, persistent commands and leases, digest-bound approvals, honest `outcome_unknown` handling, restart recovery, read-only replay, and a task-first LIVE/REPLAY control center. It does not claim intelligent routing, exactly-once side effects, multi-node HA, or production readiness.

Documentation: [domain contracts](docs/domain-contracts.md), [state machine](docs/dispatch-state-machine.md), [HTTP API](docs/api.md), [operations](docs/operations.md), [security](docs/security.md), [threat model](docs/threat-model.md), and the [private release gate](docs/release-gate.md).

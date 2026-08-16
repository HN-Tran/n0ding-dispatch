# Dispatch domain contracts

- **Run**: immutable identity for one routed DAG execution.
- **Task**: versioned DAG node with dependencies, capability requirements and bounds.
- **Route decision**: selected agent plus every candidate, exclusion and stable tie-break reason.
- **Command**: durable requested control or dispatch action; request and acknowledgement are distinct.
- **Event**: append-only observation. Events are evidence, never commands.
- **Adapter**: translates Dispatch operations to one external runtime without changing domain semantics.
- **Approval**: actor- and expiry-bound authorization for one canonical action digest.
- **Lease/fence**: ownership epoch; stale commits fail closed.
- **Replay**: read-only reconstruction that never invokes an adapter or tool.

Invariants: identifiers are opaque; sequences strictly increase per run; retries preserve prior attempts; idempotency keys return recorded outcomes; possible side effects with lost responses become `outcome_unknown`; reconciliation appends new evidence rather than rewriting history; unknown events remain forward-compatible.

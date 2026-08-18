# Public-preview v0.1 gate

Dispatch passes only when automated tests prove both fixture/HTTP and OpenClaw adapter shapes use the same persisted domain and UI; LIVE equals replay after restart; control requests and acknowledgements are distinct and idempotent; stale fences fail; mutated, expired, or unauthorized approvals fail; lost side-effect responses become `outcome_unknown` without retry; reconciliation needs new evidence; cycle, fan-out, budgets, retries and emergency stop bound runaway work; secrets are absent from every sink; replay invokes no adapter; and remote auth/origin plus adapter SSRF fail closed.

This gate does not establish intelligent routing, exactly-once side effects, multi-node availability, production readiness, or tamper-proof storage.

The bounded lifecycle evidence is documented in
[the hermetic private lifecycle gate](private-vertical-lifecycle-gate.md).

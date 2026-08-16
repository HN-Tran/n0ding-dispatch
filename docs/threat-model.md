# Threat model and trust boundaries

## Assets

Secrets, prompts and tool output; routing integrity; routing and approval authority; event history; artifacts; user identity; host and external runtime access.

## Trust boundaries

1. **Browser ↔ local API**: browser content is untrusted. State-changing requests require authentication and CSRF-safe authorization.
2. **API ↔ persistence**: only redacted, validated records cross into SQLite/WAL, logs, exports or event streams.
3. **Core ↔ adapters/providers**: all remote output is hostile input; enforce schemas, size/time limits and normalized errors.
4. **Control plane ↔ agent runtime/tools**: commands may cause irreversible effects; use least privilege, approvals, idempotency and fencing.
5. **Process ↔ filesystem/artifacts**: paths and media are untrusted; prevent traversal, unsafe rendering and executable interpretation.

Local loopback is the default trust posture. Binding beyond loopback fails closed unless authentication is configured. TLS termination and proxy trust must be explicit.

## Principal threats and controls

| Threat | Required prototype control |
|---|---|
| Secret leakage through events, WAL, SSE, logs or export | centralized redaction before every sink; deny-list plus typed secret fields; regression fixtures |
| Prompt/tool output injection into UI | render as text by default; sanitize Markdown; strict CSP; no arbitrary HTML |
| Unauthorized remote control | loopback default; authenticated remote mode; role checks for control/approval |
| Replay causes side effects | separate read model; no command-capable adapter in replay path |
| Duplicate/stale command repeats effects | idempotency keys, leases and monotonically renewed fencing tokens |
| Network ambiguity misreports outcome | terminal `outcome_unknown`; reconciliation before retry |
| Approval reused for changed action | canonical action digest, actor, scope and expiry |
| Event tampering/reordering | transactionally assigned sequence, unique IDs, conflict detection; do not claim tamper-proof storage |
| Resource exhaustion | payload/artifact quotas, bounded queues, concurrency and time limits, SSE backpressure/disconnect |
| Malicious artifact/path | content-type allow-list, size limits, generated storage names, traversal rejection |
| Cross-run data exposure | run-scoped authorization and queries; opaque IDs are not authorization |
| SSRF through provider configuration | explicit schemes, destination policy, redirect limits, metadata/private-address protection in remote mode |

## Out of scope for the prototype

Multi-tenant hostile isolation, tamper-proof audit storage, arbitrary untrusted code sandboxing, high availability and universal provider reproducibility. Documentation and UI must not imply these guarantees.

## Security gates

- Redaction tests prove sentinel secrets never reach database, WAL, SSE, logs or export.
- Remote bind without valid auth configuration refuses startup.
- Replay tests prove no adapter command or network invocation occurs.
- Approval mutation, stale fencing and duplicate command tests fail closed.
- Ambiguous transport results become `outcome_unknown`.

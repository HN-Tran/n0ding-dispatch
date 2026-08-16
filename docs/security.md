# Security model

Dispatch is loopback-first. Remote binding requires authentication and TLS at a trusted reverse proxy. Adapter endpoints are validated before and during connection; redirects and ambient proxies are disabled. Policies deny actions by default. Approvals bind a canonical action, inputs, artifact versions, policy, scope, actor and expiry. Secrets are redacted before persistence, WAL, logs, API, SSE and export.

Replay is read-only and never invokes agents or tools. A lost response after a possible side effect is `outcome_unknown`; it is not retried until an operator supplies reconciliation evidence. Local database access remains trusted-host authority.

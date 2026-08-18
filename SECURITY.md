# Security policy

n0ding Dispatch v0.1 is a public preview with best-effort security fixes and no
response-time SLA. Its safe default is loopback-only; remote binding requires
authentication and TLS should terminate at a trusted reverse proxy.

Do not open a public issue containing exploit details, runtime credentials,
private task definitions, evidence, or user data. Use this repository's private
**Report a vulnerability** flow. Include the affected revision, a redacted
deployment description, reproduction steps, impact, and known mitigations.

Dispatch is not a sandbox, secret store, policy engine, or exactly-once execution
system. Keep adapter credentials server-side and review
[docs/security.md](docs/security.md) and
[docs/threat-model.md](docs/threat-model.md) before remote use.

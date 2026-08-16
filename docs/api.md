# HTTP API

The versioned API lives under `/api/v1`: catalogs at `/agents`, DAGs at `/tasks`, execution at `/dispatch/run`, run history/events/projection/export under `/runs`, and controls, approvals, task results, decisions, messages, artifacts and reconciliation under the corresponding run. Acknowledgement leaves a task running; `POST /runs/{id}/tasks/{task}/result` records its observed result and advances the DAG. `GET /runs/{id}/events` returns JSON or resumable SSE with `Accept: text/event-stream` and `Last-Event-ID`.

Mutations require bounded JSON. Remote API access requires a strict bearer token, same-origin browser requests and TLS at a trusted reverse proxy. Arbitrary event injection is not exposed.

#!/bin/sh
set -eu
binary=${1:-./bin/n0ding-dispatch}
work=$(mktemp -d)
trap 'kill "${pid:-}" 2>/dev/null || true; rm -rf "$work"' EXIT
"$binary" init --db "$work/dispatch.db" >/dev/null
"$binary" serve --addr 127.0.0.1:18083 --db "$work/dispatch.db" >"$work/server.log" 2>&1 & pid=$!
i=0
until "$binary" doctor --server http://127.0.0.1:18083 >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 50 ] || { cat "$work/server.log"; exit 1; }; sleep .1; done
curl -fsS -X POST http://127.0.0.1:18083/api/v1/fixtures | grep -q '"id":"dispatch-fixture"'
curl -fsS http://127.0.0.1:18083/api/v1/runs/dispatch-fixture/projection | grep -q '"status":"completed"'

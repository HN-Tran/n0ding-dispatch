#!/bin/sh
set -eu

image=${1:-n0ding-dispatch:ci}
token=ci-smoke-token
work=$(mktemp -d)
volume=dispatch-smoke-data-$$
cleanup() {
  docker rm -f dispatch-smoke-raw dispatch-smoke-volume >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

wait_healthy() {
  name=$1
  port=$2
  i=0
  until curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; do
    i=$((i+1))
    if [ "$i" -ge 50 ]; then docker logs "$name"; exit 1; fi
    sleep 0.2
  done
  [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ]
  curl -fsS -H "Authorization: Bearer $token" -X POST "http://127.0.0.1:${port}/api/v1/fixtures" | grep -q '"id":"dispatch-fixture"'
  curl -fsS -H "Authorization: Bearer $token" "http://127.0.0.1:${port}/api/v1/runs/dispatch-fixture/projection" | grep -q '"status":"interrupted"'
  docker cp "$name:/data/dispatch.db" "$work/${name}.db"
  test -s "$work/${name}.db"
}

# Raw startup proves the image-layer /data directory is writable by UID 65532.
docker run -d --name dispatch-smoke-raw -p 18080:8080 \
  -e N0DING_DISPATCH_AUTH_TOKEN="$token" "$image" >/dev/null
wait_healthy dispatch-smoke-raw 18080
docker rm -f dispatch-smoke-raw >/dev/null

# This mirrors the compose persistence boundary with a hardened root filesystem.
docker volume create "$volume" >/dev/null
docker run -d --name dispatch-smoke-volume -p 18081:8080 --read-only \
  --tmpfs /tmp:size=64m,mode=1777 --security-opt no-new-privileges:true \
  -v "$volume:/data" -e N0DING_DISPATCH_AUTH_TOKEN="$token" "$image" >/dev/null
wait_healthy dispatch-smoke-volume 18081

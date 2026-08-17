#!/bin/sh
set -eu

image=${1:-n0ding-dispatch:ci}
token=ci-smoke-token
work=$(mktemp -d "${TMPDIR:-/tmp}/n0ding-dispatch-smoke.XXXXXX")
run_id=$(basename "$work" | tr -cd 'A-Za-z0-9_.-')
case "$run_id" in ''|*[!A-Za-z0-9_.-]*) echo "invalid run id" >&2; exit 1;; esac
raw="n0ding-dispatch-raw-$run_id"
volume_container="n0ding-dispatch-volume-$run_id"
volume="n0ding-dispatch-data-$run_id"
label="n0ding.smoke.run=$run_id"
raw_created=false
volume_container_created=false
volume_created=false
cleanup() {
  if [ "$raw_created" = true ]; then docker rm -f "$raw" >/dev/null 2>&1 || true; fi
  if [ "$volume_container_created" = true ]; then docker rm -f "$volume_container" >/dev/null 2>&1 || true; fi
  if [ "$volume_created" = true ]; then docker volume rm "$volume" >/dev/null 2>&1 || true; fi
  rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM

for resource in "$raw" "$volume_container"; do
  if docker container inspect "$resource" >/dev/null 2>&1; then echo "refusing preexisting container $resource" >&2; exit 1; fi
done
if docker volume inspect "$volume" >/dev/null 2>&1; then echo "refusing preexisting volume $volume" >&2; exit 1; fi

assert_layout() {
  name=$1
  [ "$(docker inspect -f '{{.Config.User}}' "$name")" = "65532" ]
  docker export "$name" >"$work/$name.tar"
  tar --numeric-owner -tvf "$work/$name.tar" | awk '$NF == "data/" && $2 == "65532/65532" { found=1 } END { exit !found }'
}

wait_healthy() {
  name=$1
  mapping=$(docker port "$name" 8080/tcp)
  case "$mapping" in 127.0.0.1:*) port=${mapping##*:};; *) echo "non-loopback mapping: $mapping" >&2; exit 1;; esac
  i=0
  until curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; do
    i=$((i+1)); if [ "$i" -ge 50 ]; then docker logs "$name"; exit 1; fi; sleep 0.2
  done
  [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ]
  [ "$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/api/v1/runs")" = 401 ]
  curl -fsS -H "Authorization: Bearer $token" -X POST "http://127.0.0.1:${port}/api/v1/fixtures" | grep -q '"id":"dispatch-fixture"'
  curl -fsS -H "Authorization: Bearer $token" "http://127.0.0.1:${port}/api/v1/runs/dispatch-fixture/projection" | grep -q '"status":"interrupted"'
  docker cp "$name:/data/dispatch.db" "$work/$name.db"; test -s "$work/$name.db"
}

docker run -d --name "$raw" --label "$label" -p 127.0.0.1::8080 -e N0DING_DISPATCH_AUTH_TOKEN="$token" "$image" >/dev/null
raw_created=true
assert_layout "$raw"
wait_healthy "$raw"
docker rm -f "$raw" >/dev/null; raw_created=false

docker volume create --label "$label" "$volume" >/dev/null; volume_created=true
docker run -d --name "$volume_container" --label "$label" -p 127.0.0.1::8080 --read-only --tmpfs /tmp:size=64m,mode=1777 \
  --security-opt no-new-privileges:true -v "$volume:/data" -e N0DING_DISPATCH_AUTH_TOKEN="$token" "$image" >/dev/null
volume_container_created=true
wait_healthy "$volume_container"

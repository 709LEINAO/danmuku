#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ -z "${DANMAKU_PASSWORD:-}" ]; then
  printf '%s\n' 'DANMAKU_PASSWORD is not set. The password is injected at build time and never kept in source.' >&2
  exit 1
fi
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -X 'main.defaultPassword=$DANMAKU_PASSWORD'" -o douyu-danmaku-linux-amd64 .
bytes=$(wc -c < douyu-danmaku-linux-amd64 | tr -d ' ')
printf '{\n  "result": "PASS",\n  "target": "linux/amd64",\n  "cgo_enabled": false,\n  "exit_code": 0,\n  "bytes": %s\n}\n' "$bytes" > deployment/deploy-current-http/build-result.json
printf '%s\n' "Built: douyu-danmaku-linux-amd64 ($bytes bytes)"

#!/bin/sh
set -eu
APP_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -gt 0 ]; then
  exec "$APP_DIR/douyu-danmaku-linux-amd64" -addr "${DANMAKU_ADDR:-0.0.0.0:8787}" -room "$1"
fi
exec "$APP_DIR/douyu-danmaku-linux-amd64" -addr "${DANMAKU_ADDR:-0.0.0.0:8787}"

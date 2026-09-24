#!/usr/bin/env bash
# dg dev (UI :3000) + материализация всех ассетов в том же DAGSTER_HOME. Запуск из корня проекта.
set -euo pipefail
export DAGSTER_HOME="${DAGSTER_HOME:-$HOME/_/.dagster_home}"
curl -sf localhost:3000/server_info >/dev/null || { nohup uv run dg dev --port 3000 > .claude/.tmp/dg-dev.log 2>&1 & sleep 20; }
uv run dg launch --assets '*'

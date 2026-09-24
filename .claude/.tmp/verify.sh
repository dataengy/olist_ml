#!/usr/bin/env bash
# Проверка: env резолвится, defs грузятся без ошибок (запуск из корня проекта).
set -euo pipefail
uv run python .claude/.tmp/04_check_env.py
uv run dg list defs >/dev/null && echo "dg list defs: OK"

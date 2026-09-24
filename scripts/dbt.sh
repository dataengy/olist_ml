#!/usr/bin/env bash
# dbt CLI с тем же окружением, что под Dagster (src/olist_ml/defs/env.py):
#   1) ./.env — точка входа, в нём ENV_FILES и REQUIRED_ENV_VARS;
#   2) файлы из ENV_FILES по порядку; уже заданное в shell/CI не перетирается;
#   3) обязательные переменные проверяются, дефолтов нет;
#   4) относительный DUCKDB_PATH → абсолютный от корня проекта (dbt работает из dbt/).
# Использование (из любого каталога):
#   scripts/dbt.sh deps | parse | build | test ...   (аргументы уходят в dbt как есть)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

load_env_file() {  # KEY=VALUE-строки; значения из окружения выигрывают
    local file="$1" line key
    [[ -f "$file" ]] || { echo "Нет $file: скопируйте ${file}.template" >&2; exit 1; }
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
        key="${line%%=*}"
        [[ -n "${!key+x}" ]] || export "$line"
    done < "$file"
}

require() {  # все переменные заданы и непусты
    local missing=() name
    for name in "$@"; do [[ -n "${!name:-}" ]] || missing+=("$name"); done
    if ((${#missing[@]})); then
        echo "Не заданы ${missing[*]} (см. *.env.template)" >&2
        exit 1
    fi
}

load_env_file "$root/.env"
require ENV_FILES REQUIRED_ENV_VARS
IFS=',' read -ra env_files <<< "$ENV_FILES"
for rel in "${env_files[@]}"; do load_env_file "$root/${rel// /}"; done
IFS=',' read -ra required <<< "$REQUIRED_ENV_VARS"
require "${required[@]// /}"

[[ "$DUCKDB_PATH" = /* ]] || export DUCKDB_PATH="$root/$DUCKDB_PATH"
mkdir -p "$(dirname "$DUCKDB_PATH")"

cd "$root/dbt"
exec uv run --project "$root" dbt "$@" --project-dir . --profiles-dir .

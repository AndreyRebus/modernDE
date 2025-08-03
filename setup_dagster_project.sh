#!/usr/bin/env bash
set -e  # останавливаемся при первой ошибке

# ────────────────────────────────────────────────────────────────
# 1. DAGSTER_HOME  ── метаданные, логи, dagster.yaml
# ────────────────────────────────────────────────────────────────
mkdir -p dagster_home/logs

# минимальная конфигурация (можно оставить пустой – Dagster создаст дефолты)
cat > dagster_home/dagster.yaml <<'YAML'
# Пример: указываем, что storage = локальная SQLite
storage:
  sqlite:
    base_dir: .
YAML

# ────────────────────────────────────────────────────────────────
# 2. DAGSTER_PROJECT  ── рабочий код
# ────────────────────────────────────────────────────────────────
mkdir -p dagster_project/my_repo

# workspace.yaml
cat > dagster_project/workspace.yaml <<'YAML'
load_from:
  - python_file:
      relative_path: my_repo/jobs.py
YAML

# requirements.txt
cat > dagster_project/requirements.txt <<'REQ'
dagster
REQ

# __init__.py (пустой)
touch dagster_project/my_repo/__init__.py

# jobs.py
cat > dagster_project/my_repo/jobs.py <<'PY'
from dagster import job, op

@op
def hello():
    return "Привет, Dagster!"

@job
def hello_job():
    hello()
PY

echo "✅  Созданы каталоги:"
echo "   • ./dagster_home/        (DAGSTER_HOME)"
echo "   • ./dagster_project/     (код и workspace)"

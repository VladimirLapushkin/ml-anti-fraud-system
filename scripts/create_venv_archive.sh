#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."


unset PYENV_ROOT PYENV_VERSION || true

export PATH="$(echo "$PATH" | awk -v RS=: -v ORS=: '$0 !~ /\/\.pyenv\/bin/ {print}' | sed 's/:$//')"
hash -r


if ! command -v python3.9 >/dev/null 2>&1; then
  echo "python3.9 not found. Install in WSL:"
  echo "  sudo apt-get update"
  echo "  sudo apt-get install -y python3.9 python3.9-venv python3.9-distutils"
  exit 2
fi

PY_BIN="$(command -v python3.9)"
echo "Using PY_BIN=${PY_BIN}"
"${PY_BIN}" --version


rm -rf .temp_venv
"${PY_BIN}" -m venv .temp_venv


.temp_venv/bin/python -m pip install --upgrade pip wheel
.temp_venv/bin/python -m pip install \
  "mlflow==2.17.2" \
  "boto3==1.37.22" \
  "psycopg2-binary==2.9.10" \
  "scikit-learn==1.3.2" \
  "pandas==2.0.3" \
  "venv-pack"


.temp_venv/bin/python -c "import sys; print('exe=',sys.executable); print('prefix=',sys.prefix); print('base_prefix=',getattr(sys,'base_prefix',None))"
.temp_venv/bin/python -c "import mlflow; print('mlflow', mlflow.__version__)"

mkdir -p venvs
.temp_venv/bin/venv-pack -p "$(pwd)/.temp_venv" -o venvs/venv.tar.gz

rm -rf .temp_venv
echo "OK: venvs/venv.tar.gz created"

#!/bin/bash
set -euo pipefail

function log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')]: $1"
}

# --- Антизависания apt/dpkg в remote-exec ---
log "Ожидание завершения cloud-init (чтобы apt не конфликтовал)"
if command -v cloud-init >/dev/null 2>&1; then
  sudo cloud-init status --wait
else
  # запасной вариант, если cloud-init недоступен
  while [ ! -f /var/lib/cloud/instance/boot-finished ]; do
    sleep 1
  done
fi

log "Настройка noninteractive для apt/dpkg"
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

APT_OPTS=(
  "-y"
  "-q"
  "-o" "Dpkg::Options::=--force-confdef"
  "-o" "Dpkg::Options::=--force-confold"
)
# -------------------------------------------

path_to_user="/home/ubuntu"
path_to_venv="$path_to_user/venv"

log "Проверка версии ОС и Python"
cat /etc/os-release | sed -n '1,6p'
python3 -V

# Обновляем пакеты
log "Обновление пакетов"
sudo -E apt-get update "${APT_OPTS[@]}"

# Устанавливаем необходимые пакеты
log "Установка необходимых пакетов"
sudo -E apt-get install "${APT_OPTS[@]}" python3-pip python3-venv wget ca-certificates

# Создаем виртуальное окружение для MLflow
log "Настройка виртуального окружения Python"
sudo rm -rf "$path_to_venv"
python3 -m venv "$path_to_venv"

# Установка Python-зависимостей
log "Установка Python-зависимостей (MLflow)"
"$path_to_venv/bin/python" -m pip install --upgrade pip setuptools wheel
"$path_to_venv/bin/python" -m pip install \
  "mlflow==2.21.0" \
  "psycopg2-binary==2.9.10" \
  "boto3==1.37.16"

# Быстрая проверка что mlflow реально установился
"$path_to_venv/bin/python" -m pip show mlflow | sed -n '1,40p'
"$path_to_venv/bin/python" -m mlflow --version

# Загружаем сертификаты для подключения к PostgreSQL
log "Загрузка сертификатов PostgreSQL"
mkdir -p "$path_to_user/.postgresql"
wget -q "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
    --output-document "$path_to_user/.postgresql/root.crt"
chmod 0600 "$path_to_user/.postgresql/root.crt"
sudo chown -R ubuntu:ubuntu "$path_to_user/.postgresql"

# Копируем конфигурационный файл
log "Копирование конфигурационного файла"
cp "$path_to_user/mlflow.conf" "$path_to_user/.mlflow.conf"
chmod 600 "$path_to_user/.mlflow.conf"
sudo chown ubuntu:ubuntu "$path_to_user/.mlflow.conf"

# Ставим systemd сервис
log "Настройка systemd сервиса"
sudo cp "$path_to_user/mlflow.service" /etc/systemd/system/mlflow.service
sudo systemctl daemon-reload
sudo systemctl enable --now mlflow.service

# Автозапуск user-сервисов
log "Настройка автозапуска пользовательских сервисов"
sudo loginctl enable-linger ubuntu

log "Установка MLflow завершена успешно"

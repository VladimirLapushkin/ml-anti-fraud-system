#!/bin/bash

function log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')]: $1"
}

# Добавляем пользователя ubuntu в группу airflow
log "Adding ubuntu user to airflow group"
sudo usermod -aG airflow ubuntu

# Изменяем владельца директории DAGs на airflow и устанавливаем групповые права на запись
log "Changing owner of DAGs directory to airflow"
sudo chown airflow:airflow /home/airflow/dags
sudo chmod 775 /home/airflow/dags

# Устанавливаем SGID бит, чтобы новые файлы наследовали группу airflow
log "Setting SGID bit on DAGs directory"
sudo chmod g+s /home/airflow/dags

# Отключаем примеры DAGs в airflow.cfg и устанавливаем интервал проверки директории
log "Configuring airflow.cfg"
sudo sed -i 's/load_examples = True/load_examples = False/' /etc/airflow/airflow.cfg
sudo sed -i 's/^dag_dir_list_interval = .*$/dag_dir_list_interval = 30/' /etc/airflow/airflow.cfg

# Перезапускаем Airflow webserver и scheduler для применения изменений
log "Restarting Airflow services"
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler

log "Setup completed successfully"
#!/bin/bash

# Функция логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date)] $sep "
    echo "[$(date)] [INFO] $1"
}

# Проверяем, передан ли аргумент (имя файла)
if [ -n "$1" ]; then
    FILE_NAME="$1"
    log "File name provided: $FILE_NAME"
else
    log "No file name provided, copying all files"
fi

# Создаем директорию в HDFS
log "Creating directory in HDFS"
hdfs dfs -mkdir -p /user/ubuntu/data

# Копируем данные из S3 в зависимости от того, передано ли имя файла
if [ -n "$FILE_NAME" ]; then
    # Копируем конкретный файл
    log "Copying specific file from S3 to HDFS"
    hadoop distcp s3a://{{ s3_bucket }}/$FILE_NAME /user/ubuntu/data/$FILE_NAME
else
    # Копируем все данные
    log "Copying all data from S3 to HDFS"
    hadoop distcp s3a://{{ s3_bucket }}/ /user/ubuntu/data
fi

# Выводим содержимое директории для проверки
log "Listing files in HDFS directory"
hdfs dfs -ls /user/ubuntu/data

# Проверяем успешность выполнения операции
if [ $? -eq 0 ]; then
    log "Data was successfully copied to HDFS"
else
    log "Failed to copy data to HDFS"
    exit 1
fi

#!/bin/bash

# Функция для логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $sep " | tee -a $HOME/user_data_execution.log
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [INFO] $1" | tee -a $HOME/user_data_execution.log
}

log "Starting user data script execution"

# Устанавливаем yc CLI
log "Installing yc CLI"
export HOME="/home/ubuntu"
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Изменяем владельца директории yandex-cloud и её содержимого
log "Changing ownership of yandex-cloud directory"
sudo chown -R ubuntu:ubuntu $HOME/yandex-cloud

# Применяем изменения из .bashrc
log "Applying changes from .bashrc"
source $HOME/.bashrc

# Проверяем, что yc доступен
if command -v yc &> /dev/null; then
    log "yc CLI is now available"
    yc --version
else
    log "yc CLI is still not available. Adding it to PATH manually"
    export PATH="$PATH:$HOME/yandex-cloud/bin"
    yc --version
fi

# Настраиваем yc CLI
log "Configuring yc CLI"
yc config set token ${token}
yc config set cloud-id ${cloud_id}
yc config set folder-id ${folder_id}

# Устанавливаем jq
log "Installing jq"
sudo apt-get update
sudo apt-get install -y jq

# Получаем ID мастер-ноды Dataproc кластера
log "Getting Dataproc master node ID"
DATAPROC_MASTER_FQDN=$(yc compute instance list --format json | jq -r '.[] | select(.labels.subcluster_role == "masternode") | .fqdn')

if [ -z "$DATAPROC_MASTER_FQDN" ]; then
    log "Failed to get master node ID"
    exit 1
fi

log "Master node FQDN: $DATAPROC_MASTER_FQDN"

# Создаем директорию .ssh и настраиваем приватный ключ
log "Creating .ssh directory and setting up private key"
mkdir -p /home/ubuntu/.ssh
echo "${private_key}" > /home/ubuntu/.ssh/dataproc_key
chmod 600 /home/ubuntu/.ssh/dataproc_key
chown ubuntu:ubuntu /home/ubuntu/.ssh/dataproc_key

# Добавляем конфигурацию SSH для удобного подключения к мастер-ноде
log "Adding SSH configuration for master node connection"
cat <<EOF > /home/ubuntu/.ssh/config
Host dataproc-master
    HostName $DATAPROC_MASTER_FQDN
    User ubuntu
    IdentityFile ~/.ssh/dataproc_key
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF

chown ubuntu:ubuntu /home/ubuntu/.ssh/config
chmod 600 /home/ubuntu/.ssh/config

# Настраиваем SSH-agent
log "Configuring SSH-agent"
eval $(ssh-agent -s)
echo "eval \$(ssh-agent -s)" >> /home/ubuntu/.bashrc
ssh-add /home/ubuntu/.ssh/dataproc_key
echo "ssh-add /home/ubuntu/.ssh/dataproc_key" >> /home/ubuntu/.bashrc

# Устанавливаем дополнительные полезные инструменты
log "Installing additional tools"
apt-get update
apt-get install -y tmux htop iotop

# Устанавливаем s3cmd
log "Installing s3cmd"
apt-get install -y s3cmd

# Настраиваем s3cmd
log "Configuring s3cmd"
cat <<EOF > /home/ubuntu/.s3cfg
[default]
access_key = ${access_key}
secret_key = ${secret_key}
host_base = storage.yandexcloud.net
host_bucket = %(bucket)s.storage.yandexcloud.net
use_https = True
EOF

chown ubuntu:ubuntu /home/ubuntu/.s3cfg
chmod 600 /home/ubuntu/.s3cfg

# Определяем целевой бакет
TARGET_BUCKET=${s3_bucket}
SOURCE_BUCKET="otus-mlops-source-data"
CONFIG_S3="/home/ubuntu/.s3cfg"

# Копируем конкретный файл из исходного бакета в наш новый бакет
log "Copying file from source bucket to destination bucket"

for file in $(s3cmd --config=$CONFIG_S3 ls s3://$SOURCE_BUCKET/ | awk '{print $4}'); do
    FILE_NAME=$(basename $file)
    log "Copying file $FILE_NAME"
    s3cmd cp --config=$CONFIG_S3 --acl-public $file s3://$TARGET_BUCKET/$FILE_NAME
    if [ $? -eq 0 ]; then
        log "File $FILE_NAME successfully copied to $TARGET_BUCKET"
        log "Listing contents of $TARGET_BUCKET"
    else
        log "Error occurred while copying file $FILE_NAME to $TARGET_BUCKET"
fi
s3cmd ls --config=CONFIG_S3 s3://$TARGET_BUCKET/
done

# Создаем директорию для скриптов на прокси-машине
log "Creating scripts directory on proxy machine"
mkdir -p /home/ubuntu/scripts

# Копируем скрипт upload_data_to_hdfs.sh на прокси-машину
log "Copying upload_data_to_hdfs.sh script to proxy machine"
echo '${upload_data_to_hdfs_content}' > /home/ubuntu/scripts/upload_data_to_hdfs.sh
sed -i 's/{{ s3_bucket }}/'$TARGET_BUCKET'/g' /home/ubuntu/scripts/upload_data_to_hdfs.sh

# Устанавливаем правильные разрешения для скрипта на прокси-машине
log "Setting permissions for upload_data_to_hdfs.sh on proxy machine"
chmod +x /home/ubuntu/scripts/upload_data_to_hdfs.sh

# Проверяем подключение к мастер-ноде
log "Checking connection to master node"
source /home/ubuntu/.bashrc
ssh -i /home/ubuntu/.ssh/dataproc_key -o StrictHostKeyChecking=no ubuntu@$DATAPROC_MASTER_FQDN "echo 'Connection successful'"
if [ $? -eq 0 ]; then
    log "Connection to master node successful"
else
    log "Failed to connect to master node"
    exit 1
fi

# Копируем скрипт upload_data_to_hdfs.sh с прокси-машины на мастер-ноду
log "Copying upload_data_to_hdfs.sh script from proxy machine to master node"
scp -i /home/ubuntu/.ssh/dataproc_key -o StrictHostKeyChecking=no /home/ubuntu/scripts/upload_data_to_hdfs.sh ubuntu@$DATAPROC_MASTER_FQDN:/home/ubuntu/

# Устанавливаем правильные разрешения для скрипта на мастер-ноде
log "Setting permissions for upload_data_to_hdfs.sh on master node"
ssh -i /home/ubuntu/.ssh/dataproc_key -o StrictHostKeyChecking=no ubuntu@$DATAPROC_MASTER_FQDN "chmod +x /home/ubuntu/upload_data_to_hdfs.sh"

log "Script upload_data_to_hdfs.sh has been copied to the master node"

# Изменяем владельца лог-файла
log "Changing ownership of log file"
sudo chown ubuntu:ubuntu /home/ubuntu/user_data_execution.log

# копируем все двнные нашего s3 на hdfs на мастер ноде
ssh -i /home/ubuntu/.ssh/dataproc_key -o StrictHostKeyChecking=no ubuntu@$DATAPROC_MASTER_FQDN "/bin/bash /home/ubuntu/upload_data_to_hdfs.sh"
if [ $? -eq 0 ]; then
    log "all files s3-hdfs copied"
else
    log "Error in process script s3-hdfs execute"
    exit 1
fi

log "User data script execution completed"
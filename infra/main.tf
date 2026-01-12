# main.tf

module "iam" {
  source          = "./modules/iam"
  name            = var.yc_service_account_name
  provider_config = var.yc_config
}

module "network" {
  source          = "./modules/network"
  network_name    = var.yc_network_name
  subnet_name     = var.yc_subnet_name
  provider_config = var.yc_config
}

module "storage" {
  source          = "./modules/storage"
  name            = var.yc_bucket_name
  provider_config = var.yc_config
  access_key      = module.iam.access_key
  secret_key      = module.iam.secret_key
}

module "airflow-cluster" {
  source             = "./modules/airflow-cluster"
  instance_name      = var.yc_instance_name
  subnet_id          = module.network.subnet_id
  service_account_id = module.iam.service_account_id
  admin_password     = var.admin_password
  bucket_name        = module.storage.bucket
  provider_config    = var.yc_config
}

# main.tf (исправленная секция variables_file)

resource "local_file" "variables_file" {
  content = jsonencode({
    # Общие
    YC_ZONE      = var.yc_config.zone
    YC_FOLDER_ID = var.yc_config.folder_id
    YC_SUBNET_ID = module.network.subnet_id

    # SSH публичный ключ ДЛЯ DATAPROC (ssh-ed25519 формат!)
    YC_SSH_PUBLIC_KEY           = trimspace(module.iam.ssh_public_key)
    DP_SA_AUTH_KEY_PUBLIC_KEY   = trimspace(module.iam.ssh_public_key)

    # S3
    S3_ENDPOINT_URL = var.yc_storage_endpoint_url
    S3_ACCESS_KEY   = module.iam.access_key
    S3_SECRET_KEY   = module.iam.secret_key
    S3_BUCKET_NAME  = module.storage.bucket

    # S3 clean/source
    S3_CLEAN_ACCESS_KEY = var.yc_clean_bucket_ak
    S3_CLEAN_SECRET_KEY = var.yc_clean_bucket_sk
    S3_CLEAN_PATH       = var.yc_clean_bucket_path
    S3_SOURCE_BUCKET    = var.yc_source_bucket

    # DataProc
    DP_SECURITY_GROUP_ID = module.network.security_group_id
    DP_SA_ID             = module.iam.service_account_id
    DP_SA_JSON = jsonencode({
      id                 = module.iam.auth_key_id
      service_account_id = module.iam.service_account_id
      created_at         = module.iam.auth_key_created_at
      public_key         = module.iam.public_key
      private_key        = module.iam.private_key
    })
  })
  filename        = "./variables.json"
  file_permission = "0600"
}


# Запись переменных в .env файл

resource "null_resource" "update_env" {
  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-PS
      $AIRFLOW_ID             = "${module.airflow-cluster.airflow_id}"
      $AIRFLOW_ADMIN_PASSWORD = "${var.admin_password}"
      $STORAGE_ENDPOINT_URL   = "${var.yc_storage_endpoint_url}"
      $BUCKET_NAME            = "${module.storage.bucket}"
      $ACCESS_KEY             = "${module.iam.access_key}"
      $SECRET_KEY             = "${module.iam.secret_key}"

      $envPath = "..\\.env"

      (Get-Content $envPath) -replace '^AIRFLOW_URL=.*',           "AIRFLOW_URL=https://c-$AIRFLOW_ID.airflow.yandexcloud.net" | Set-Content $envPath
      (Get-Content $envPath) -replace '^AIRFLOW_ADMIN_PASSWORD=.*',"AIRFLOW_ADMIN_PASSWORD=$AIRFLOW_ADMIN_PASSWORD"            | Set-Content $envPath
      (Get-Content $envPath) -replace '^S3_ENDPOINT_URL=.*',       "S3_ENDPOINT_URL=$STORAGE_ENDPOINT_URL"                    | Set-Content $envPath
      (Get-Content $envPath) -replace '^S3_BUCKET_NAME=.*',        "S3_BUCKET_NAME=$BUCKET_NAME"                             | Set-Content $envPath
      (Get-Content $envPath) -replace '^S3_ACCESS_KEY=.*',         "S3_ACCESS_KEY=$ACCESS_KEY"                               | Set-Content $envPath
      (Get-Content $envPath) -replace '^S3_SECRET_KEY=.*',         "S3_SECRET_KEY=$SECRET_KEY"                               | Set-Content $envPath
    PS
  }

  depends_on = [
    module.iam,
    module.storage,
    module.airflow-cluster,
  ]
}

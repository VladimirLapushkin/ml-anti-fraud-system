resource "yandex_airflow_cluster" "airflow_cluster" {
  name               = var.instance_name
  subnet_ids         = [var.subnet_id]
  service_account_id = var.service_account_id
  admin_password     = var.admin_password

  code_sync = {
    git_sync = {
      repo     = var.git_repo
      branch   = "main"
      sub_path = "/dags"
      ssh_key  = file(pathexpand(var.git_ssh_private_key))
    }
  }

  webserver = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  scheduler = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  worker = {
    min_count          = 1
    max_count          = 2
    resource_preset_id = "c1-m4"
  }

  airflow_config = {
    "api" = {
      "auth_backends" = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    }

    "scheduler" = {
      "dag_dir_list_interval" = "10"
    }

    "bootstrap" = {
      "bucket"   = var.bucket_name
      "key"      = "vars/variables.json"
      "endpoint" = var.yc_storage_endpoint_url
    }
  }

  pip_packages = [
    "boto3"
  ]

  logging = {
    enabled   = true
    folder_id = var.provider_config.folder_id
    min_level = "INFO"
  }
}

resource "yandex_iam_service_account" "sa" {
  name        = var.name
  description = "Service account for Airflow management"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_roles" {
  for_each = toset([
    "managed-airflow.integrationProvider",
    "managed-airflow.admin",
    "dataproc.editor",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
    "compute.admin",
    "storage.admin",
    "storage.uploader",
    "storage.viewer",
    "storage.editor"
  ])

  folder_id = var.provider_config.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "Static access key for service account"
}

resource "local_file" "sa_static_key_file" {
  content = jsonencode({
    id                 = yandex_iam_service_account_static_access_key.sa_static_key.id
    service_account_id = yandex_iam_service_account_static_access_key.sa_static_key.service_account_id
    created_at         = yandex_iam_service_account_static_access_key.sa_static_key.created_at
    s3_key_id          = yandex_iam_service_account_static_access_key.sa_static_key.access_key
    s3_secret_key      = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  })
  filename        = "${path.module}/static_key.json"
  file_permission = "0600"
}

resource "yandex_iam_service_account_key" "sa_auth_key" {
  service_account_id = yandex_iam_service_account.sa.id
}

# SSH ключи для DataProc (ОТДЕЛЬНО от authorized_key)
resource "tls_private_key" "dataproc_ssh" {
  algorithm = "ED25519"
}

resource "local_file" "ssh_private_key" {
  content  = tls_private_key.dataproc_ssh.private_key_pem
  filename = "${path.module}/ssh_dataproc_private.pem"
  file_permission = "0600"
}

resource "local_file" "ssh_public_key" {
  content  = tls_private_key.dataproc_ssh.public_key_openssh
  filename = "${path.module}/ssh_dataproc_public.pub"
}

# Authorized key для API (JWT аутентификация)
resource "local_file" "sa_auth_key_file" {
  content = jsonencode({
    id                 = yandex_iam_service_account_key.sa_auth_key.id
    service_account_id = yandex_iam_service_account_key.sa_auth_key.service_account_id
    created_at         = yandex_iam_service_account_key.sa_auth_key.created_at
    public_key         = yandex_iam_service_account_key.sa_auth_key.public_key
    private_key        = yandex_iam_service_account_key.sa_auth_key.private_key
  })
  filename        = "${path.module}/authorized_key.json"
  file_permission = "0600"
}

# Outputs для Airflow Variables
output "dataproc_ssh_public_key" {
  value     = tls_private_key.dataproc_ssh.public_key_openssh
  sensitive = false
  description = "SSH public key для DP_SA_AUTH_KEY_PUBLIC_KEY и YC_SSH_PUBLIC_KEY (ssh-ed25519 формат)"
}

output "sa_id" {
  value     = yandex_iam_service_account.sa.id
  sensitive = false
  description = "Service Account ID для DP_SA_ID"
}

output "sa_auth_key_id" {
  value     = yandex_iam_service_account_key.sa_auth_key.id
  sensitive = true
  description = "Authorized key ID"
}

output "sa_json_path" {
  value       = "${path.module}/authorized_key.json"
  description = "Путь к JSON для DP_SA_JSON (весь файл)"
}

output "s3_static_key_path" {
  value       = "${path.module}/static_key.json"
  description = "Путь к S3 ключам"
}

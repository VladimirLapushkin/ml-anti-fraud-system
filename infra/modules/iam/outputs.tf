output "service_account_id" {
  value = yandex_iam_service_account.sa.id
}

output "access_key" {
  value     = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  sensitive = true
}

output "secret_key" {
  value     = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  sensitive = true
}

output "auth_key_id" {
  value     = yandex_iam_service_account_key.sa_auth_key.id
  sensitive = true
}

output "auth_key_created_at" {
  value = yandex_iam_service_account_key.sa_auth_key.created_at
}

output "public_key" {
  value     = yandex_iam_service_account_key.sa_auth_key.public_key
  sensitive = true
}

output "private_key" {
  value     = yandex_iam_service_account_key.sa_auth_key.private_key
  sensitive = true
}

output "ssh_public_key" {
  value       = tls_private_key.dataproc_ssh.public_key_openssh
  sensitive   = false
  description = "SSH public key для DataProc YC_SSH_PUBLIC_KEY и DP_SA_AUTH_KEY_PUBLIC_KEY"
}

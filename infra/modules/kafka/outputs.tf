output "topic_in" { value = var.topic_in }
output "topic_out" { value = var.topic_out }

output "consumer_user" { value = var.consumer_user }
output "consumer_password" {
  value     = var.consumer_password
  sensitive = true
}
output "cluster_id" {
  value = yandex_mdb_kafka_cluster.this.id
}

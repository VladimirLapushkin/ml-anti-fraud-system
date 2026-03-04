variable "kafka_port" {
  type    = number
  default = 9092
}

data "yandex_mdb_kafka_cluster" "kafka" {
  cluster_id = module.kafka.cluster_id
}


locals {
  kafka_bootstrap_servers = join(",", [
    for h in data.yandex_mdb_kafka_cluster.kafka.host : "${h.name}:${var.kafka_port}"
  ])
}

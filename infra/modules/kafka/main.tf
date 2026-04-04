resource "yandex_mdb_kafka_cluster" "this" {
  name        = var.name
  environment = "PRODUCTION"
  network_id  = var.network_id
  subnet_ids  = var.subnet_ids

  security_group_ids = [var.security_group_id]

  config {
    version          = "3.7"
    brokers_count    = 1
    zones            = [var.zone]
    assign_public_ip = false
    schema_registry  = false

    kafka_ui {
      enabled = true
    }
    kafka {
      resources {
        resource_preset_id = "s2.micro"
        disk_type_id       = "network-ssd"
        disk_size          = 80
      }

      kafka_config {
        num_partitions             = 10
        default_replication_factor = 1
        sasl_enabled_mechanisms    = ["SASL_MECHANISM_SCRAM_SHA_256"]
      }
    }
  }
}

resource "yandex_mdb_kafka_topic" "transactions_in" {
  cluster_id         = yandex_mdb_kafka_cluster.this.id
  name               = var.topic_in
  partitions         = var.partitions_in
  replication_factor = var.replication_factor
}

resource "yandex_mdb_kafka_topic" "predictions_out" {
  cluster_id         = yandex_mdb_kafka_cluster.this.id
  name               = var.topic_out
  partitions         = var.partitions_out
  replication_factor = var.replication_factor
}

  resource "yandex_mdb_kafka_user" "producer" {
    cluster_id = yandex_mdb_kafka_cluster.this.id
    name       = var.producer_user
    password   = var.producer_password

    permission {
      topic_name = yandex_mdb_kafka_topic.transactions_in.name
      role       = "ACCESS_ROLE_PRODUCER"
    }
    
  }

resource "yandex_mdb_kafka_user" "consumer" {
  cluster_id = yandex_mdb_kafka_cluster.this.id
  name       = var.consumer_user
  password   = var.consumer_password

  permission {
    topic_name = yandex_mdb_kafka_topic.transactions_in.name
    role       = "ACCESS_ROLE_CONSUMER"
  }

  # ДОБАВИТЬ: право писать в transactions_in
  permission {
    topic_name = yandex_mdb_kafka_topic.transactions_in.name
    role       = "ACCESS_ROLE_PRODUCER"
  }

  permission {
    topic_name = yandex_mdb_kafka_topic.predictions_out.name
    role       = "ACCESS_ROLE_CONSUMER"
  }

  permission {
    topic_name = yandex_mdb_kafka_topic.predictions_out.name
    role       = "ACCESS_ROLE_PRODUCER"
  }
}

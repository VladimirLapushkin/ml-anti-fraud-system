variable "provider_config" { type = any }

variable "name" { type = string }
variable "network_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "security_group_id" { type = string }
variable "zone" { type = string }

variable "topic_in" { type = string }
variable "topic_out" { type = string }

variable "partitions_in" { type = number }
variable "partitions_out" { type = number }
variable "replication_factor" { type = number }

variable "producer_user" { type = string }
variable "consumer_user" { type = string }
variable "producer_password" {
  type      = string
  sensitive = true
}
variable "consumer_password" {
  type      = string
  sensitive = true
}

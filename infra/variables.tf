
variable "yc_token" {
  type        = string
  description = "Yandex Cloud OAuth token"
}

variable "yc_cloud_id" {
  type        = string
  description = "Yandex Cloud ID"
}

variable "yc_folder_id" {
  type        = string
  description = "Yandex Cloud Folder ID"
}

variable "yc_zone" {
  type        = string
  description = "Zone for Yandex Cloud resources"
}

variable "yc_instance_name" {
  type        = string
  description = "Name of the virtual machine"
}

variable "yc_image_id" {
  type        = string
  description = "ID of the image for the virtual machine"
}

variable "yc_subnet_name" {
  type        = string
  description = "Name of the custom subnet"
}

variable "yc_service_account_name" {
  type        = string
  description = "Name of the service account"
  default     = "value"
}

variable "yc_bucket_name" {
  type        = string
  description = "Name of the bucket"
}


variable "yc_network_name" {
  type        = string
  description = "Name of the network"
}

variable "yc_route_table_name" {
  type        = string
  description = "Name of the route table"
}

variable "yc_nat_gateway_name" {
  type        = string
  description = "Name of the NAT gateway"
}

variable "yc_security_group_name" {
  type        = string
  description = "Name of the security group"
}

variable "yc_subnet_range" {
  type        = string
  description = "CIDR block for the subnet"
}

variable "yc_dataproc_cluster_name" {
  type        = string
  description = "Name of the Dataproc cluster"
}

variable "yc_dataproc_version" {
  type        = string
  description = "Version of Dataproc"
}

variable "public_key_path" {
  type        = string
  description = "Path to the public key file"
}

variable "private_key_path" {
  type        = string
  description = "Path to the private key file"
}

variable "dataproc_master_resources" {
  type = object({
  
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  
    
  })
  default = {
    resource_preset_id = "s3-c2-m8"
    disk_type_id       = "network-ssd"
    disk_size          = 40
    }
}

variable "dataproc_compute_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  })
  default = {
    resource_preset_id = "s3-c2-m8"
    disk_type_id       = "network-ssd"
    disk_size          = 50
  }
}

variable "dataproc_data_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  })
  default = {
    resource_preset_id = "s3-c4-m16"
    disk_type_id       = "network-ssd"
    disk_size          = 150
  }
}

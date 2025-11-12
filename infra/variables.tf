variable "yc_zone" {
  description = "Yandex Cloud availability zone"
  type        = string
  default     = "ru-central1-a"
}

variable "yc_folder_id" {
  description = "Yandex Cloud folder ID"
  type        = string
}

variable "yc_cloud_id" {
  description = "Yandex Cloud cloud ID"
  type        = string
}

variable "yc_token" {
  description = "Yandex Cloud OAuth token"
  type        = string
}

variable "bucket_name" {
  description = "Name of the storage bucket"
  type        = string
}

variable "ssh_public_key" {
  description = "Public SSH key for accessing the VM"
  type        = string
}

variable "disk_params" {
  description = "Parameters for the compute disk"
  type = object({
    name : string
    type : string
    image_id : string
    size: string
  })
  default = {
    name     = "otus-disk"
    type     = "network-ssd"
    image_id = "fd805qs1mn3n0casp7lt"
    size = "20"
  }
}

variable "vm_name" {
  type    = string
  default = "otus-vm"
}

variable "platform_id" {
  type    = string
  default = "standard-v3"
}

variable "sa_name" {
  type    = string
  default = "storage-editor"
}

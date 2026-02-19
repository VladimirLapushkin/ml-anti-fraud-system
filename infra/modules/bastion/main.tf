variable "name" {}
variable "zone" {}
variable "subnet_id" {}
variable "network_id" {}
variable "ubuntu_image_id" {}
variable "ssh_public_key_path" {}
variable "my_public_ip_cidr" {} # "X.X.X.X/32"
variable "provider_config" {}

provider "yandex" {
  alias  = "this"
  token  = var.provider_config.token
  cloud_id  = var.provider_config.cloud_id
  folder_id = var.provider_config.folder_id
  zone      = var.provider_config.zone
}

resource "yandex_vpc_security_group" "bastion_sg" {
  provider   = yandex.this
  name       = "${var.name}-sg"
  network_id = var.network_id

  ingress {
    protocol       = "TCP"
    port           = 22
    v4_cidr_blocks = [var.my_public_ip_cidr]
    description    = "SSH to bastion"
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
    description    = "Allow egress"
  }
}

resource "yandex_compute_instance" "bastion" {
  provider = yandex.this
  name     = var.name
  zone     = var.zone

  resources {
    cores  = 2
    memory = 2
  }

  boot_disk {
    initialize_params {
      image_id = var.ubuntu_image_id
      size     = 20
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = var.subnet_id
    nat                = true
    security_group_ids = [yandex_vpc_security_group.bastion_sg.id]
  }

  metadata = {
    ssh-keys = "ubuntu:${file(var.ssh_public_key_path)}"
  }
}

output "bastion_public_ip" {
  value = yandex_compute_instance.bastion.network_interface[0].nat_ip_address
}

output "bastion_sg_id" {
  value = yandex_vpc_security_group.bastion_sg.id
}

//
// Create a new Compute Disk.
//
resource "yandex_compute_disk" "disk" {
  name     = var.disk_params.name
  type     = var.disk_params.type
  image_id = var.disk_params.image_id
  zone     = var.yc_zone
  size     = var.disk_params.size
}

//
// Create a new Compute Instance
//
resource "yandex_compute_instance" "vm" {
  name        = var.vm_name
  platform_id = var.platform_id
  zone        = var.yc_zone

  resources {
    cores  = 2
    memory = 4
  }

  boot_disk {
    disk_id = yandex_compute_disk.disk.id
  }

  network_interface {
    subnet_id = yandex_vpc_subnet.subnet.id
    nat       = true
  }

  metadata = {
    ssh-keys = "ubuntu:${file(var.ssh_public_key)}"
  }
}

// Auxiliary resources for Compute Instance
resource "yandex_vpc_network" "network" {}

resource "yandex_vpc_subnet" "subnet" {
  zone           = var.yc_zone
  network_id     = yandex_vpc_network.network.id
  v4_cidr_blocks = ["10.5.0.0/24"]
}


 //Create a new IAM Service Account (SA).

resource "yandex_iam_service_account" "sa" {
  name        = var.sa_name
  description = "Service Account for OTUS Terraform course"
}

//
// Create a new IAM Service Account IAM Member.
//
resource "yandex_resourcemanager_folder_iam_member" "sa-role" {
  folder_id = var.yc_folder_id
  role      = "storage.admin"
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

//
// Create a new IAM Service Account Static Access SKey.
//
resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}

//
// Create a new Storage Bucket. 
//
resource "yandex_storage_bucket" "bucket" {
  bucket     = var.bucket_name
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
}

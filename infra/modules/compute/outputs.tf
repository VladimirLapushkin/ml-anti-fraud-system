output "external_ip_address" {
  value = yandex_compute_instance.vm.network_interface.0.nat_ip_address
}

output "instance_id" {
  value = yandex_compute_instance.vm.id
}
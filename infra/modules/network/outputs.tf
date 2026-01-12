output "subnet_id" {
  value = yandex_vpc_subnet.subnet.id
}

output "security_group_id" {
  value = yandex_vpc_security_group.security_group.id
}
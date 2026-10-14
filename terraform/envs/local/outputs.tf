output "buckets" {
  value = module.storage.bucket_names
}

output "tables" {
  value = module.state.table_names
}

output "queues" {
  value = { load = module.load.queue_name, dlq = module.load.dlq_name }
}

output "secrets" {
  value = module.security.secret_names
}

output "alarms" {
  value = module.observability.alarm_names
}

output "state_machine_arn" {
  value = var.enable_compute ? module.orchestration[0].state_machine_arn : null
}

output "state_machine_arn" {
  value = module.orchestration.state_machine_arn
}

output "export_bucket" {
  value = module.storage.bucket_names["exports"]
}

output "alerts_topic_arn" {
  value = module.observability.alerts_topic_arn
}

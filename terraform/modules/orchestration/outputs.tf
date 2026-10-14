output "state_machine_arn" {
  value = aws_sfn_state_machine.run.arn
}

output "state_machine_name" {
  value = aws_sfn_state_machine.run.name
}

output "scheduler_dlq_arn" {
  value = aws_sqs_queue.scheduler_dlq.arn
}

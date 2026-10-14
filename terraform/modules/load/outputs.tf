output "queue_name" {
  value = aws_sqs_queue.load.name
}

output "queue_arn" {
  value = aws_sqs_queue.load.arn
}

output "dlq_name" {
  value = aws_sqs_queue.dlq.name
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "loader_function_name" {
  value = var.enable_compute ? aws_lambda_function.loader[0].function_name : null
}

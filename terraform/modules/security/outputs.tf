output "kms_key_arn" {
  value = aws_kms_key.data.arn
}

output "secret_arns" {
  value = { for k, s in aws_secretsmanager_secret.api_key : k => s.arn }
}

output "secret_names" {
  value = { for k, s in aws_secretsmanager_secret.api_key : k => s.name }
}

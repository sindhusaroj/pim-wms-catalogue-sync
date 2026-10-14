output "bucket_names" {
  value = { for k, b in aws_s3_bucket.zone : k => b.bucket }
}

output "bucket_arns" {
  value = { for k, b in aws_s3_bucket.zone : k => b.arn }
}

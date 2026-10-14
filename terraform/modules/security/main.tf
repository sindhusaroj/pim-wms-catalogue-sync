# KMS key for the data plane and the Secrets Manager containers for vendor API keys.
# Secret *values* are never managed here, except an explicit seed for the local
# LocalStack environment (section 7.6).

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

data "aws_iam_policy_document" "kms" {
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
  statement {
    sid       = "AllowAwsServicesInAccount"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = ["*"]
    principals {
      type = "Service"
      # CloudWatch Logs uses a regional service principal.
      identifiers = ["sqs.amazonaws.com", "sns.amazonaws.com", "s3.amazonaws.com", "logs.${data.aws_region.current.name}.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_kms_key" "data" {
  description             = "${var.name_prefix} data plane (S3, SQS, DynamoDB, secrets, logs)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms.json
  tags                    = var.tags
}

resource "aws_kms_alias" "data" {
  name          = "alias/${var.name_prefix}-data"
  target_key_id = aws_kms_key.data.key_id
}

resource "aws_secretsmanager_secret" "api_key" {
  for_each    = var.secret_names
  name        = each.value
  description = "${each.key} API key for ${var.name_prefix}. Value set out of band; rotation tracked as U7."
  kms_key_id  = aws_kms_key.data.arn
  tags        = var.tags

  # Local only: allow `terraform destroy` + re-apply against LocalStack without the 7-day hold.
  recovery_window_in_days = var.seed_secret_values == null ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "seed" {
  # Keys are not secret; only the values are, so iterate over nonsensitive keys.
  for_each      = toset(nonsensitive(keys(var.seed_secret_values == null ? {} : var.seed_secret_values)))
  secret_id     = aws_secretsmanager_secret.api_key[each.key].id
  secret_string = var.seed_secret_values[each.key]
}

# S3 zones. The 90-day retention requirement for the original export is one
# lifecycle rule on the exports bucket, declared rather than scripted.
#
#   raw      - page shards, deterministic keys raw/<run>/page_NNNNN.json
#   exports  - the canonical CSV per run, exports/<run>/catalogue.csv (retained 90 days)
#   curated  - derived work chunks and quarantined records

locals {
  zones = {
    raw     = { expire_days = var.raw_retention_days }
    exports = { expire_days = var.export_retention_days }
    curated = { expire_days = var.curated_retention_days }
  }
}

resource "aws_s3_bucket" "zone" {
  for_each      = local.zones
  bucket        = "${var.name_prefix}-${each.key}"
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { Zone = each.key })
}

resource "aws_s3_bucket_versioning" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "zone" {
  for_each                = aws_s3_bucket.zone
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true # cuts KMS request charges by roughly an order of magnitude
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id

  dynamic "rule" {
    # Exports: infrequent access after 30 days still satisfies the 90-day requirement.
    for_each = each.key == "exports" ? [1] : []
    content {
      id     = "transition-to-ia"
      status = "Enabled"
      filter {
        prefix = "exports/"
      }
      transition {
        days          = 30
        storage_class = "STANDARD_IA"
      }
    }
  }

  rule {
    id     = "retention"
    status = "Enabled"
    filter {}
    expiration {
      days = local.zones[each.key].expire_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }

  depends_on = [aws_s3_bucket_versioning.zone]
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.zone
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "zone" {
  for_each   = var.enforce_tls ? aws_s3_bucket.zone : {}
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.zone]
}

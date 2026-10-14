# DynamoDB state (figure 7): run ledger + page/chunk/batch/rejection items,
# per-SKU content hashes, and the run lock. On-demand billing for a workload
# that is idle 23 hours a day; point-in-time recovery on everything.

locals {
  tables = {
    runs          = { range_key = "sk", ttl = "ttl" }
    product-state = { range_key = "sk", ttl = null }
    run-lock      = { range_key = null, ttl = "expires_at" }
  }
}

resource "aws_dynamodb_table" "this" {
  for_each     = local.tables
  name         = "${var.name_prefix}-${each.key}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = each.value.range_key

  attribute {
    name = "pk"
    type = "S"
  }

  dynamic "attribute" {
    for_each = each.value.range_key == null ? [] : [each.value.range_key]
    content {
      name = attribute.value
      type = "S"
    }
  }

  dynamic "ttl" {
    for_each = each.value.ttl == null ? [] : [each.value.ttl]
    content {
      attribute_name = ttl.value
      enabled        = true
    }
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection
  tags                        = var.tags
}

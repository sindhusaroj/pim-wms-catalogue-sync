# Pipeline stage functions invoked by the state machine: StartRun, ExtractPages,
# AssembleExport, ValidateAndDiff, CheckLoadProgress, FinalizeRun, FailRun.
#
# One role per function, each scoped to the exact resources it touches (section
# 7.6): ExtractPages can write only the raw zone and read only the PIM secret;
# nothing here can read the WMS secret.

locals {
  runs  = var.table_arns["runs"]
  state = var.table_arns["product-state"]
  lock  = var.table_arns["run-lock"]
  raw   = var.bucket_arns["raw"]
  exp   = var.bucket_arns["exports"]
  cur   = var.bucket_arns["curated"]
  kms   = { actions = ["kms:Decrypt", "kms:GenerateDataKey"], resources = [var.kms_key_arn] }

  functions = {
    start_run = {
      timeout = 60, memory = 256, reserved = 2
      statements = [
        { actions = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"], resources = [local.lock] },
        { actions = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"], resources = [local.runs] },
        { actions = ["secretsmanager:GetSecretValue"], resources = [var.pim_secret_arn] },
        local.kms,
      ]
    }
    extract_pages = {
      # Reserved concurrency = map concurrency: a second line of defence for the PIM budget.
      timeout = 900, memory = 512, reserved = var.extract_concurrency
      statements = [
        { actions = ["dynamodb:GetItem", "dynamodb:PutItem"], resources = [local.runs] },
        { actions = ["s3:PutObject"], resources = ["${local.raw}/raw/*"] },
        { actions = ["secretsmanager:GetSecretValue"], resources = [var.pim_secret_arn] },
        local.kms,
      ]
    }
    assemble_export = {
      timeout = 900, memory = 1024, reserved = 2
      statements = [
        { actions = ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"], resources = [local.runs] },
        { actions = ["s3:GetObject"], resources = ["${local.raw}/raw/*"] },
        { actions = ["s3:PutObject", "s3:AbortMultipartUpload"], resources = ["${local.exp}/exports/*", "${local.cur}/work/*"] },
        local.kms,
      ]
    }
    diff_chunk = {
      timeout = 600, memory = 1024, reserved = var.diff_concurrency
      statements = [
        { actions = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"], resources = [local.runs] },
        { actions = ["dynamodb:BatchGetItem"], resources = [local.state] },
        { actions = ["s3:GetObject"], resources = ["${local.cur}/work/*"] },
        { actions = ["s3:PutObject"], resources = ["${local.cur}/quarantine/*"] },
        { actions = ["sqs:SendMessage", "sqs:GetQueueUrl"], resources = [var.load_queue_arn] },
        local.kms,
      ]
    }
    check_load_progress = {
      timeout = 60, memory = 256, reserved = 2
      statements = [
        { actions = ["dynamodb:Query", "dynamodb:GetItem"], resources = [local.runs] },
        local.kms,
      ]
    }
    finalize_run = {
      timeout = 300, memory = 512, reserved = 2
      statements = [
        { actions = ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query"], resources = [local.runs] },
        { actions = ["dynamodb:DeleteItem"], resources = [local.lock] },
        local.kms,
      ]
    }
    fail_run = {
      timeout = 60, memory = 256, reserved = 2
      statements = [
        { actions = ["dynamodb:UpdateItem"], resources = [local.runs] },
        { actions = ["dynamodb:DeleteItem"], resources = [local.lock] },
        local.kms,
      ]
    }
  }
}

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "fn" {
  for_each = local.functions

  dynamic "statement" {
    for_each = each.value.statements
    content {
      actions   = statement.value.actions
      resources = statement.value.resources
    }
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.fn[each.key].arn}:*"]
  }
  statement {
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"] # X-Ray does not support resource-level permissions
  }
}

resource "aws_cloudwatch_log_group" "fn" {
  for_each          = local.functions
  name              = "/aws/lambda/${var.name_prefix}-${replace(each.key, "_", "-")}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

resource "aws_iam_role" "fn" {
  for_each           = var.enable_compute ? local.functions : {}
  name               = "${var.name_prefix}-${replace(each.key, "_", "-")}"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "fn" {
  for_each = aws_iam_role.fn
  role     = each.value.id
  policy   = data.aws_iam_policy_document.fn[each.key].json
}

resource "aws_lambda_function" "fn" {
  for_each                       = var.enable_compute ? local.functions : {}
  function_name                  = "${var.name_prefix}-${replace(each.key, "_", "-")}"
  role                           = aws_iam_role.fn[each.key].arn
  handler                        = "app.handlers.lambda_handlers.${each.key}_handler"
  runtime                        = var.lambda_runtime
  architectures                  = [var.lambda_architecture]
  filename                       = var.lambda_package_path
  source_code_hash               = filebase64sha256(var.lambda_package_path)
  memory_size                    = each.value.memory
  timeout                        = each.value.timeout
  reserved_concurrent_executions = each.value.reserved
  environment {
    variables = var.lambda_environment
  }
  tracing_config {
    mode = "Active"
  }
  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.fn[each.key].name
  }
  tags = var.tags
}

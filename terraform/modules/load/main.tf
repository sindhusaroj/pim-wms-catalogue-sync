# Load tier: the batch queue, its dead-letter queue, and the LoadBatch Lambda
# with partial batch failure reporting (figure 11).
#
# Rate control: the event source mapping's maximum_concurrency is LOAD_CONCURRENCY
# and each loader paces itself at wms_rate_budget / LOAD_CONCURRENCY, so the
# aggregate WMS request rate is bounded by arithmetic (section 6.3).

resource "aws_sqs_queue" "dlq" {
  name                              = "${var.name_prefix}-load-dlq"
  message_retention_seconds         = 14 * 24 * 3600
  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 3600
  tags                              = var.tags
}

resource "aws_sqs_queue" "load" {
  name                              = "${var.name_prefix}-load"
  visibility_timeout_seconds        = var.visibility_timeout_seconds
  message_retention_seconds         = 4 * 24 * 3600
  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 3600
  # The loader dead-letters on real delivery attempts (MAX_DELIVERY_ATTEMPTS) with
  # full context; SQS redrive is the backstop for a consumer that crashes repeatedly.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.backstop_max_receive_count
  })
  tags = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.load.arn]
  })
}

# ------------------------------------------------------------------ loader Lambda

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "loader" {
  statement {
    sid       = "ConsumeLoadQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
    resources = [aws_sqs_queue.load.arn]
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage", "sqs:GetQueueUrl"]
    resources = [aws_sqs_queue.dlq.arn]
  }
  statement {
    sid       = "BatchLedger"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [var.runs_table_arn]
  }
  statement {
    sid       = "CommitContentHashes"
    actions   = ["dynamodb:BatchWriteItem"]
    resources = [var.product_state_table_arn]
  }
  statement {
    sid       = "WmsApiKey"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.wms_secret_arn]
  }
  statement {
    sid       = "DataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.loader.arn}:*"]
  }
  statement {
    sid       = "Tracing"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"] # X-Ray does not support resource-level permissions
  }
}

resource "aws_cloudwatch_log_group" "loader" {
  name              = "/aws/lambda/${var.name_prefix}-load-batch"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

resource "aws_iam_role" "loader" {
  count              = var.enable_compute ? 1 : 0
  name               = "${var.name_prefix}-load-batch"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "loader" {
  count  = var.enable_compute ? 1 : 0
  role   = aws_iam_role.loader[0].id
  policy = data.aws_iam_policy_document.loader.json
}

resource "aws_lambda_function" "loader" {
  count                          = var.enable_compute ? 1 : 0
  function_name                  = "${var.name_prefix}-load-batch"
  role                           = aws_iam_role.loader[0].arn
  handler                        = "app.handlers.lambda_handlers.load_batch_handler"
  runtime                        = var.lambda_runtime
  architectures                  = [var.lambda_architecture]
  filename                       = var.lambda_package_path
  source_code_hash               = filebase64sha256(var.lambda_package_path)
  memory_size                    = 512
  timeout                        = var.lambda_timeout_seconds
  reserved_concurrent_executions = var.load_concurrency + 2
  environment {
    variables = var.lambda_environment
  }
  tracing_config {
    mode = "Active"
  }
  logging_config {
    log_format = "Text" # the app already writes JSON lines and EMF documents
    log_group  = aws_cloudwatch_log_group.loader.name
  }
  tags = var.tags
}

resource "aws_lambda_event_source_mapping" "loader" {
  count                              = var.enable_compute ? 1 : 0
  event_source_arn                   = aws_sqs_queue.load.arn
  function_name                      = aws_lambda_function.loader[0].arn
  batch_size                         = 5
  maximum_batching_window_in_seconds = 0
  function_response_types            = ["ReportBatchItemFailures"]
  scaling_config {
    maximum_concurrency = var.load_concurrency # minimum allowed by AWS is 2
  }
}

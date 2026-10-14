# Step Functions (Standard outer workflow, Express Distributed Map children) and
# the EventBridge Scheduler rule that starts the daily run (figures 9-11).
#
# The state machine definition lives with the code in app/stepfunctions/ and is
# rendered here; Distributed Map MaxConcurrency is the global PIM rate control.

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  state_machine_name = "${var.name_prefix}-run"
  state_machine_arn  = "arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.state_machine_name}"
  execution_arns     = "arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:execution:${local.state_machine_name}:*"
}

data "aws_iam_policy_document" "assume_states" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

data "aws_iam_policy_document" "states" {
  statement {
    sid       = "InvokeStageFunctions"
    actions   = ["lambda:InvokeFunction"]
    resources = flatten([for arn in values(var.function_arns) : [arn, "${arn}:*"]])
  }
  statement {
    sid       = "DistributedMapChildExecutions"
    actions   = ["states:StartExecution", "states:DescribeExecution", "states:StopExecution"]
    resources = [local.state_machine_arn, local.execution_arns]
  }
  statement {
    sid = "ExecutionLogging"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"] # these log-delivery APIs do not support resource-level permissions
  }
  statement {
    sid       = "Tracing"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
    resources = ["*"]
  }
}

resource "aws_iam_role" "states" {
  name               = "${var.name_prefix}-states"
  assume_role_policy = data.aws_iam_policy_document.assume_states.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "states" {
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}

resource "aws_cloudwatch_log_group" "states" {
  name              = "/aws/vendedlogs/states/${local.state_machine_name}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

resource "aws_sfn_state_machine" "run" {
  name     = local.state_machine_name
  type     = "STANDARD"
  role_arn = aws_iam_role.states.arn
  definition = templatefile(var.definition_path, {
    start_run_arn           = var.function_arns["start_run"]
    extract_pages_arn       = var.function_arns["extract_pages"]
    assemble_export_arn     = var.function_arns["assemble_export"]
    diff_chunk_arn          = var.function_arns["diff_chunk"]
    check_load_progress_arn = var.function_arns["check_load_progress"]
    finalize_run_arn        = var.function_arns["finalize_run"]
    fail_run_arn            = var.function_arns["fail_run"]
    extract_concurrency     = var.extract_concurrency
    diff_concurrency        = var.diff_concurrency
    run_timeout_seconds     = var.run_timeout_seconds
    map_execution_type      = var.map_execution_type
  })
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }
  tracing_configuration {
    enabled = true
  }
  tags = var.tags
}

# ------------------------------------------------------------------ scheduler

resource "aws_sqs_queue" "scheduler_dlq" {
  name              = "${var.name_prefix}-scheduler-dlq"
  kms_master_key_id = var.kms_key_arn
  tags              = var.tags
}

data "aws_iam_policy_document" "assume_scheduler" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.run.arn]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.scheduler_dlq.arn]
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.assume_scheduler.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "scheduler" {
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "daily" {
  name                         = "${var.name_prefix}-daily"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = var.schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.run.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
    retry_policy {
      maximum_retry_attempts       = 3
      maximum_event_age_in_seconds = 3600
    }
    dead_letter_config {
      arn = aws_sqs_queue.scheduler_dlq.arn
    }
  }
}

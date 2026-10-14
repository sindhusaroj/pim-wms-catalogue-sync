# Alarms from section 7.7, an SNS topic for paging, and a run dashboard.
# Custom metrics are emitted by the code as CloudWatch EMF (namespace CatalogueSync).

locals {
  namespace  = "CatalogueSync"
  dimensions = { Pipeline = "catalogue-sync" }

  custom_alarms = {
    run-failed = {
      metric = "RunFailed", stat = "Sum", threshold = 0, period = 300
      text   = "A run ended FAILED. Check the run ledger failure_reason and the state machine execution."
    }
    run-partial = {
      metric = "RunPartial", stat = "Sum", threshold = 0, period = 300
      text   = "A run ended PARTIAL (failed pages, dead-lettered or ambiguous batches, or reconciliation variance)."
    }
    run-overrunning = {
      metric = "RunElapsedSeconds", stat = "Maximum", threshold = var.overrun_alarm_seconds, period = 60
      text   = "A run is still RUNNING past the overrun threshold; 10 minutes remain before the 30-minute expectation."
    }
    reconciliation-variance = {
      metric = "ReconciliationVariance", stat = "Sum", threshold = 0, period = 300
      text   = "Run totals do not reconcile: possible silent loss."
    }
    ambiguous-batches = {
      metric = "RowsAmbiguous", stat = "Sum", threshold = 0, period = 300
      text   = "Batches with an unknown WMS outcome are parked in the DLQ awaiting a decision (see ARCHITECTURE.md section 'Duplicate and ambiguous processing')."
    }
    unexpected-rejections = {
      metric = "RowsRejectedUnexpected", stat = "Sum", threshold = var.unexpected_rejection_threshold, period = 300
      text   = "The WMS rejected products for reasons outside the known business rules."
    }
  }
}

resource "aws_sns_topic" "alerts" {
  name              = "${var.name_prefix}-alerts"
  kms_master_key_id = var.kms_key_arn
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alarm_emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_cloudwatch_metric_alarm" "custom" {
  for_each            = local.custom_alarms
  alarm_name          = "${var.name_prefix}-${each.key}"
  alarm_description   = each.value.text
  namespace           = local.namespace
  metric_name         = each.value.metric
  dimensions          = local.dimensions
  statistic           = each.value.stat
  period              = each.value.period
  evaluation_periods  = 1
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${var.name_prefix}-dlq-depth"
  alarm_description   = "Batches are in the load DLQ: replay with `python -m app.main replay-dlq <run_id>` after triage."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = var.dlq_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_metric_alarm" "state_machine" {
  for_each            = var.enable_state_machine_alarms ? { ExecutionsFailed = "failed", ExecutionsTimedOut = "timed-out", ExecutionsAborted = "aborted" } : {}
  alarm_name          = "${var.name_prefix}-execution-${each.value}"
  alarm_description   = "The catalogue sync state machine execution ${each.value}."
  namespace           = "AWS/States"
  metric_name         = each.key
  dimensions          = { StateMachineArn = var.state_machine_arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

resource "aws_cloudwatch_dashboard" "run" {
  dashboard_name = "${var.name_prefix}-run"
  dashboard_body = jsonencode({
    widgets = [
      for i, group in [
        ["ProductsExtracted", "ProductsChanged", "ProductsUnchanged", "ProductsInvalid"],
        ["SkusAccepted", "SkusRejectedExpected", "SkusRejectedUnexpected"],
        ["BatchesDispatched", "BatchesDelivered", "BatchesDeadLettered", "BatchesAmbiguous"],
        ["PimRetries", "WmsRetries", "PagesFailed"],
        ["RunDurationSeconds", "RunElapsedSeconds"],
        ["ReconciliationVariance", "RunPartial", "RunFailed"],
        ] : {
        type   = "metric"
        x      = (i % 2) * 12
        y      = floor(i / 2) * 6
        width  = 12
        height = 6
        properties = {
          region  = var.region
          stat    = "Sum"
          period  = 300
          title   = join(", ", group)
          metrics = [for m in group : [local.namespace, m, "Pipeline", "catalogue-sync"]]
        }
      }
    ]
  })
}

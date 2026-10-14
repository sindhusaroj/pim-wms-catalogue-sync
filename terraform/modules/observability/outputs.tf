output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "alarm_names" {
  value = concat([for a in aws_cloudwatch_metric_alarm.custom : a.alarm_name], [aws_cloudwatch_metric_alarm.dlq_depth.alarm_name])
}

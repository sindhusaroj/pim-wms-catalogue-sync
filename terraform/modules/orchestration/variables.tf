variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "definition_path" {
  description = "Path to the ASL template (app/stepfunctions/catalogue_sync.asl.json)."
  type        = string
}

variable "function_arns" {
  description = "From the extract module, keyed by stage name."
  type        = map(string)
}

variable "extract_concurrency" {
  description = "Distributed Map MaxConcurrency. This is also the PIM rate-limit control."
  type        = number
  default     = 8
}

variable "map_execution_type" {
  description = "Distributed Map child executions: EXPRESS keeps per-page transitions cheap. LocalStack 4.4 only supports STANDARD."
  type        = string
  default     = "EXPRESS"
}

variable "diff_concurrency" {
  type    = number
  default = 4
}

variable "run_timeout_seconds" {
  type    = number
  default = 2700
}

variable "schedule_expression" {
  description = "Daily, before business hours."
  type        = string
  default     = "cron(0 5 * * ? *)"
}

variable "schedule_timezone" {
  type    = string
  default = "Europe/London"
}

variable "schedule_enabled" {
  type    = bool
  default = true
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "tags" {
  type    = map(string)
  default = {}
}

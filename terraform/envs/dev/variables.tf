variable "environment" {
  type = string
}

variable "region" {
  type    = string
  default = "eu-west-2"
}

variable "cost_centre" {
  type    = string
  default = "integration"
}

variable "product_api_url" {
  type = string
}

variable "warehouse_api_url" {
  type = string
}

variable "lambda_package_path" {
  type    = string
  default = "../../../build/lambda.zip"
}

variable "pim_rate_budget" {
  description = "Requests/s across ALL extract workers: 80 % of the documented 10 req/s."
  type        = number
  default     = 8
}

variable "wms_rate_budget" {
  description = "Requests/s across ALL loaders: 80 % of the documented 20 req/s."
  type        = number
  default     = 16
}

variable "extract_concurrency" {
  description = "In flight = rate x latency: ~24 sustains 8 req/s at 3 s latency."
  type        = number
  default     = 24
}

variable "load_concurrency" {
  type    = number
  default = 48
}

variable "pages_per_extract_batch" {
  type    = number
  default = 10
}

variable "wms_honours_idempotency_key" {
  description = "Set true only once the Warehouse team confirms they deduplicate on Idempotency-Key (open question, section 5.3)."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  type    = string
  default = "cron(0 5 * * ? *)"
}

variable "schedule_enabled" {
  type    = bool
  default = true
}

variable "alarm_emails" {
  type    = list(string)
  default = []
}

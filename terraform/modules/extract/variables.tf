variable "name_prefix" {
  type = string
}

variable "enable_compute" {
  type    = bool
  default = true
}

variable "kms_key_arn" {
  type = string
}

variable "table_arns" {
  description = "From the state module: runs, product-state, run-lock."
  type        = map(string)
}

variable "bucket_arns" {
  description = "From the storage module: raw, exports, curated."
  type        = map(string)
}

variable "pim_secret_arn" {
  type = string
}

variable "load_queue_arn" {
  type = string
}

variable "extract_concurrency" {
  description = "Distributed Map MaxConcurrency; each worker runs at pim_rate_budget / extract_concurrency."
  type        = number
  default     = 8
}

variable "diff_concurrency" {
  type    = number
  default = 4
}

variable "lambda_package_path" {
  type    = string
  default = null
}

variable "lambda_environment" {
  type    = map(string)
  default = {}
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12"
}

variable "lambda_architecture" {
  type    = string
  default = "arm64"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "tags" {
  type    = map(string)
  default = {}
}

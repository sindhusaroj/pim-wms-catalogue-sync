variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "enable_compute" {
  description = "Create the loader Lambda and its event source mapping. False for the LocalStack data-plane-only environment."
  type        = bool
  default     = true
}

variable "visibility_timeout_seconds" {
  description = "Must exceed the loader timeout (AWS guidance: 6x). The loader shortens it explicitly for retries."
  type        = number
  default     = 1800
}

variable "backstop_max_receive_count" {
  type    = number
  default = 20
}

variable "load_concurrency" {
  type    = number
  default = 8
  validation {
    condition     = var.load_concurrency >= 2
    error_message = "SQS event source maximum_concurrency must be at least 2."
  }
}

variable "runs_table_arn" {
  type = string
}

variable "product_state_table_arn" {
  type = string
}

variable "wms_secret_arn" {
  type = string
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

variable "lambda_timeout_seconds" {
  type    = number
  default = 300
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "tags" {
  type    = map(string)
  default = {}
}

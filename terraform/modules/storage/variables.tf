variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "export_retention_days" {
  description = "Retention of the original CSV export per run. Business requirement: 90 days."
  type        = number
  default     = 90
  validation {
    condition     = var.export_retention_days >= 90
    error_message = "The original export must be retained for at least 90 days."
  }
}

variable "raw_retention_days" {
  type    = number
  default = 14
}

variable "curated_retention_days" {
  type    = number
  default = 30
}

variable "force_destroy" {
  description = "Allow destroying non-empty buckets. Only for ephemeral environments."
  type        = bool
  default     = false
}

variable "enforce_tls" {
  description = "Attach a deny-non-TLS bucket policy. Disabled for LocalStack, which serves plain HTTP."
  type        = bool
  default     = true
}

variable "tags" {
  type    = map(string)
  default = {}
}

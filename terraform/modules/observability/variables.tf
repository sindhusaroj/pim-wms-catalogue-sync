variable "name_prefix" {
  type = string
}

variable "region" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "dlq_name" {
  type = string
}

variable "state_machine_arn" {
  type    = string
  default = null
}

variable "enable_state_machine_alarms" {
  description = "Static switch (the ARN is unknown until apply, so it cannot drive for_each)."
  type        = bool
  default     = false
}

variable "alarm_emails" {
  type    = list(string)
  default = []
}

variable "overrun_alarm_seconds" {
  description = "Alarm while a run is still going at this age: 20 minutes leaves 10 to intervene."
  type        = number
  default     = 1200
}

variable "unexpected_rejection_threshold" {
  type    = number
  default = 0
}

variable "tags" {
  type    = map(string)
  default = {}
}

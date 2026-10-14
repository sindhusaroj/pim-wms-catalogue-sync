variable "name_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "point_in_time_recovery" {
  type    = bool
  default = true
}

variable "deletion_protection" {
  type    = bool
  default = true
}

variable "tags" {
  type    = map(string)
  default = {}
}

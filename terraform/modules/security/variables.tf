variable "name_prefix" {
  type = string
}

variable "secret_names" {
  description = "Logical name => Secrets Manager secret name, e.g. { pim = \"catalogue-sync/prod/pim-api-key\" }."
  type        = map(string)
}

variable "seed_secret_values" {
  description = "LOCAL ENVIRONMENT ONLY: values to seed into the secrets. Leave null everywhere else."
  type        = map(string)
  default     = null
  sensitive   = true
}

variable "tags" {
  type    = map(string)
  default = {}
}

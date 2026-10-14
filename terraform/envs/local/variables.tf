variable "region" {
  type    = string
  default = "eu-west-2"
}

variable "localstack_endpoint" {
  description = "http://localstack:4566 from the compose network, http://localhost:4566 from the host."
  type        = string
  default     = "http://localhost:4566"
}

variable "enable_compute" {
  description = "Also deploy the Lambda functions and the state machine to LocalStack (requires scripts/build_lambda.sh)."
  type        = bool
  default     = false
}

variable "lambda_package_path" {
  type    = string
  default = "../../../build/lambda.zip"
}

variable "lambda_architecture" {
  description = "x86_64 locally (LocalStack runs Lambda containers on the host architecture); arm64 in AWS."
  type        = string
  default     = "x86_64"
}

variable "extract_concurrency" {
  type    = number
  default = 8
}

variable "load_concurrency" {
  type    = number
  default = 8
}

# Development root module. Identical to prod/ apart from its tfvars and backend;
# one state file per environment, S3 backend with DynamoDB locking (section 9.1).
#
#   ../../../scripts/build_lambda.sh
#   terraform init -backend-config=backend.hcl
#   terraform plan -var-file=terraform.tfvars

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
  backend "s3" {} # bucket/key/region/dynamodb_table supplied by backend.hcl
}

provider "aws" {
  region = var.region
  default_tags {
    tags = local.tags
  }
}

locals {
  name_prefix = "catalogue-sync-${var.environment}"
  tags        = { Application = "catalogue-sync", Environment = var.environment, ManagedBy = "terraform", CostCentre = var.cost_centre }
  secret_names = {
    pim = "catalogue-sync/${var.environment}/pim-api-key"
    wms = "catalogue-sync/${var.environment}/wms-api-key"
  }
  lambda_environment = {
    APP_ENV                     = var.environment
    NAME_PREFIX                 = local.name_prefix
    PRODUCT_API_URL             = var.product_api_url
    WAREHOUSE_API_URL           = var.warehouse_api_url
    PIM_API_KEY_SECRET_ID       = local.secret_names.pim
    WMS_API_KEY_SECRET_ID       = local.secret_names.wms
    PIM_RATE_BUDGET             = tostring(var.pim_rate_budget)
    WMS_RATE_BUDGET             = tostring(var.wms_rate_budget)
    EXTRACT_CONCURRENCY         = tostring(var.extract_concurrency)
    LOAD_CONCURRENCY            = tostring(var.load_concurrency)
    PAGES_PER_EXTRACT_BATCH     = tostring(var.pages_per_extract_batch)
    WMS_HONOURS_IDEMPOTENCY_KEY = tostring(var.wms_honours_idempotency_key)
    METRICS_ENABLED             = "true"
  }
}

module "security" {
  source       = "../../modules/security"
  name_prefix  = local.name_prefix
  secret_names = local.secret_names
}

module "storage" {
  source      = "../../modules/storage"
  name_prefix = local.name_prefix
  kms_key_arn = module.security.kms_key_arn
}

module "state" {
  source      = "../../modules/state"
  name_prefix = local.name_prefix
  kms_key_arn = module.security.kms_key_arn
}

module "load" {
  source                  = "../../modules/load"
  name_prefix             = local.name_prefix
  kms_key_arn             = module.security.kms_key_arn
  load_concurrency        = var.load_concurrency
  runs_table_arn          = module.state.table_arns["runs"]
  product_state_table_arn = module.state.table_arns["product-state"]
  wms_secret_arn          = module.security.secret_arns["wms"]
  lambda_package_path     = var.lambda_package_path
  lambda_environment      = local.lambda_environment
}

module "extract" {
  source              = "../../modules/extract"
  name_prefix         = local.name_prefix
  kms_key_arn         = module.security.kms_key_arn
  table_arns          = module.state.table_arns
  bucket_arns         = module.storage.bucket_arns
  pim_secret_arn      = module.security.secret_arns["pim"]
  load_queue_arn      = module.load.queue_arn
  extract_concurrency = var.extract_concurrency
  lambda_package_path = var.lambda_package_path
  lambda_environment  = local.lambda_environment
}

module "orchestration" {
  source              = "../../modules/orchestration"
  name_prefix         = local.name_prefix
  kms_key_arn         = module.security.kms_key_arn
  definition_path     = "${path.root}/../../../app/stepfunctions/catalogue_sync.asl.json"
  function_arns       = module.extract.function_arns
  extract_concurrency = var.extract_concurrency
  schedule_expression = var.schedule_expression
  schedule_enabled    = var.schedule_enabled
}

module "observability" {
  source                      = "../../modules/observability"
  name_prefix                 = local.name_prefix
  region                      = var.region
  kms_key_arn                 = module.security.kms_key_arn
  dlq_name                    = module.load.dlq_name
  state_machine_arn           = module.orchestration.state_machine_arn
  enable_state_machine_alarms = true
  alarm_emails                = var.alarm_emails
}

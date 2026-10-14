# Local environment on LocalStack.
#
# By default this deploys the data plane the application uses - KMS, Secrets
# Manager (seeded with the mock API keys), the three S3 zones with their
# lifecycle rules, the DynamoDB tables, the SQS queue and DLQ, SNS and the
# CloudWatch alarms - and the pipeline is driven by `python -m app.main run`,
# which executes the same stage functions in the state machine's order.
#
# Set enable_compute = true (and build the package with scripts/build_lambda.sh)
# to also deploy the Lambda functions and state machine; see UNFINISHED.md.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region                      = var.region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  s3_use_path_style           = true

  endpoints {
    cloudwatch     = var.localstack_endpoint
    dynamodb       = var.localstack_endpoint
    iam            = var.localstack_endpoint
    kms            = var.localstack_endpoint
    lambda         = var.localstack_endpoint
    logs           = var.localstack_endpoint
    s3             = var.localstack_endpoint
    scheduler      = var.localstack_endpoint
    secretsmanager = var.localstack_endpoint
    sfn            = var.localstack_endpoint
    sns            = var.localstack_endpoint
    sqs            = var.localstack_endpoint
    sts            = var.localstack_endpoint
  }

  default_tags {
    tags = local.tags
  }
}

locals {
  name_prefix = "catalogue-sync-local"
  tags        = { Application = "catalogue-sync", Environment = "local", ManagedBy = "terraform" }
  secret_names = {
    pim = "catalogue-sync/local/pim-api-key"
    wms = "catalogue-sync/local/wms-api-key"
  }
}

module "security" {
  source       = "../../modules/security"
  name_prefix  = local.name_prefix
  secret_names = local.secret_names
  # The supplied mock services' keys. Local only: never seed secrets in a real environment.
  seed_secret_values = {
    pim = "challenge-product-key"
    wms = "challenge-warehouse-key"
  }
}

module "storage" {
  source        = "../../modules/storage"
  name_prefix   = local.name_prefix
  kms_key_arn   = module.security.kms_key_arn
  force_destroy = true
  enforce_tls   = false # LocalStack serves plain HTTP
}

module "state" {
  source                 = "../../modules/state"
  name_prefix            = local.name_prefix
  kms_key_arn            = module.security.kms_key_arn
  deletion_protection    = false
  point_in_time_recovery = true
}

module "load" {
  source                     = "../../modules/load"
  name_prefix                = local.name_prefix
  kms_key_arn                = module.security.kms_key_arn
  enable_compute             = var.enable_compute
  # Local: a dropped ReceiveMessage response strands its messages until the
  # visibility timeout expires. 60 s still exceeds a worst-case delivery
  # attempt here (6 tries with jittered backoff against an instant mock).
  visibility_timeout_seconds = 60
  load_concurrency           = var.load_concurrency
  runs_table_arn             = module.state.table_arns["runs"]
  product_state_table_arn    = module.state.table_arns["product-state"]
  wms_secret_arn             = module.security.secret_arns["wms"]
  lambda_package_path        = var.lambda_package_path
  lambda_architecture        = var.lambda_architecture
  lambda_environment         = local.lambda_environment
  log_retention_days         = 7
}

module "extract" {
  source              = "../../modules/extract"
  name_prefix         = local.name_prefix
  enable_compute      = var.enable_compute
  kms_key_arn         = module.security.kms_key_arn
  table_arns          = module.state.table_arns
  bucket_arns         = module.storage.bucket_arns
  pim_secret_arn      = module.security.secret_arns["pim"]
  load_queue_arn      = module.load.queue_arn
  extract_concurrency = var.extract_concurrency
  lambda_package_path = var.lambda_package_path
  lambda_architecture = var.lambda_architecture
  lambda_environment  = local.lambda_environment
  log_retention_days  = 7
}

module "orchestration" {
  count               = var.enable_compute ? 1 : 0
  source              = "../../modules/orchestration"
  name_prefix         = local.name_prefix
  kms_key_arn         = module.security.kms_key_arn
  definition_path     = "${path.root}/../../../app/stepfunctions/catalogue_sync.asl.json"
  function_arns       = module.extract.function_arns
  extract_concurrency = var.extract_concurrency
  schedule_enabled    = false      # run on demand locally
  map_execution_type  = "STANDARD" # LocalStack 4.4 does not support EXPRESS map children
  log_retention_days  = 7
}

module "observability" {
  source                      = "../../modules/observability"
  name_prefix                 = local.name_prefix
  region                      = var.region
  kms_key_arn                 = module.security.kms_key_arn
  dlq_name                    = module.load.dlq_name
  state_machine_arn           = var.enable_compute ? module.orchestration[0].state_machine_arn : null
  enable_state_machine_alarms = var.enable_compute
}

locals {
  lambda_environment = {
    APP_ENV                 = "local"
    AWS_ENDPOINT_URL        = "http://localstack:4566"
    NAME_PREFIX             = local.name_prefix
    PRODUCT_API_URL         = "http://product-api:8000"
    WAREHOUSE_API_URL       = "http://warehouse-api:8000"
    PIM_API_KEY_SECRET_ID   = local.secret_names.pim
    WMS_API_KEY_SECRET_ID   = local.secret_names.wms
    EXTRACT_CONCURRENCY     = tostring(var.extract_concurrency)
    LOAD_CONCURRENCY        = tostring(var.load_concurrency)
    PAGES_PER_EXTRACT_BATCH = "10"
  }
}

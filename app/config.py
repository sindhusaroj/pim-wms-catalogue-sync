"""Runtime configuration.

Non-secret settings come from the environment, with defaults that match the
``terraform/envs/local`` deployment on LocalStack. API keys are never read from
here: they live in Secrets Manager and are resolved by ``app.secrets`` (F7).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from functools import lru_cache


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _bool(name: str, default: bool) -> bool:
    return str(_env(name, str(default))).lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    app_env: str = "local"
    name_prefix: str = "catalogue-sync-local"

    # External systems
    product_api_url: str = "http://localhost:8001"
    warehouse_api_url: str = "http://localhost:8002"
    pim_api_key_secret_id: str = "catalogue-sync/local/pim-api-key"
    wms_api_key_secret_id: str = "catalogue-sync/local/wms-api-key"

    # AWS
    aws_region: str = "eu-west-2"
    aws_endpoint_url: str | None = "http://localhost:4566"
    raw_bucket: str = ""
    export_bucket: str = ""
    curated_bucket: str = ""
    runs_table: str = ""
    product_state_table: str = ""
    run_lock_table: str = ""
    load_queue_name: str = ""
    load_dlq_name: str = ""

    # Vendor contract limits (hard) and the rate budget we run at (80 %, section 6.3)
    page_size: int = 500
    batch_size: int = 100
    pim_rate_budget: float = 8.0
    wms_rate_budget: float = 16.0
    extract_concurrency: int = 8
    load_concurrency: int = 8
    pages_per_extract_batch: int = 10
    diff_chunk_size: int = 5000
    diff_concurrency: int = 4

    # Retry policy (section 7.1)
    max_attempts: int = 6
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 32.0
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0
    keepalive_expiry_seconds: float = 2.0
    breaker_window: int = 20
    breaker_failure_ratio: float = 0.5
    breaker_cooldown_seconds: float = 30.0

    # Load behaviour
    max_delivery_attempts: int = 5
    retry_visibility_seconds: int = 10
    wms_honours_idempotency_key: bool = False
    run_lock_ttl_seconds: int = 2 * 60 * 60
    load_wait_timeout_seconds: int = 25 * 60

    def __post_init__(self) -> None:
        if self.batch_size > 100:
            raise ValueError("WMS hard limit is 100 products per batch")
        if self.page_size > 500:
            raise ValueError("PIM hard limit is 500 products per page")
        defaults = {
            "raw_bucket": f"{self.name_prefix}-raw",
            "export_bucket": f"{self.name_prefix}-exports",
            "curated_bucket": f"{self.name_prefix}-curated",
            "runs_table": f"{self.name_prefix}-runs",
            "product_state_table": f"{self.name_prefix}-product-state",
            "run_lock_table": f"{self.name_prefix}-run-lock",
            "load_queue_name": f"{self.name_prefix}-load",
            "load_dlq_name": f"{self.name_prefix}-load-dlq",
        }
        for attr, value in defaults.items():
            if not getattr(self, attr):
                object.__setattr__(self, attr, value)

    @property
    def extract_worker_rate(self) -> float:
        """Partitioned budget: each extract worker gets budget / N (section 6.3)."""
        return self.pim_rate_budget / self.extract_concurrency

    @property
    def load_worker_rate(self) -> float:
        return self.wms_rate_budget / self.load_concurrency

    def with_overrides(self, **kwargs) -> "Settings":
        return replace(self, **kwargs)


def load_settings() -> Settings:
    app_env = _env("APP_ENV", "local")
    endpoint_default = "http://localhost:4566" if app_env == "local" else None
    if app_env == "local":
        # LocalStack accepts any credentials; never needed against real AWS.
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    return Settings(
        app_env=app_env,
        name_prefix=_env("NAME_PREFIX", "catalogue-sync-local"),
        product_api_url=_env("PRODUCT_API_URL", "http://localhost:8001"),
        warehouse_api_url=_env("WAREHOUSE_API_URL", "http://localhost:8002"),
        pim_api_key_secret_id=_env("PIM_API_KEY_SECRET_ID", "catalogue-sync/local/pim-api-key"),
        wms_api_key_secret_id=_env("WMS_API_KEY_SECRET_ID", "catalogue-sync/local/wms-api-key"),
        aws_region=_env("AWS_REGION", _env("AWS_DEFAULT_REGION", "eu-west-2")),
        aws_endpoint_url=_env("AWS_ENDPOINT_URL", endpoint_default),
        raw_bucket=_env("RAW_BUCKET", ""),
        export_bucket=_env("EXPORT_BUCKET", ""),
        curated_bucket=_env("CURATED_BUCKET", ""),
        runs_table=_env("RUNS_TABLE", ""),
        product_state_table=_env("PRODUCT_STATE_TABLE", ""),
        run_lock_table=_env("RUN_LOCK_TABLE", ""),
        load_queue_name=_env("LOAD_QUEUE_NAME", ""),
        load_dlq_name=_env("LOAD_DLQ_NAME", ""),
        page_size=_int("PAGE_SIZE", 500),
        batch_size=_int("BATCH_SIZE", 100),
        pim_rate_budget=_float("PIM_RATE_BUDGET", 8.0),
        wms_rate_budget=_float("WMS_RATE_BUDGET", 16.0),
        extract_concurrency=_int("EXTRACT_CONCURRENCY", 8),
        load_concurrency=_int("LOAD_CONCURRENCY", 8),
        pages_per_extract_batch=_int("PAGES_PER_EXTRACT_BATCH", 10),
        diff_chunk_size=_int("DIFF_CHUNK_SIZE", 5000),
        diff_concurrency=_int("DIFF_CONCURRENCY", 4),
        max_attempts=_int("MAX_ATTEMPTS", 6),
        backoff_base_seconds=_float("BACKOFF_BASE_SECONDS", 1.0),
        backoff_cap_seconds=_float("BACKOFF_CAP_SECONDS", 32.0),
        max_delivery_attempts=_int("MAX_DELIVERY_ATTEMPTS", 5),
        retry_visibility_seconds=_int("RETRY_VISIBILITY_SECONDS", 10),
        keepalive_expiry_seconds=_float("KEEPALIVE_EXPIRY_SECONDS", 2.0),
        breaker_cooldown_seconds=_float("BREAKER_COOLDOWN_SECONDS", 30.0),
        wms_honours_idempotency_key=_bool("WMS_HONOURS_IDEMPOTENCY_KEY", False),
        load_wait_timeout_seconds=_int("LOAD_WAIT_TIMEOUT_SECONDS", 25 * 60),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()

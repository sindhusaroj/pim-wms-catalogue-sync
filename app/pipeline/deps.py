"""Wiring of AWS clients, stores and vendor clients for the pipeline stages."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

import boto3
import httpx
from botocore.config import Config

from app.clients.product_api import ProductApiClient
from app.clients.warehouse_api import WarehouseApiClient
from app.config import Settings
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.http import ResilientClient, RetryPolicy, build_http_client
from app.resilience.rate_limit import RateLimiter
from app.secrets import SecretProvider, SecretsManagerSecret
from app.state.ledger import Ledger
from app.storage.s3 import S3Store


def aws_client(settings: Settings, service: str):
    return boto3.client(
        service,
        region_name=settings.aws_region,
        endpoint_url=settings.aws_endpoint_url,
        config=Config(retries={"mode": "standard", "max_attempts": 5}, max_pool_connections=50),
    )


@dataclass
class Deps:
    settings: Settings
    ledger: Ledger
    raw: S3Store
    exports: S3Store
    curated: S3Store
    sqs: object
    pim_secret: SecretProvider
    wms_secret: SecretProvider
    pim_http: httpx.Client
    wms_http: httpx.Client
    sleep: Callable[[float], None] = time.sleep
    _queue_urls: dict = field(default_factory=dict)

    def _policy(self) -> RetryPolicy:
        s = self.settings
        return RetryPolicy(s.max_attempts, s.backoff_base_seconds, s.backoff_cap_seconds)

    def _breaker(self) -> CircuitBreaker:
        s = self.settings
        return CircuitBreaker(window=s.breaker_window, failure_ratio=s.breaker_failure_ratio,
                              cooldown_seconds=s.breaker_cooldown_seconds)

    def pim_client(self, rate_per_second: float) -> ProductApiClient:
        http = ResilientClient(self.pim_http, secret=self.pim_secret, limiter=RateLimiter(rate_per_second, sleep=self.sleep),
                               breaker=self._breaker(), policy=self._policy(), sleep=self.sleep)
        return ProductApiClient(http, self.settings.page_size)

    def wms_client(self, rate_per_second: float) -> WarehouseApiClient:
        http = ResilientClient(self.wms_http, secret=self.wms_secret, limiter=RateLimiter(rate_per_second, sleep=self.sleep),
                               breaker=self._breaker(), policy=self._policy(), sleep=self.sleep)
        return WarehouseApiClient(http, honours_idempotency_key=self.settings.wms_honours_idempotency_key)

    def queue_url(self, name: str | None = None) -> str:
        name = name or self.settings.load_queue_name
        if name not in self._queue_urls:
            self._queue_urls[name] = self.sqs.get_queue_url(QueueName=name)["QueueUrl"]
        return self._queue_urls[name]

    @property
    def dlq_url(self) -> str:
        return self.queue_url(self.settings.load_dlq_name)


def build_deps(settings: Settings, *, pim_http: httpx.Client | None = None, wms_http: httpx.Client | None = None,
               pim_secret: SecretProvider | None = None, wms_secret: SecretProvider | None = None,
               sleep: Callable[[float], None] = time.sleep) -> Deps:
    s3 = aws_client(settings, "s3")
    secrets = None
    if pim_secret is None or wms_secret is None:
        secrets = aws_client(settings, "secretsmanager")
    return Deps(
        settings=settings,
        ledger=Ledger(aws_client(settings, "dynamodb"), settings.runs_table, settings.product_state_table,
                      settings.run_lock_table),
        raw=S3Store(s3, settings.raw_bucket),
        exports=S3Store(s3, settings.export_bucket),
        curated=S3Store(s3, settings.curated_bucket),
        sqs=aws_client(settings, "sqs"),
        pim_secret=pim_secret or SecretsManagerSecret(secrets, settings.pim_api_key_secret_id),
        wms_secret=wms_secret or SecretsManagerSecret(secrets, settings.wms_api_key_secret_id),
        pim_http=pim_http or build_http_client(settings.product_api_url, settings.connect_timeout_seconds,
                                               settings.read_timeout_seconds, settings.keepalive_expiry_seconds),
        wms_http=wms_http or build_http_client(settings.warehouse_api_url, settings.connect_timeout_seconds,
                                               settings.read_timeout_seconds, settings.keepalive_expiry_seconds),
        sleep=sleep,
    )

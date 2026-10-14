import importlib.util
import json
import os
import threading
import time
from pathlib import Path

import httpx
import pytest

os.environ.setdefault("METRICS_ENABLED", "false")

ROOT = Path(__file__).resolve().parents[1]
REGION = "eu-west-2"


def load_mock_app(name: str):
    """Import a supplied mock service (unchanged) under a unique module name.

    Each call returns a fresh module, so the mock's fault-injection request
    counters start from zero for every test.
    """
    path = ROOT / "mock-services" / name / "app" / "main.py"
    spec = importlib.util.spec_from_file_location(f"mock_{name.replace('-', '_')}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_aws_resources(settings) -> None:
    """Mirror of terraform/modules/{storage,state,load,security} for moto."""
    import boto3

    kwargs = {"region_name": settings.aws_region, "endpoint_url": settings.aws_endpoint_url}
    s3 = boto3.client("s3", **kwargs)
    for bucket in (settings.raw_bucket, settings.export_bucket, settings.curated_bucket):
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": settings.aws_region})
    ddb = boto3.client("dynamodb", **kwargs)
    for table, keys in ((settings.runs_table, ("pk", "sk")), (settings.product_state_table, ("pk", "sk")),
                        (settings.run_lock_table, ("pk",))):
        ddb.create_table(
            TableName=table,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": k, "AttributeType": "S"} for k in keys],
            KeySchema=[{"AttributeName": k, "KeyType": "HASH" if i == 0 else "RANGE"} for i, k in enumerate(keys)],
        )
    sqs = boto3.client("sqs", **kwargs)
    dlq_url = sqs.create_queue(QueueName=settings.load_dlq_name)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.create_queue(QueueName=settings.load_queue_name, Attributes={
        "VisibilityTimeout": "60",
        # The app dead-letters on real delivery attempts; SQS redrive is only the crash backstop.
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "20"}),
    })
    sm = boto3.client("secretsmanager", **kwargs)
    sm.create_secret(Name=settings.pim_api_key_secret_id, SecretString="challenge-product-key")
    sm.create_secret(Name=settings.wms_api_key_secret_id, SecretString="challenge-warehouse-key")


@pytest.fixture
def settings():
    from app.config import Settings

    return Settings(
        app_env="test", name_prefix="test-sync", aws_region=REGION, aws_endpoint_url=None,
        extract_concurrency=4, load_concurrency=4, diff_chunk_size=2500, diff_concurrency=2,
        load_wait_timeout_seconds=120, breaker_cooldown_seconds=1, retry_visibility_seconds=0,
    )


@pytest.fixture
def aws(monkeypatch, settings):
    from moto import mock_aws

    for key, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                       "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(key, value)
    # moto's in-memory backends are not thread-safe; the pipeline is. Serialise the
    # AWS calls (only) so the worker threads still run concurrently against the mocks.
    from botocore.client import BaseClient

    original = BaseClient._make_api_call
    aws_lock = threading.RLock()

    def serialised(self, operation, params):
        if operation == "ReceiveMessage":
            # A long poll would hold the lock and starve every other thread.
            params = {**params, "WaitTimeSeconds": 0}
        with aws_lock:
            response = original(self, operation, params)
        if operation == "ReceiveMessage" and not response.get("Messages"):
            time.sleep(0.05)
        return response

    monkeypatch.setattr(BaseClient, "_make_api_call", serialised)
    with mock_aws():
        create_aws_resources(settings)
        yield


@pytest.fixture
def mock_pim():
    return load_mock_app("product-api")


@pytest.fixture
def mock_wms():
    return load_mock_app("warehouse-api")


@pytest.fixture
def make_deps(aws, settings, mock_pim, mock_wms):
    """Pipeline deps wired to moto and the real (unmodified) mock services in-process."""
    from fastapi.testclient import TestClient

    from app.pipeline.deps import build_deps

    def factory(pim_http=None, wms_http=None, **overrides):
        return build_deps(
            settings.with_overrides(**overrides) if overrides else settings,
            pim_http=pim_http or TestClient(mock_pim.app, raise_server_exceptions=False),
            wms_http=wms_http or TestClient(mock_wms.app, raise_server_exceptions=False),
            sleep=lambda _seconds: None,  # Retry-After and rate pacing are asserted elsewhere
        )

    return factory


class Forwarder:
    """httpx client in front of a mock app: records every request and lets a test
    intercept some of them (inject 5xx, timeouts) without modifying the mock."""

    def __init__(self, app, intercept=None):
        from fastapi.testclient import TestClient

        self.upstream = TestClient(app, raise_server_exceptions=False)  # like a real server: 500
        self.intercept = intercept
        self.requests: list[httpx.Request] = []
        self.delivered: list[str] = []  # SKUs in batch POSTs the mock answered with 200
        self._lock = threading.Lock()
        self.client = httpx.Client(base_url="http://testserver", transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.requests.append(request)
        if self.intercept is not None:
            replaced = self.intercept(request)
            if replaced is not None:
                return replaced
        response = self.upstream.request(request.method, request.url.path, params=request.url.params,
                                         content=request.content, headers=dict(request.headers))
        if request.method == "POST" and response.status_code == 200:
            with self._lock:
                self.delivered.extend(p["sku"] for p in json.loads(request.content)["products"])
        return httpx.Response(response.status_code, headers=response.headers, content=response.content)

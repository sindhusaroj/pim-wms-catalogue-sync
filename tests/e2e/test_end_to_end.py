"""End-to-end: the real pipeline against the docker mock PIM/WMS and LocalStack
resources created by terraform/envs/local. Nothing is stubbed.

    docker compose -f docker-compose.yml -f docker-compose.localstack.yml up -d --build
    docker compose -f docker-compose.yml -f docker-compose.localstack.yml run --rm terraform
    pytest -m e2e
"""
import csv
import io
import socket
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.config import load_settings
from app.pipeline.deps import aws_client, build_deps
from app.pipeline.start_run import RunInProgress, start_run
from app.local_orchestrator import run_pipeline

pytestmark = pytest.mark.e2e

TOTAL = 12_000


def _reachable(port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex(("localhost", port)) == 0


if not all(_reachable(p) for p in (4566, 8001, 8002)):
    pytest.skip("docker compose stack (LocalStack :4566, mocks :8001/:8002) is not running",
                allow_module_level=True)


@pytest.fixture(scope="module")
def settings():
    return load_settings()  # APP_ENV=local defaults: LocalStack endpoint, catalogue-sync-local-* names


@pytest.fixture(scope="module")
def deps(settings):
    return build_deps(settings)


@pytest.fixture(scope="module")
def clean_slate(settings, deps):
    """Forget committed hashes and drain queues so the first run sends everything."""
    ddb = aws_client(settings, "dynamodb")
    keys = []
    for page in ddb.get_paginator("scan").paginate(TableName=settings.product_state_table, ProjectionExpression="pk, sk"):
        keys.extend(page["Items"])

    def delete(group):
        ddb.batch_write_item(RequestItems={settings.product_state_table: [{"DeleteRequest": {"Key": k}} for k in group]})

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(delete, [keys[i:i + 25] for i in range(0, len(keys), 25)]))
    for url in (deps.queue_url(), deps.dlq_url):
        deps.sqs.purge_queue(QueueUrl=url)
    holder = deps.ledger.lock_holder()
    if holder:
        deps.ledger.release_lock(holder["run_id"])


@pytest.fixture(scope="module")
def first_run(clean_slate, deps):
    return run_pipeline(deps)


def test_terraform_created_the_data_plane(settings):
    s3 = aws_client(settings, "s3")
    rules = s3.get_bucket_lifecycle_configuration(Bucket=settings.export_bucket)["Rules"]
    retention = next(r for r in rules if r["ID"] == "retention")
    assert retention["Expiration"]["Days"] == 90
    encryption = s3.get_bucket_encryption(Bucket=settings.export_bucket)
    assert encryption["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "aws:kms"
    assert s3.get_bucket_versioning(Bucket=settings.export_bucket)["Status"] == "Enabled"

    sqs = aws_client(settings, "sqs")
    url = sqs.get_queue_url(QueueName=settings.load_queue_name)["QueueUrl"]
    assert "RedrivePolicy" in sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["RedrivePolicy"])["Attributes"]

    alarms = {a["AlarmName"] for a in aws_client(settings, "cloudwatch").describe_alarms()["MetricAlarms"]}
    assert {f"{settings.name_prefix}-{n}" for n in ("run-failed", "run-partial", "run-overrunning", "dlq-depth",
                                                     "reconciliation-variance", "ambiguous-batches")} <= alarms

    secret = aws_client(settings, "secretsmanager").get_secret_value(SecretId=settings.pim_api_key_secret_id)
    assert secret["SecretString"]


def test_first_run_completes_and_reconciles(first_run):
    assert first_run["status"] == "COMPLETE"
    assert first_run["pim_total"] == first_run["rows_extracted"] == first_run["rows_changed"] == TOTAL
    assert first_run["rows_accepted"] == TOTAL - 12
    assert first_run["rows_rejected"] == first_run["rows_rejected_expected"] == 12
    assert first_run["rows_dlq"] == first_run["rows_ambiguous"] == first_run["rows_outstanding"] == 0
    assert set(first_run["reconciliation"].values()) <= {0, None}
    assert first_run["batches_by_status"] == {"DONE": TOTAL // 100}


def test_original_export_is_retained_in_s3(first_run, settings):
    s3 = aws_client(settings, "s3")
    key = f"exports/{first_run['run_id']}/catalogue.csv"
    body = s3.get_object(Bucket=settings.export_bucket, Key=key)["Body"].read().decode()
    rows = list(csv.DictReader(io.StringIO(body)))
    assert len(rows) == TOTAL
    assert list(rows[0]) == ["id", "name", "category", "price", "currency", "updated_at"]


def test_rejections_are_recorded_and_classified(first_run, deps):
    rejects = list(deps.ledger.items(first_run["run_id"], "REJECT#"))
    assert len(rejects) == 12
    assert all(r["classification"] == "WMS_RULE_SKU_999" and r["expected"] for r in rejects)


def test_run_status_is_visible_to_operations(first_run, deps):
    run = deps.ledger.get_run(first_run["run_id"])
    assert run["status"] == "COMPLETE" and run["finished_at"] and run["duration_seconds"] > 0
    assert any(r["sk"] == first_run["run_id"] for r in deps.ledger.recent_runs())
    assert deps.ledger.lock_holder() is None


def test_second_run_sends_nothing_unchanged(first_run, deps):
    second = run_pipeline(deps)
    assert second["status"] == "NO_CHANGE"
    assert second["rows_unchanged"] == TOTAL
    assert second["batches_total"] == 0  # no WMS call was made


def test_overlapping_run_is_refused(first_run, deps):
    deps.ledger.acquire_lock("e2e-holder", 60)
    try:
        with pytest.raises(RunInProgress):
            start_run(deps)
    finally:
        deps.ledger.release_lock("e2e-holder")


def test_state_machine_runs_the_same_pipeline_as_lambdas(first_run, settings):
    """Only when terraform/envs/local was applied with enable_compute = true."""
    from app.main import start_execution

    sfn = aws_client(settings, "stepfunctions")
    if not any(m["name"] == f"{settings.name_prefix}-run" for m in sfn.list_state_machines()["stateMachines"]):
        pytest.skip("state machine not deployed (apply terraform/envs/local with -var enable_compute=true)")
    result = start_execution(settings, timeout_seconds=540)
    assert result["execution_status"] == "SUCCEEDED"
    assert result["status"] == "NO_CHANGE"  # first_run already committed every hash
    assert set(result["reconciliation"].values()) <= {0, None}

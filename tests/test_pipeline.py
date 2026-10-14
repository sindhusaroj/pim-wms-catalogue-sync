"""Whole-pipeline tests: moto for AWS, the supplied mock PIM and WMS in-process.

The mocks are used unchanged; they inject 429s and 503s on a fixed cadence and
reject SKUs ending in 999. Faults the mocks cannot produce (persistent 5xx,
timeouts) are injected by a forwarding client in front of them.
"""
import csv
import io
import json
from collections import Counter

import boto3
import httpx
import pytest

from app.local_orchestrator import run_diff, run_extract, run_pipeline, run_replay
from app.pipeline.assemble import assemble_export
from app.pipeline.load import handle_sqs_event
from app.pipeline.start_run import RunInProgress, start_run
from app.state.ledger import AMBIGUOUS, DLQ, DONE
from tests.conftest import REGION, Forwarder

TOTAL = 12_000
REJECTED_999 = TOTAL // 1000  # P0000999, P0001999, ... P0011999


def s3_text(bucket: str, key: str) -> str:
    return boto3.client("s3", region_name=REGION).get_object(Bucket=bucket, Key=key)["Body"].read().decode()


def dlq_bodies(deps) -> list[dict]:
    response = deps.sqs.receive_message(QueueUrl=deps.dlq_url, MaxNumberOfMessages=10, VisibilityTimeout=0)
    return [json.loads(m["Body"]) for m in response.get("Messages", [])]


def test_full_run_survives_injected_faults_and_records_every_outcome(make_deps, mock_wms, settings):
    wms = Forwarder(mock_wms.app)
    deps = make_deps(wms_http=wms.client)

    summary = run_pipeline(deps)

    assert summary["status"] == "COMPLETE"
    assert summary["pim_total"] == summary["rows_extracted"] == summary["rows_changed"] == TOTAL
    assert summary["rows_accepted"] == TOTAL - REJECTED_999
    assert summary["rows_rejected"] == summary["rows_rejected_expected"] == REJECTED_999
    assert summary["rows_rejected_unexpected"] == summary["rows_dlq"] == summary["rows_ambiguous"] == 0
    assert all(v in (0, None) for v in summary["reconciliation"].values())
    assert summary["batches_total"] == TOTAL // 100

    # The mock injected throttling and server errors; they were retried, not fatal.
    statuses = Counter()
    for r in wms.requests:
        statuses[r.method] += 1
    assert statuses["POST"] > TOTAL // 100

    # Never sent twice within a run: every SKU reached the WMS exactly once.
    assert len(wms.delivered) == len(set(wms.delivered)) == TOTAL

    # The original export is retained in S3 as CSV.
    rows = list(csv.DictReader(io.StringIO(s3_text(settings.export_bucket, f"exports/{summary['run_id']}/catalogue.csv"))))
    assert len(rows) == TOTAL and rows[0]["id"] == "P0000001"

    # Rejections are recorded with their classification, not discarded.
    rejects = list(deps.ledger.items(summary["run_id"], "REJECT#"))
    assert sorted(r["sku"] for r in rejects) == [f"P{i:04d}999" for i in range(REJECTED_999)]
    assert {r["classification"] for r in rejects} == {"WMS_RULE_SKU_999"}

    run = deps.ledger.get_run(summary["run_id"])
    assert run["status"] == "COMPLETE" and run["finished_at"]
    assert deps.ledger.lock_holder() is None


def test_second_run_sends_nothing_when_nothing_changed(make_deps, mock_wms):
    run_pipeline(make_deps())
    wms = Forwarder(mock_wms.app)

    summary = run_pipeline(make_deps(wms_http=wms.client))

    assert summary["status"] == "NO_CHANGE"
    assert summary["rows_unchanged"] == TOTAL and summary["rows_changed"] == 0
    assert [r for r in wms.requests if r.method == "POST"] == []


def test_only_changed_products_are_sent_on_a_later_run(make_deps, mock_pim, mock_wms):
    run_pipeline(make_deps())
    original = mock_pim.product_for

    def repriced(i):
        product = original(i)
        if i % 500 == 0:
            product["price"] = round(product["price"] + 1, 2)
        return product

    mock_pim.product_for = repriced
    wms = Forwarder(mock_wms.app)
    summary = run_pipeline(make_deps(wms_http=wms.client))

    assert summary["status"] == "COMPLETE"
    assert summary["rows_changed"] == TOTAL // 500
    assert sorted(wms.delivered) == [f"P{i:07d}" for i in range(500, TOTAL + 1, 500)]


def test_invalid_records_are_quarantined_without_aborting_the_run(make_deps, mock_pim, settings):
    original = mock_pim.product_for

    def corrupt(i):
        product = original(i)
        if i in (7, 4242):
            product["price"] = None
        return product

    mock_pim.product_for = corrupt
    summary = run_pipeline(make_deps())

    assert summary["status"] == "COMPLETE"
    assert summary["rows_invalid"] == 2 and summary["rows_changed"] == TOTAL - 2
    invalid = list(make_deps().ledger.items(summary["run_id"], "INVALID#"))
    assert len(invalid) == 2 and all("price" in i["reason"] for i in invalid)


def test_overlapping_run_is_refused(make_deps):
    deps = make_deps()
    deps.ledger.acquire_lock("someone-else", 3600)
    with pytest.raises(RunInProgress):
        start_run(deps)


def test_interrupted_run_resumes_without_resending_delivered_batches(make_deps, mock_wms):
    wms = Forwarder(mock_wms.app)
    deps = make_deps(wms_http=wms.client)

    # Run the stages by hand and "crash" half way through the load phase.
    run_id = start_run(deps)["run_id"]
    run_extract(deps, run_id, list(range(1, TOTAL // 500 + 1)))
    run_diff(deps, run_id, assemble_export(deps, run_id)["chunks"])
    client = deps.wms_client(100)
    processed = 0
    while processed < 50:
        messages = deps.sqs.receive_message(QueueUrl=deps.queue_url(), MaxNumberOfMessages=10,
                                            AttributeNames=["ApproximateReceiveCount"])["Messages"]
        event = {"Records": [{"messageId": m["MessageId"], "body": m["Body"],
                              "attributes": {"ApproximateReceiveCount": "1"}} for m in messages]}
        assert handle_sqs_event(deps, client, event) == {"batchItemFailures": []}
        for m in messages:
            deps.sqs.delete_message(QueueUrl=deps.queue_url(), ReceiptHandle=m["ReceiptHandle"])
        processed += len(messages)
    delivered_before_crash = len(wms.delivered)
    assert 0 < delivered_before_crash < TOTAL

    summary = run_pipeline(deps, run_id, resume=True)

    assert summary["status"] == "COMPLETE"
    assert summary["rows_accepted"] == TOTAL - REJECTED_999
    assert len(wms.delivered) == len(set(wms.delivered)) == TOTAL  # nothing delivered twice


def test_persistent_wms_failure_dead_letters_only_that_batch_and_replay_recovers_it(make_deps, mock_wms):
    healthy = {"value": False}

    def fail_batch_with_p500(request):
        if request.method == "POST" and b'"P0000500"' in request.content and not healthy["value"]:
            return httpx.Response(503, json={"detail": "down"})
        return None

    wms = Forwarder(mock_wms.app, intercept=fail_batch_with_p500)
    deps = make_deps(wms_http=wms.client)

    summary = run_pipeline(deps)

    assert summary["status"] == "PARTIAL"
    assert summary["rows_dlq"] == 100 and summary["rows_accepted"] == TOTAL - REJECTED_999 - 100
    assert summary["reconciliation"]["load_variance"] == 0
    [failed] = [b for b in deps.ledger.items(summary["run_id"], "BATCH#") if b["status"] == DLQ]
    assert failed["attempts"] == deps.settings.max_delivery_attempts
    [dead] = dlq_bodies(deps)
    assert dead["batch_id"] == failed["batch_id"] and "after 5 delivery attempts" in dead["dlq_reason"]

    healthy["value"] = True
    delivered_before = list(wms.delivered)
    replayed = run_replay(deps, summary["run_id"])

    assert replayed["replay"]["replayed"] == 1
    assert replayed["status"] == "COMPLETE"
    newly_delivered = wms.delivered[len(delivered_before):]
    assert sorted(newly_delivered) == sorted(failed["skus"])  # only the failed batch was resent
    assert len(wms.delivered) == len(set(wms.delivered)) == TOTAL
    sent_key = {r.headers["Idempotency-Key"] for r in wms.requests if b'"P0000500"' in r.content}
    assert sent_key == {failed["idempotency_key"]}  # replay reused the original key


def test_ambiguous_write_is_parked_not_blindly_resent(make_deps, mock_wms):
    def time_out_batch_with_p700(request):
        if request.method == "POST" and b'"P0000700"' in request.content:
            raise httpx.ReadTimeout("no response from WMS")
        return None

    wms = Forwarder(mock_wms.app, intercept=time_out_batch_with_p700)
    deps = make_deps(wms_http=wms.client)

    summary = run_pipeline(deps)

    assert summary["status"] == "PARTIAL"
    assert summary["rows_ambiguous"] == 100
    attempts = [r for r in wms.requests if b'"P0000700"' in r.content]
    assert len(attempts) == 1  # never retried: the WMS might have applied it
    [batch] = [b for b in deps.ledger.items(summary["run_id"], "BATCH#") if b["status"] == AMBIGUOUS]
    assert "P0000700" in batch["skus"]
    assert "P0000700" not in deps.ledger.get_product_states(["P0000700"])  # hash not committed
    [dead] = dlq_bodies(deps)
    assert dead["dlq_reason"].startswith("AMBIGUOUS")

    # Replaying without the operator's explicit decision leaves it parked.
    assert run_replay(deps, summary["run_id"])["replay"]["replayed"] == 0


def test_redelivered_message_for_a_done_batch_does_not_call_the_wms(make_deps, mock_wms):
    wms = Forwarder(mock_wms.app)
    deps = make_deps(wms_http=wms.client)
    summary = run_pipeline(deps)
    batch = next(b for b in deps.ledger.items(summary["run_id"], "BATCH#") if b["status"] == DONE)
    posts_before = sum(r.method == "POST" for r in wms.requests)
    body = {"run_id": summary["run_id"], "batch_id": batch["batch_id"], "idempotency_key": batch["idempotency_key"],
            "products": [{"sku": s} for s in batch["skus"]], "hashes": {}}
    event = {"Records": [{"messageId": "dup", "body": json.dumps(body), "attributes": {"ApproximateReceiveCount": "2"}}]}

    assert handle_sqs_event(deps, deps.wms_client(100), event) == {"batchItemFailures": []}
    assert sum(r.method == "POST" for r in wms.requests) == posts_before


def test_persistently_failing_pim_page_makes_the_run_partial_not_silent(make_deps, mock_pim):
    def fail_page_3(request):
        if request.url.params.get("page") == "3" and request.url.params.get("page_size") == "500":
            return httpx.Response(503, json={"detail": "down"})
        return None

    pim = Forwarder(mock_pim.app, intercept=fail_page_3)
    summary = run_pipeline(make_deps(pim_http=pim.client))

    assert summary["status"] == "PARTIAL"
    assert summary["pages_failed"] == [3]
    assert summary["rows_extracted"] == TOTAL - 500
    assert summary["rows_accepted"] + summary["rows_rejected"] == TOTAL - 500

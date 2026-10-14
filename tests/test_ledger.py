"""State model against moto DynamoDB: run lock, run ledger, batch state transitions."""
import pytest

from app.pipeline.deps import aws_client
from app.state.ledger import AMBIGUOUS, DONE, INFLIGHT, PENDING, RETRY, RUNNING, Ledger


@pytest.fixture
def ledger(aws, settings):
    return Ledger(aws_client(settings, "dynamodb"), settings.runs_table, settings.product_state_table,
                  settings.run_lock_table)


def test_run_lock_prevents_overlapping_runs(ledger):
    assert ledger.acquire_lock("run-a", 3600)
    assert not ledger.acquire_lock("run-b", 3600)
    assert ledger.acquire_lock("run-a", 3600)  # the holder can re-acquire (resume)
    ledger.release_lock("run-b")  # not the holder: no effect
    assert ledger.lock_holder()["run_id"] == "run-a"
    ledger.release_lock("run-a")
    assert ledger.acquire_lock("run-b", 3600)


def test_expired_lock_can_be_taken_over(ledger):
    assert ledger.acquire_lock("crashed-run", -1)
    assert ledger.acquire_lock("next-run", 3600)


def test_run_ledger_lifecycle_and_listing(ledger):
    ledger.create_run("20261014T060000Z-aaaa", export_s3_key="exports/x/catalogue.csv")
    ledger.create_run("20261015T060000Z-bbbb")
    ledger.update_run("20261014T060000Z-aaaa", status="COMPLETE", rows_accepted=5)
    run = ledger.get_run("20261014T060000Z-aaaa")
    assert run["status"] == "COMPLETE" and run["rows_accepted"] == 5
    recent = ledger.recent_runs()
    assert [r["sk"] for r in recent] == ["20261015T060000Z-bbbb", "20261014T060000Z-aaaa"]
    assert recent[0]["status"] == RUNNING


def test_batch_creation_is_conditional_and_transitions_are_guarded(ledger):
    assert ledger.create_batch("r", "00000-000", idempotency_key="k", skus=["P1", "P2"])
    assert not ledger.create_batch("r", "00000-000", idempotency_key="k", skus=["P1", "P2"])
    assert ledger.transition_batch("r", "00000-000", INFLIGHT, [PENDING, RETRY], increment_attempts=True)
    assert not ledger.transition_batch("r", "00000-000", INFLIGHT, [PENDING, RETRY])  # already claimed
    assert ledger.transition_batch("r", "00000-000", DONE, [INFLIGHT], accepted_count=2)
    assert not ledger.transition_batch("r", "00000-000", AMBIGUOUS, [INFLIGHT])
    batch = ledger.get_batch("r", "00000-000")
    assert batch["status"] == DONE and batch["attempts"] == 1 and batch["accepted_count"] == 2


def test_product_state_batch_get_and_commit_over_100_keys(ledger):
    records = [{"sku": f"P{i:05d}", "content_hash": f"h{i}", "last_synced_run": "r", "last_status": "ACCEPTED"}
               for i in range(230)]
    ledger.commit_product_states(records)
    found = ledger.get_product_states([r["sku"] for r in records] + ["UNKNOWN"])
    assert len(found) == 230 and found["P00007"]["content_hash"] == "h7"

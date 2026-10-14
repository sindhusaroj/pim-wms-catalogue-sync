"""DynamoDB state: run ledger, page/batch state, rejections, product hashes, run lock.

Implements the state model in figure 7. Counts are derived from per-page,
per-chunk and per-batch items rather than from shared counters, so a retried
Lambda invocation that rewrites its own item cannot double count.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Iterator

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from app.domain.batching import chunked

_ser = TypeSerializer()
_deser = TypeDeserializer()

LOCK_KEY = "LOCK#catalogue-sync"

# Run status
RUNNING, COMPLETE, PARTIAL, FAILED, NO_CHANGE = "RUNNING", "COMPLETE", "PARTIAL", "FAILED", "NO_CHANGE"
# Batch status
PENDING, INFLIGHT, RETRY, DONE, DLQ, AMBIGUOUS = "PENDING", "INFLIGHT", "RETRY", "DONE", "DLQ", "AMBIGUOUS"
BATCH_TERMINAL = {DONE, DLQ, AMBIGUOUS}
# Page status
PAGE_OK, PAGE_DLQ = "OK", "DLQ"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _to_ddb(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _to_ddb(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_ddb(v) for v in value]
    return value


def _from_ddb(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: _from_ddb(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_ddb(v) for v in value]
    return value


def serialize(item: dict) -> dict:
    return {k: _ser.serialize(_to_ddb(v)) for k, v in item.items() if v is not None}


def deserialize(item: dict) -> dict:
    return _from_ddb({k: _deser.deserialize(v) for k, v in item.items()})


def _is_conditional_failure(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def run_pk(run_id: str) -> str:
    return f"RUN#{run_id}"


def page_sk(page: int) -> str:
    return f"PAGE#{page:05d}"


def batch_sk(batch_id: str) -> str:
    return f"BATCH#{batch_id}"


class Ledger:
    def __init__(self, client, runs_table: str, product_state_table: str, run_lock_table: str) -> None:
        self.client = client
        self.runs_table = runs_table
        self.product_state_table = product_state_table
        self.run_lock_table = run_lock_table

    # ---------------------------------------------------------------- run lock
    def acquire_lock(self, run_id: str, ttl_seconds: int) -> bool:
        """Conditional put: no overlapping runs (figure 10, AcquireRunLock)."""
        now = int(time.time())
        try:
            self.client.put_item(
                TableName=self.run_lock_table,
                Item=serialize({"pk": LOCK_KEY, "run_id": run_id, "acquired_at": now_iso(), "expires_at": now + ttl_seconds}),
                ConditionExpression="attribute_not_exists(pk) OR expires_at < :now OR run_id = :run",
                ExpressionAttributeValues=serialize({":now": now, ":run": run_id}),
            )
            return True
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise

    def lock_holder(self) -> dict | None:
        item = self.client.get_item(TableName=self.run_lock_table, Key=serialize({"pk": LOCK_KEY}), ConsistentRead=True).get("Item")
        return deserialize(item) if item else None

    def release_lock(self, run_id: str) -> None:
        try:
            self.client.delete_item(
                TableName=self.run_lock_table,
                Key=serialize({"pk": LOCK_KEY}),
                ConditionExpression="run_id = :run",
                ExpressionAttributeValues=serialize({":run": run_id}),
            )
        except ClientError as exc:
            if not _is_conditional_failure(exc):
                raise

    # ---------------------------------------------------------------- run META
    def create_run(self, run_id: str, **attrs: Any) -> None:
        started = now_iso()
        item = {"pk": run_pk(run_id), "sk": "META", "run_id": run_id, "status": RUNNING, "started_at": started, **attrs}
        self.client.put_item(
            TableName=self.runs_table, Item=serialize(item), ConditionExpression="attribute_not_exists(pk)"
        )
        self._index_run(run_id, RUNNING, started)

    def _index_run(self, run_id: str, status: str, started_at: str | None = None) -> None:
        # A single "RUNS" partition sorted by run_id (time-prefixed) lists recent runs.
        values = {":s": status}
        expr = "SET #st = :s"
        if started_at:
            expr += ", started_at = :t"
            values[":t"] = started_at
        self.client.update_item(
            TableName=self.runs_table,
            Key=serialize({"pk": "RUNS", "sk": run_id}),
            UpdateExpression=expr,
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=serialize(values),
        )

    def update_run(self, run_id: str, **attrs: Any) -> None:
        if not attrs:
            return
        names = {f"#a{i}": k for i, k in enumerate(attrs)}
        values = {f":v{i}": v for i, v in enumerate(attrs.values())}
        expr = "SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(attrs)))
        self.client.update_item(
            TableName=self.runs_table,
            Key=serialize({"pk": run_pk(run_id), "sk": "META"}),
            UpdateExpression=expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=serialize(values),
        )
        if "status" in attrs:
            self._index_run(run_id, attrs["status"])

    def get_run(self, run_id: str) -> dict | None:
        item = self.client.get_item(
            TableName=self.runs_table, Key=serialize({"pk": run_pk(run_id), "sk": "META"}), ConsistentRead=True
        ).get("Item")
        return deserialize(item) if item else None

    def recent_runs(self, limit: int = 10) -> list[dict]:
        response = self.client.query(
            TableName=self.runs_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues=serialize({":pk": "RUNS"}),
            ScanIndexForward=False,
            Limit=limit,
        )
        return [deserialize(i) for i in response.get("Items", [])]

    # ---------------------------------------------------------------- generic query
    def items(self, run_id: str, prefix: str) -> Iterator[dict]:
        kwargs = {
            "TableName": self.runs_table,
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :p)",
            "ExpressionAttributeValues": serialize({":pk": run_pk(run_id), ":p": prefix}),
            "ConsistentRead": True,
        }
        while True:
            response = self.client.query(**kwargs)
            for item in response.get("Items", []):
                yield deserialize(item)
            if "LastEvaluatedKey" not in response:
                return
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    def get_item(self, run_id: str, sk: str) -> dict | None:
        item = self.client.get_item(
            TableName=self.runs_table, Key=serialize({"pk": run_pk(run_id), "sk": sk}), ConsistentRead=True
        ).get("Item")
        return deserialize(item) if item else None

    # ---------------------------------------------------------------- pages
    def put_page(self, run_id: str, page: int, status: str, *, rows: int = 0, s3_key: str | None = None,
                 attempts: int = 0, last_error: str | None = None, ttl_days: int = 30) -> None:
        self.client.put_item(
            TableName=self.runs_table,
            Item=serialize({
                "pk": run_pk(run_id), "sk": page_sk(page), "page": page, "status": status, "rows": rows,
                "s3_key": s3_key, "attempts": attempts, "last_error": (last_error or "")[:500] or None,
                "updated_at": now_iso(), "ttl": int(time.time()) + ttl_days * 86400,
            }),
        )

    # ---------------------------------------------------------------- chunks (diff results)
    def put_chunk(self, run_id: str, index: int, counts: dict[str, int]) -> None:
        self.client.put_item(
            TableName=self.runs_table,
            Item=serialize({"pk": run_pk(run_id), "sk": f"CHUNK#{index:05d}", "chunk": index, **counts, "updated_at": now_iso()}),
        )

    def put_invalid(self, run_id: str, ref: str, reason: str) -> None:
        self.client.put_item(
            TableName=self.runs_table,
            Item=serialize({"pk": run_pk(run_id), "sk": f"INVALID#{ref}", "reason": reason[:1000], "created_at": now_iso()}),
        )

    # ---------------------------------------------------------------- batches
    def create_batch(self, run_id: str, batch_id: str, *, idempotency_key: str, skus: list[str]) -> bool:
        """Returns False if the batch already exists (a retried diff invocation)."""
        try:
            self.client.put_item(
                TableName=self.runs_table,
                Item=serialize({
                    "pk": run_pk(run_id), "sk": batch_sk(batch_id), "batch_id": batch_id, "status": PENDING,
                    "idempotency_key": idempotency_key, "batch_size": len(skus), "skus": skus, "attempts": 0,
                    "enqueued": False, "created_at": now_iso(),
                }),
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise

    def get_batch(self, run_id: str, batch_id: str) -> dict | None:
        item = self.client.get_item(
            TableName=self.runs_table, Key=serialize({"pk": run_pk(run_id), "sk": batch_sk(batch_id)}), ConsistentRead=True
        ).get("Item")
        return deserialize(item) if item else None

    def mark_enqueued(self, run_id: str, batch_ids: Iterable[str]) -> None:
        for batch_id in batch_ids:
            self.client.update_item(
                TableName=self.runs_table,
                Key=serialize({"pk": run_pk(run_id), "sk": batch_sk(batch_id)}),
                UpdateExpression="SET enqueued = :t",
                ExpressionAttributeValues=serialize({":t": True}),
            )

    def transition_batch(self, run_id: str, batch_id: str, to_status: str, allowed_from: Iterable[str],
                         *, increment_attempts: bool = False, **attrs: Any) -> bool:
        """Conditional state transition; False if the batch is not in an allowed state."""
        allowed = list(allowed_from)
        names = {"#st": "status"}
        values: dict[str, Any] = {":to": to_status, ":now": now_iso()}
        sets = ["#st = :to", "updated_at = :now"]
        for i, (k, v) in enumerate(attrs.items()):
            names[f"#a{i}"] = k
            values[f":v{i}"] = v
            sets.append(f"#a{i} = :v{i}")
        expr = "SET " + ", ".join(sets)
        if increment_attempts:
            expr += " ADD attempts :one"
            values[":one"] = 1
        placeholders = []
        for i, status in enumerate(allowed):
            values[f":f{i}"] = status
            placeholders.append(f":f{i}")
        try:
            self.client.update_item(
                TableName=self.runs_table,
                Key=serialize({"pk": run_pk(run_id), "sk": batch_sk(batch_id)}),
                UpdateExpression=expr,
                ConditionExpression=f"#st IN ({', '.join(placeholders)})",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=serialize(values),
            )
            return True
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise

    # ---------------------------------------------------------------- rejections
    def put_rejection(self, run_id: str, sku: str | None, *, reason: str, classification: str, expected: bool,
                      batch_id: str) -> None:
        ref = sku if sku else f"NONE#{batch_id}#{time.time_ns()}"
        self.client.put_item(
            TableName=self.runs_table,
            Item=serialize({
                "pk": run_pk(run_id), "sk": f"REJECT#SKU#{ref}", "sku": sku, "reason": (reason or "")[:500],
                "classification": classification, "expected": expected, "batch_id": batch_id,
                "replayed": False, "created_at": now_iso(),
            }),
        )

    # ---------------------------------------------------------------- product state
    def get_product_states(self, skus: list[str]) -> dict[str, dict]:
        """BatchGetItem, 100 keys per request, with unprocessed-key retry."""
        found: dict[str, dict] = {}
        for group in chunked(list(dict.fromkeys(skus)), 100):
            request = {self.product_state_table: {"Keys": [serialize({"pk": f"SKU#{s}", "sk": "STATE"}) for s in group]}}
            delay = 0.05
            while request:
                response = self.client.batch_get_item(RequestItems=request)
                for item in response.get("Responses", {}).get(self.product_state_table, []):
                    record = deserialize(item)
                    found[record["pk"][4:]] = record
                request = response.get("UnprocessedKeys") or {}
                if request:
                    time.sleep(delay)
                    delay = min(delay * 2, 2.0)
        return found

    def commit_product_states(self, records: list[dict]) -> None:
        """Write the committed content hash for SKUs with a definitive WMS outcome."""
        for group in chunked(records, 25):
            request = {self.product_state_table: [
                {"PutRequest": {"Item": serialize({"pk": f"SKU#{r['sku']}", "sk": "STATE", **r, "updated_at": now_iso()})}}
                for r in group
            ]}
            delay = 0.05
            while request:
                response = self.client.batch_write_item(RequestItems=request)
                request = response.get("UnprocessedItems") or {}
                if request:
                    time.sleep(delay)
                    delay = min(delay * 2, 2.0)

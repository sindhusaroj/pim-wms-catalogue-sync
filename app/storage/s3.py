"""S3 storage for the raw, export and curated zones (finding F6).

Replaces the local-filesystem stub. The canonical CSV export is streamed with a
multipart upload, so peak memory is one part regardless of catalogue size
(section 6.4). Retention is a bucket lifecycle rule managed by Terraform.
"""
from __future__ import annotations

import csv
import io
import json
from typing import Iterator

CSV_FIELDS = ["id", "name", "category", "price", "currency", "updated_at"]
MIN_PART_BYTES = 8 * 1024 * 1024  # S3 minimum is 5 MiB for all parts but the last


def raw_shard_key(run_id: str, page: int) -> str:
    return f"raw/{run_id}/page_{page:05d}.json"


def export_key(run_id: str) -> str:
    return f"exports/{run_id}/catalogue.csv"


def chunk_key(run_id: str, index: int) -> str:
    return f"work/{run_id}/chunk_{index:05d}.jsonl"


def quarantine_key(run_id: str, chunk: int) -> str:
    return f"quarantine/{run_id}/chunk_{chunk:05d}.jsonl"


class S3Store:
    def __init__(self, client, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def put_json(self, key: str, payload) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
        )

    def get_json(self, key: str):
        return json.loads(self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read())

    def put_lines(self, key: str, records: list[dict]) -> None:
        body = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/x-ndjson")

    def iter_lines(self, key: str) -> Iterator[dict]:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        for line in body.iter_lines():
            if line:
                yield json.loads(line)

    def open_multipart(self, key: str, content_type: str) -> "MultipartWriter":
        return MultipartWriter(self.client, self.bucket, key, content_type)


class MultipartWriter:
    """Buffered multipart upload; falls back to a single PUT for small objects."""

    def __init__(self, client, bucket: str, key: str, content_type: str, part_bytes: int = MIN_PART_BYTES) -> None:
        self.client, self.bucket, self.key, self.content_type = client, bucket, key, content_type
        self.part_bytes = part_bytes
        self._buffer = bytearray()
        self._upload_id: str | None = None
        self._parts: list[dict] = []
        self.bytes_written = 0

    def write(self, data: bytes) -> None:
        self._buffer.extend(data)
        self.bytes_written += len(data)
        if len(self._buffer) >= self.part_bytes:
            self._flush_part()

    def _flush_part(self) -> None:
        if self._upload_id is None:
            self._upload_id = self.client.create_multipart_upload(
                Bucket=self.bucket, Key=self.key, ContentType=self.content_type
            )["UploadId"]
        number = len(self._parts) + 1
        etag = self.client.upload_part(
            Bucket=self.bucket, Key=self.key, UploadId=self._upload_id, PartNumber=number, Body=bytes(self._buffer)
        )["ETag"]
        self._parts.append({"ETag": etag, "PartNumber": number})
        self._buffer.clear()

    def close(self) -> None:
        if self._upload_id is None:
            self.client.put_object(Bucket=self.bucket, Key=self.key, Body=bytes(self._buffer), ContentType=self.content_type)
            return
        if self._buffer:
            self._flush_part()
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self._upload_id, MultipartUpload={"Parts": self._parts}
        )

    def abort(self) -> None:
        if self._upload_id is not None:
            self.client.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self._upload_id)

    def __enter__(self) -> "MultipartWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()


def csv_bytes(rows: list[dict], header: bool) -> bytes:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_FIELDS, extrasaction="ignore", lineterminator="\n")
    if header:
        writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode("utf-8")

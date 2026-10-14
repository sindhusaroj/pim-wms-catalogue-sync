"""Phase 3a - AssembleExport (figure 10).

Streams the page shards, in page order, into the canonical CSV export in S3.
The CSV is the retained artefact (90-day lifecycle) and is written unmodified.
The same pass deduplicates on product id (reading I1) and cuts the work into
fixed curated chunks that ValidateAndDiff processes in parallel.
"""
from __future__ import annotations

import logging

from app.observability import emit_metrics, get_logger, log
from app.pipeline.deps import Deps
from app.state.ledger import PAGE_DLQ, PAGE_OK
from app.storage.s3 import chunk_key, csv_bytes, export_key

logger = get_logger(__name__)


def assemble_export(deps: Deps, run_id: str) -> dict:
    meta = deps.ledger.get_run(run_id) or {}
    if meta.get("chunks_total") is not None:
        # Checkpoint: the export was assembled by an earlier attempt of this run.
        return {"chunks": list(range(int(meta["chunks_total"])))}

    pages = sorted(deps.ledger.items(run_id, "PAGE#"), key=lambda p: p["page"])
    ok_pages = [p for p in pages if p["status"] == PAGE_OK]
    failed_pages = [p["page"] for p in pages if p["status"] == PAGE_DLQ]
    size = deps.settings.diff_chunk_size

    seen: set[str] = set()
    rows = duplicates = 0
    pending: list[dict] = []
    chunks = 0

    def flush(records: list[dict]) -> None:
        nonlocal chunks
        deps.curated.put_lines(chunk_key(run_id, chunks), records)
        chunks += 1

    key = export_key(run_id)
    with deps.exports.open_multipart(key, "text/csv") as writer:
        writer.write(csv_bytes([], header=True))
        for page in ok_pages:
            products = deps.raw.get_json(page["s3_key"])["products"]
            writer.write(csv_bytes(products, header=False))
            rows += len(products)
            for product in products:
                pid = product.get("id")
                if pid:
                    if pid in seen:
                        duplicates += 1
                        continue
                    seen.add(pid)
                pending.append(product)
            while len(pending) >= size:
                flush(pending[:size])
                pending = pending[size:]
        if pending:
            flush(pending)

    deps.ledger.update_run(
        run_id, export_s3_key=key, rows_exported=rows, rows_duplicate=duplicates, chunks_total=chunks,
        export_complete=not failed_pages, pages_failed=len(failed_pages), export_bytes=writer.bytes_written,
    )
    log(logger, logging.INFO, "export assembled", run_id=run_id, key=key, rows=rows, duplicates=duplicates,
        chunks=chunks, pages_failed=failed_pages)
    emit_metrics({"ProductsExported": rows, "DuplicateProducts": duplicates})
    return {"chunks": list(range(chunks))}

"""Structured JSON logs and CloudWatch Embedded Metric Format metrics (section 7.7).

Metrics are emitted from code rather than parsed out of log text. In Lambda,
CloudWatch extracts EMF documents from stdout automatically; locally they are
written to the same stream and can be ignored or grepped.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

NAMESPACE = "CatalogueSync"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in getattr(record, "ctx", {}).items():
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_configured = False


def configure_logging(level: str | None = None) -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    for noisy in ("botocore", "boto3", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log(logger: logging.Logger, level: int, message: str, **ctx: Any) -> None:
    logger.log(level, message, extra={"ctx": ctx})


def emit_metrics(metrics: dict[str, float], *, unit: str = "Count", dimensions: dict[str, str] | None = None,
                 properties: dict[str, Any] | None = None) -> None:
    """Write one EMF document. Disabled with METRICS_ENABLED=false (e.g. in unit tests)."""
    if os.getenv("METRICS_ENABLED", "true").lower() in ("0", "false", "no"):
        return
    dims = dimensions or {"Pipeline": "catalogue-sync"}
    document = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": NAMESPACE,
                "Dimensions": [list(dims.keys())],
                "Metrics": [{"Name": name, "Unit": unit if not name.endswith("Seconds") else "Seconds"} for name in metrics],
            }],
        },
        **dims,
        **{name: value for name, value in metrics.items()},
        **(properties or {}),
    }
    sys.stdout.write(json.dumps(document, default=str) + "\n")
    sys.stdout.flush()

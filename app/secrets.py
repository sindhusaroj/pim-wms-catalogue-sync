"""API keys from AWS Secrets Manager (section 7.6, finding F7).

Keys are cached in-process for the life of a Lambda container and refreshed
once when a vendor answers 401/403, so a rotation mid-run is survivable.
"""
from __future__ import annotations

import threading
from typing import Protocol


class SecretProvider(Protocol):
    def get(self) -> str: ...

    def refresh(self) -> str: ...


class SecretsManagerSecret:
    def __init__(self, client, secret_id: str) -> None:
        self._client = client
        self._secret_id = secret_id
        self._value: str | None = None
        self._lock = threading.Lock()

    def get(self) -> str:
        with self._lock:
            if self._value is None:
                self._value = self._fetch()
            return self._value

    def refresh(self) -> str:
        with self._lock:
            self._value = self._fetch()
            return self._value

    def _fetch(self) -> str:
        response = self._client.get_secret_value(SecretId=self._secret_id)
        return response["SecretString"]


class StaticSecret:
    """For tests only."""

    def __init__(self, value: str) -> None:
        self.value = value
        self.refreshes = 0

    def get(self) -> str:
        return self.value

    def refresh(self) -> str:
        self.refreshes += 1
        return self.value

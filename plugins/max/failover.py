"""Durable MAX transport state and opt-in failover without a gateway restart."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping

from .polling_state import max_update_event_key


class WebhookPollingStore:
    """Polling checkpoints and Webhook events share one durable inbox and dedup key."""

    def __init__(self, path: str | Path):
        self._conn = sqlite3.connect(str(path), timeout=5)
        self._conn.execute("CREATE TABLE IF NOT EXISTS max_transport_state (name TEXT PRIMARY KEY, value TEXT)")
        self._conn.commit()

    def state(self, name: str) -> str | None:
        row = self._conn.execute("SELECT value FROM max_transport_state WHERE name=?", (name,)).fetchone()
        return row[0] if row else None

    def latch(self) -> None:
        with self._conn:
            self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('fallback', 'polling')")

    def begin_return(self) -> None:
        with self._conn:
            self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('returning','1')")

    def finish_return(self) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM max_transport_state WHERE name IN ('fallback','returning')")

    def cancel_return(self) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM max_transport_state WHERE name='returning'")

    def get(self) -> int | None:
        value = self.state("marker")
        return int(value) if value is not None else None

    def accept_batch(self, updates: list[Mapping[str, Any]], marker: int | None):
        with self._conn:
            for update in updates:
                if not isinstance(update, Mapping):
                    continue
                self._conn.execute(
                    "INSERT OR IGNORE INTO max_webhook_inbox(event_key,payload,status,created_at) VALUES(?,?,'pending',?)",
                    (max_update_event_key(update), json.dumps(update, ensure_ascii=False), time.time()),
                )
            if marker is not None:
                self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('marker',?)", (str(marker),))

    def seed_legacy(self, path: str | Path) -> None:
        """Read-only import, once: never turn processed/unknown actions back into pending."""
        if self.state("seeded") is not None:
            return
        path = Path(path)
        if path.exists():
            with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as legacy:
                rows = legacy.execute(
                    "SELECT payload,status,attempts,created_at,started_at,processed_at,last_error FROM max_polling_inbox"
                ).fetchall()
                marker = legacy.execute("SELECT marker FROM max_polling_state WHERE name='updates'").fetchone()
            with self._conn:
                for payload, status, attempts, created, started, processed, error in rows:
                    key = max_update_event_key(json.loads(payload))
                    if status in {"dispatching", "accepted", "processing"}:
                        status, error = "unknown", "Прерванное действие прежнего транспорта; повтор запрещён"
                    self._conn.execute(
                        "INSERT OR IGNORE INTO max_webhook_inbox "
                        "(event_key,payload,status,attempts,created_at,started_at,processed_at,last_error) VALUES(?,?,?,?,?,?,?,?)",
                        (key, payload, status, attempts, created, started, processed, error),
                    )
                if marker and marker[0] is not None and self.get() is None:
                    self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('marker',?)", (str(marker[0]),))
                self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('seeded','1')")
        else:
            with self._conn:
                self._conn.execute("INSERT OR REPLACE INTO max_transport_state VALUES ('seeded','1')")

    def close(self):
        self._conn.close()


class PollingFailover:
    """Latch into polling only after authenticated API confirms ALL subscriptions absent."""

    def __init__(self, client, url: str, probe, activate, *, threshold: int = 3):
        self.client = client
        self.url = url
        self.probe = probe
        self.activate = activate
        self.threshold = threshold
        self.failures = 0
        self.state = "webhook"
        self.error: str | None = None

    @staticmethod
    def urls(result) -> list[str]:
        values = result.get("subscriptions") if isinstance(result, Mapping) else None
        if not isinstance(values, list) or any(
            not isinstance(item, Mapping) or not isinstance(item.get("url"), str) for item in values
        ):
            raise ValueError("invalid_subscriptions_response")
        return [item["url"] for item in values]

    async def check(self) -> bool:
        if self.state == "polling":
            return True
        try:
            try:
                healthy = await self.probe()
            except Exception:
                healthy = False
            urls = self.urls(await self.client.get_subscriptions())
            if any(url != self.url for url in urls):
                self.state, self.error = "blocked", "foreign_subscription"
                return False
            if healthy and urls == [self.url]:
                self.failures, self.state, self.error = 0, "webhook", None
                return False
            self.failures += 1
            self.state = "degraded"
            if self.failures < self.threshold:
                return False
            if self.url in urls:
                await self.client.unsubscribe_webhook(self.url)
            if self.urls(await self.client.get_subscriptions()):
                self.state, self.error = "blocked", "subscription_still_present"
                return False
            result = self.activate()
            if inspect.isawaitable(result):
                await result
            self.state, self.error = "polling", None
            return True
        except Exception as exc:
            self.failures += 1
            self.state, self.error = "degraded", type(exc).__name__
            return False

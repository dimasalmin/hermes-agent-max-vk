"""Minimal ASGI ingress for MAX Webhook updates.

The app is framework-neutral and can be mounted behind a trusted HTTPS
reverse proxy. TLS termination and public port 443 remain deployment concerns.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from typing import Any, Mapping

from .webhook import WebhookResult


class MaxWebhookIngress:
    """Small handler used by a standalone ingress process."""

    def __init__(self, receiver: Any) -> None:
        self._receiver = receiver

    async def handle_webhook(
        self, headers: Mapping[str, str], update: Mapping[str, Any]
    ) -> WebhookResult:
        return await self._receiver.receive(headers, update)

    def ready(self) -> bool:
        return self._receiver.is_ready()


class MaxWebhookASGI:
    """ASGI 3 application that ACKs through ``MaxAdapter.handle_webhook``."""

    def __init__(self, adapter: Any, *, max_body_bytes: int = 1_048_576,
                 secret: str | None = None, path: str | None = None,
                 body_timeout: float = 10.0) -> None:
        self._adapter = adapter
        self._max_body_bytes = max_body_bytes
        self._secret = secret
        self._path = path
        self._body_timeout = body_timeout

    async def __call__(self, scope: Mapping[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            return
        headers = {}
        secrets = []
        for key, value in scope.get("headers", []):
            name = key.decode("latin-1").lower()
            decoded = value.decode("latin-1")
            headers[name] = decoded
            if name == "x-max-bot-api-secret":
                secrets.append(decoded)
                headers["X-Max-Bot-Api-Secret"] = decoded
        request_path = scope.get("path", "")
        if self._path and request_path not in {self._path, self._path + "/ready"}:
            await self._respond(send, 404, {"error": "not_found"})
            return
        if self._secret is not None and (
            len(secrets) != 1 or not hmac.compare_digest(secrets[0].encode(), self._secret.encode())
        ):
            await self._respond(send, 403, {"error": "forbidden"})
            return
        if self._path and request_path == self._path + "/ready":
            if scope.get("method", "").upper() != "GET":
                await self._respond(send, 405, {"error": "method_not_allowed"})
                return
            try:
                ready = self._adapter.ready()
            except Exception:
                ready = False
            await self._respond(send, 200 if ready else 503,
                                {"ready": bool(ready), "nonce": headers.get("x-max-health-nonce", "")[:64]})
            return
        if scope.get("method", "").upper() != "POST":
            await self._respond(send, 405, {"error": "method_not_allowed"})
            return

        body = bytearray()
        deadline = time.monotonic() + self._body_timeout
        while True:
            if time.monotonic() >= deadline:
                await self._respond(send, 408, {"error": "body_timeout"})
                return
            try:
                event = await asyncio.wait_for(receive(), timeout=max(0.001, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                await self._respond(send, 408, {"error": "body_timeout"})
                return
            if event.get("type") == "http.disconnect":
                return
            if event.get("type") != "http.request":
                continue
            body.extend(event.get("body", b""))
            if len(body) > self._max_body_bytes:
                await self._respond(send, 413, {"error": "request_too_large"})
                return
            if not event.get("more_body", False):
                break

        try:
            update = json.loads(bytes(body).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            await self._respond(send, 400, {"error": "invalid_json"})
            return
        if not isinstance(update, Mapping):
            await self._respond(send, 400, {"error": "update_must_be_object"})
            return

        result = await self._adapter.handle_webhook(headers, update)
        if not isinstance(result, WebhookResult):
            result = WebhookResult(status_code=500, accepted=False)
        await self._respond(
            send,
            result.status_code,
            {"accepted": result.accepted, "duplicate": result.duplicate},
        )

    @staticmethod
    async def _respond(send: Any, status: int, payload: Mapping[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

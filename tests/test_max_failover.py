from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from plugins.max.adapter import MaxAdapter
from plugins.max.failover import PollingFailover, WebhookPollingStore
from plugins.max.polling_state import PollingMarkerStore, max_update_event_key
from plugins.max.webhook import MaxWebhookReceiver

URL = "https://example.test/hermes/max"


class Client:
    def __init__(self):
        self.urls = [URL]
        self.calls = []
        self.remove = True
        self.broken = False

    async def get_subscriptions(self):
        self.calls.append("get")
        if self.broken:
            raise TimeoutError("API unavailable")
        return {"subscriptions": [{"url": url} for url in self.urls]}

    async def unsubscribe_webhook(self, url):
        assert url == URL
        self.calls.append("delete")
        if self.remove:
            self.urls = []


@pytest.mark.asyncio
async def test_fallback_deletes_and_verifies_before_activation():
    client = Client()
    async def probe():
        raise ConnectionError("VPS down")
    async def activate():
        assert client.urls == [] and client.calls[-1] == "get"
        client.calls.append("activate")
    fallback = PollingFailover(client, URL, probe, activate)
    assert not await fallback.check()
    assert not await fallback.check()
    assert await fallback.check()
    assert client.calls[-3:] == ["delete", "get", "activate"]
    assert await fallback.check()
    assert client.calls.count("activate") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["api", "foreign", "delete_ineffective"])
async def test_failure_cannot_start_polling(failure):
    client = Client()
    client.broken = failure == "api"
    client.remove = failure != "delete_ineffective"
    if failure == "foreign":
        client.urls.append("https://other.test/hook")
    async def probe():
        return False
    def activate():
        raise AssertionError("Polling must not start")
    fallback = PollingFailover(client, URL, probe, activate, threshold=1)
    assert not await fallback.check()
    assert fallback.state != "polling"
    if failure == "foreign":
        assert "delete" not in client.calls


@pytest.mark.asyncio
async def test_healthy_probe_resets_failures_and_missing_subscription_falls_back():
    client = Client()
    healthy = False
    async def probe():
        return healthy
    activated = []
    fallback = PollingFailover(client, URL, probe, lambda: activated.append(True), threshold=2)
    assert not await fallback.check()
    healthy = True
    assert not await fallback.check() and fallback.failures == 0
    client.urls = []
    assert not await fallback.check()
    assert await fallback.check() and activated == [True]


@pytest.mark.asyncio
async def test_one_inbox_deduplicates_webhook_and_polling(tmp_path):
    path = tmp_path / "unified.sqlite3"
    receiver = MaxWebhookReceiver("test-secret", inbox_path=path)
    polling = WebhookPollingStore(path)
    update = {"update_type": "message_created", "message": {"body": {"mid": "same"}}}
    await receiver.receive({"X-Max-Bot-Api-Secret": "test-secret"}, update)
    polling.accept_batch([{**update, "update_id": 99}], 101)
    assert polling.get() == 101
    assert len(await receiver.pending_updates()) == 1
    claimed = await receiver.claim_next()
    await receiver.mark_processed(claimed)
    polling.accept_batch([update], 102)
    assert await receiver.pending_updates() == []
    polling.latch()
    polling.close()
    reopened = WebhookPollingStore(path)
    assert reopened.state("fallback") == "polling" and reopened.get() == 102
    reopened.close()
    await receiver.close()


@pytest.mark.asyncio
async def test_legacy_import_preserves_states_and_marker(tmp_path):
    old_path, new_path = tmp_path / "old.db", tmp_path / "new.db"
    old = PollingMarkerStore(old_path)
    update = {"update_type": "message_created", "update_id": 1}
    old.accept_batch([update], 55)
    old.claim_next()
    old.close()
    before = old_path.read_bytes()
    receiver = MaxWebhookReceiver("test-secret", inbox_path=new_path)
    store = WebhookPollingStore(new_path)
    store.seed_legacy(old_path)
    store.seed_legacy(old_path)
    assert store.get() == 55
    assert old_path.read_bytes() == before
    assert receiver.status_summary()["unknown"] == 1
    assert await receiver.pending_updates() == []
    store.close()
    await receiver.close()


@pytest.mark.asyncio
async def test_processing_hooks_still_target_unified_inbox_after_switch(tmp_path):
    receiver = MaxWebhookReceiver("test-secret", inbox_path=tmp_path / "inbox.db")
    update = {"update_type": "message_created", "update_id": 123}
    await receiver.receive({"X-Max-Bot-Api-Secret": "test-secret"}, update)
    await receiver.claim_next()
    adapter = MaxAdapter(SimpleNamespace(token="synthetic", extra={"allowed_users": ["1"]}))
    adapter._receiver = receiver
    adapter._webhook_mode = False
    event = SimpleNamespace(metadata={"max_update_event_key": max_update_event_key(update)})
    await adapter.on_processing_start(event)
    await adapter.on_processing_complete(event, "success")
    assert receiver.status_summary()["processed"] == 1
    await receiver.close()


@pytest.mark.asyncio
async def test_adapter_switch_keeps_active_worker_and_deduplicates(monkeypatch, tmp_path):
    import plugins.max.adapter as module
    old = {"update_type": "message_created", "message": {"body": {"mid": "active", "text": "work"}}}
    new = {"update_type": "message_created", "message": {"body": {"mid": "control", "text": "/status"}}}
    blocker = asyncio.Event()
    started, control, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
    delivered = []

    class FakeClient(Client):
        def __init__(self, *args, **kwargs):
            super().__init__()
            self.urls = []
            self.polled = False
        async def get_me(self):
            return {"user_id": 12, "username": "synthetic"}
        async def set_bot_commands(self, commands):
            return {"success": True}
        async def subscribe_webhook(self, *args, **kwargs):
            self.urls = [URL]
            return {"success": True}
        async def get_updates(self, **kwargs):
            assert self.urls == []
            if self.polled:
                await blocker.wait()
            self.polled = True
            return {"updates": [old, new], "marker": 99}
        async def close(self):
            pass

    monkeypatch.setattr(module, "MaxClient", FakeClient)
    config = SimpleNamespace(token="synthetic", extra={"webhook_url": URL,
        "webhook_secret": "test-secret", "webhook_auto_fallback": True,
        "inbox_path": str(tmp_path / "inbox.db"), "marker_path": str(tmp_path / "old.db"),
        "target_path": str(tmp_path / "targets.db")})
    adapter = MaxAdapter(config)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *args: True, raising=False)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None, raising=False)
    async def probe():
        return False
    monkeypatch.setattr(adapter, "_probe_webhook", probe)
    async def dispatch(update):
        mid = update["message"]["body"]["mid"]
        delivered.append(mid)
        event = SimpleNamespace(metadata={"max_update_event_key": max_update_event_key(update)})
        await adapter.on_processing_start(event)
        if mid == "active":
            started.set()
            await finish.wait()
        else:
            control.set()
        await adapter.on_processing_complete(event, "success")
        return "accepted"
    monkeypatch.setattr(adapter, "_dispatch_update", dispatch)
    try:
        assert await adapter.connect()
        workers = list(adapter._worker_tasks)
        await adapter.handle_webhook({"X-Max-Bot-Api-Secret": "test-secret"}, old)
        await asyncio.wait_for(started.wait(), 2)
        for _ in range(3):
            await adapter._failover.check()
        assert not adapter._webhook_mode and adapter._worker_tasks == workers
        await asyncio.wait_for(control.wait(), 2)
        assert not any(task.cancelled() for task in workers)
        finish.set()
        for _ in range(100):
            if adapter._receiver.status_summary()["processed"] == 2:
                break
            await asyncio.sleep(0.01)
        assert delivered == ["active", "control"]
        assert adapter._receiver.status_summary()["processed"] == 2
        assert adapter._marker_store.state("fallback") == "polling"
        await adapter.disconnect()
        assert await adapter.connect(is_reconnect=True)
        assert not adapter._webhook_mode and adapter._receiver is not None
        assert adapter._marker_store.get() == 99
        assert adapter._failover.state == "polling"
    finally:
        finish.set()
        await adapter.disconnect()

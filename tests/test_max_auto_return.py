from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from plugins.max.adapter import MaxAdapter
from plugins.max.failover import PollingFailover, WebhookPollingStore
from plugins.max.webhook import MaxWebhookReceiver

URL = "https://example.test/max"


class Client:
    def __init__(self):
        self.urls=[]
        self.fail_post=False
        self.apply_post=True
        self.fail_read=False
        self.paused=None

    async def get_subscriptions(self):
        if self.fail_read: raise TimeoutError("API")
        return {"subscriptions":[{"url":url} for url in self.urls]}

    async def subscribe_webhook(self,*args,**kwargs):
        assert self.paused(), "Subscription must follow poller cancellation"
        if self.apply_post: self.urls=[URL]
        if self.fail_post: raise TimeoutError("ambiguous POST")

    async def get_me(self): return {"user_id":123}
    async def set_bot_commands(self,*args): return {"success":True}
    async def get_updates(self,**kwargs): await asyncio.Event().wait()
    async def close(self): pass


async def make_adapter(tmp_path):
    a=MaxAdapter(SimpleNamespace(token="test",extra={"webhook_url":URL,"webhook_secret":"test",
        "webhook_auto_fallback":True,"webhook_auto_return":True}))
    a._receiver=MaxWebhookReceiver("test",inbox_path=tmp_path/"inbox.db")
    a._marker_store=WebhookPollingStore(tmp_path/"inbox.db")
    a._client=Client()
    a._client.paused=lambda:a._polling_task is None
    a._failover=PollingFailover(a._client,URL,None,None)
    a._running=True
    blocker=asyncio.Event()
    async def poll(): await blocker.wait()
    a._poll_updates=poll
    async def probe(): return True
    a._probe_webhook=probe
    await a._activate_fallback()
    a._recovery_min_seconds=0
    return a


async def close(a):
    await a._pause_polling()
    a._marker_store.close()
    await a._receiver.close()


@pytest.mark.asyncio
async def test_return_waits_for_stable_health_and_preserves_inbox(tmp_path):
    a=await make_adapter(tmp_path)
    event={"update_type":"message_created","message":{"body":{"mid":"saved"}}}
    a._marker_store.accept_batch([event],123)
    workers=[object(),object()]
    a._worker_tasks=workers
    try:
        await a._check_webhook_return()
        assert not a._webhook_mode and a._polling_task is not None
        await a._check_webhook_return()
        assert not a._webhook_mode
        await a._check_webhook_return()
        assert a._webhook_mode and a._polling_task is None
        assert a._marker_store.state("fallback") is None and a._marker_store.get()==123
        assert len(await a._receiver.pending_updates())==1 and a._worker_tasks is workers
    finally: await close(a)


@pytest.mark.asyncio
async def test_return_minimum_hold_and_bad_probe_reset(tmp_path):
    a=await make_adapter(tmp_path)
    try:
        a._recovery_min_seconds=3600
        for _ in range(4): await a._check_webhook_return()
        assert a._polling_task is not None
        async def unhealthy(): return False
        a._probe_webhook=unhealthy
        await a._check_webhook_return()
        assert a._recovery_successes==0
    finally: await close(a)


@pytest.mark.asyncio
@pytest.mark.parametrize("applied",[True,False])
async def test_ambiguous_post_resolved_by_readback(tmp_path,applied):
    a=await make_adapter(tmp_path)
    a._client.fail_post=True
    a._client.apply_post=applied
    try:
        for _ in range(3): await a._check_webhook_return()
        assert a._webhook_mode is applied
        assert (a._polling_task is None) is applied
        assert a._marker_store.state("returning") is None
    finally: await close(a)


@pytest.mark.asyncio
async def test_foreign_subscription_fences_polling_without_deleting_it(tmp_path):
    a=await make_adapter(tmp_path)
    try:
        a._client.urls=["https://foreign.test/hook"]
        await a._check_webhook_return()
        assert a._polling_task is None and a._failover.state=="blocked"
        assert a._marker_store.state("returning")=="1"
        a._client.urls=[]
        await a._check_webhook_return()
        assert a._polling_task is not None and a._marker_store.state("returning") is None
    finally: await close(a)


@pytest.mark.asyncio
async def test_uncertain_read_after_post_stays_paused_until_reconciled(tmp_path):
    a=await make_adapter(tmp_path)
    original=a._client.subscribe_webhook
    async def subscribe(*args,**kwargs):
        await original(*args,**kwargs)
        a._client.fail_read=True
    a._client.subscribe_webhook=subscribe
    try:
        for _ in range(3): await a._check_webhook_return()
        assert a._polling_task is None and a._marker_store.state("returning")=="1"
        assert not a._webhook_mode and a._failover.state=="blocked"
        a._client.fail_read=False
        await a._check_webhook_return()
        assert a._webhook_mode and a._marker_store.state("returning") is None
    finally: await close(a)


@pytest.mark.asyncio
async def test_watchdog_keeps_running_in_polling_mode(monkeypatch,tmp_path):
    import plugins.max.adapter as module
    a=await make_adapter(tmp_path)
    count=0
    async def tick(_seconds):
        nonlocal count
        count+=1
        if count==3: a._running=False
    monkeypatch.setattr(module.asyncio,"sleep",tick)
    try:
        await a._monitor_webhook()
        assert count==3 and a._webhook_mode
    finally: await close(a)


@pytest.mark.asyncio
@pytest.mark.parametrize("registered",[True,False])
async def test_restart_during_return_reconciles_before_polling(monkeypatch,tmp_path,registered):
    import plugins.max.adapter as module
    path=tmp_path/"inbox.db"
    receiver=MaxWebhookReceiver("test",inbox_path=path)
    store=WebhookPollingStore(path)
    store.latch(); store.begin_return(); store.close()
    await receiver.close()
    client=Client()
    client.urls=[URL] if registered else []
    monkeypatch.setattr(module,"MaxClient",lambda *args,**kwargs:client)
    a=MaxAdapter(SimpleNamespace(token="test",extra={"webhook_url":URL,"webhook_secret":"test",
        "webhook_auto_fallback":True,"webhook_auto_return":True,"inbox_path":str(path),
        "marker_path":str(tmp_path/"old.db"),"target_path":str(tmp_path/"targets.db")}))
    monkeypatch.setattr(a,"_acquire_platform_lock",lambda *args:True,raising=False)
    monkeypatch.setattr(a,"_release_platform_lock",lambda:None,raising=False)
    try:
        assert await a.connect()
        assert a._webhook_mode is registered
        assert (a._polling_task is None) is registered
        assert a._marker_store.state("returning") is None
    finally: await a.disconnect()

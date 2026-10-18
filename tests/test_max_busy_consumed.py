from types import SimpleNamespace

import pytest

from plugins.max.adapter import MaxAdapter, BasePlatformAdapter
from plugins.max.polling_state import PollingMarkerStore
from plugins.max.webhook import MaxWebhookReceiver


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted",[True,False])
async def test_busy_consumption_is_not_blanket_acceptance(monkeypatch,admitted):
    # The native contract separately uses the actual BasePlatformAdapter setter.
    monkeypatch.setattr(BasePlatformAdapter,"set_busy_session_handler",lambda self,h:setattr(self,"_busy_session_handler",h),raising=False)
    a=MaxAdapter(SimpleNamespace(token="test",extra={}))
    async def handler(event,key):
        event._bot_loop_admitted=admitted
        return True
    a.set_busy_session_handler(handler)
    event=SimpleNamespace(_gateway_accepted=False)
    assert await a._busy_session_handler(event,"session") is True
    assert event._max_gateway_consumed is admitted
    assert event._gateway_accepted is False


@pytest.mark.asyncio
async def test_real_rejection_remains_rejection(monkeypatch):
    monkeypatch.setattr(BasePlatformAdapter,"set_busy_session_handler",lambda self,h:setattr(self,"_busy_session_handler",h),raising=False)
    a=MaxAdapter(SimpleNamespace(token="test",extra={}))
    async def handler(event,key): return False
    a.set_busy_session_handler(handler)
    event=SimpleNamespace(_gateway_accepted=False,_bot_loop_admitted=True)
    assert await a._busy_session_handler(event,"s") is False
    assert not event._max_gateway_consumed


@pytest.mark.asyncio
async def test_consumed_is_durable_and_never_replayed(tmp_path):
    event={"update_type":"message_created","message":{"body":{"mid":"consumed"}}}
    old=PollingMarkerStore(tmp_path/"old.db")
    old.accept_batch([event],1)
    old.claim_next()
    old.mark_consumed(event)
    assert old.status_summary()["consumed"]==1
    assert old.claim_next() is None
    old.close()
    receiver=MaxWebhookReceiver("test",inbox_path=tmp_path/"new.db")
    await receiver.receive({"X-Max-Bot-Api-Secret":"test"},event)
    await receiver.claim_next()
    await receiver.mark_consumed(event)
    assert receiver.status_summary()["consumed"]==1
    assert receiver.status_summary()["failed"]==0
    assert await receiver.claim_next() is None
    await receiver.close()

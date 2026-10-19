import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from plugins.max.adapter import MaxAdapter
from plugins.max.client import MaxApiError


def make_adapter():
    adapter = object.__new__(MaxAdapter)
    adapter._client = SimpleNamespace(
        send_action=AsyncMock(return_value={"success": True}),
        send_message=AsyncMock(return_value={"message": {"body": {"mid": "reply"}}}),
    )
    adapter._rate_limiter = SimpleNamespace(acquire=AsyncMock())
    adapter._chat_target_types = {"42": "user", "43": "user"}
    adapter._target_store = None
    return adapter


def make_event(target="42", internal_id=9001, **raw):
    return SimpleNamespace(
        source=SimpleNamespace(chat_id=target),
        metadata={},
        raw_message={"recipient": {"chat_type": "dialog", "chat_id": internal_id}, **raw},
    )


@pytest.mark.asyncio
async def test_dm_typing_uses_dialog_id_without_changing_reply_target():
    adapter = make_adapter()
    event = make_event()

    await adapter.on_processing_start(event)
    await adapter.send_typing("42")
    result = await adapter.send("42", "reply")
    await adapter.stop_typing("42")

    assert event.source.chat_id == "42"
    adapter._client.send_action.assert_awaited_once_with("9001", "typing_on")
    assert result.success
    assert adapter._client.send_message.await_args.args == ("42", "reply")
    assert adapter._client.send_message.await_args.kwargs["target_type"] == "user"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", 0, True, 9001.5, "9001/actions", 2**63, -(2**63)-1])
async def test_missing_or_invalid_dialog_id_never_falls_back_to_user_id(value):
    adapter = make_adapter()
    await adapter.on_processing_start(make_event(internal_id=value))
    await adapter.send_typing("42")
    adapter._client.send_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_forwarded_dialog_id_does_not_replace_outer_recipient():
    adapter = make_adapter()
    event = make_event(internal_id=None, link={"type": "forward", "message": {
        "recipient": {"chat_type": "dialog", "chat_id": 9999},
    }})
    await adapter.on_processing_start(event)
    await adapter.send_typing("42")
    adapter._client.send_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_dialog_mapping_is_scoped_to_adapter_and_user():
    adapter = make_adapter()
    other = make_adapter()
    await adapter.on_processing_start(make_event())
    await adapter.on_processing_start(make_event(target="43", internal_id="9002"))
    await adapter.send_typing("42")
    await adapter.send_typing("43")
    await other.send_typing("42")
    assert adapter._client.send_action.await_args_list == [
        call("9001", "typing_on"), call("9002", "typing_on"),
    ]
    other._client.send_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_latest_authorized_turn_refreshes_dialog_mapping():
    adapter = make_adapter()
    await adapter.on_processing_start(make_event())
    await adapter.on_processing_start(make_event(internal_id=9003))
    await adapter.send_typing("42")
    adapter._client.send_action.assert_awaited_once_with("9003", "typing_on")


@pytest.mark.asyncio
async def test_typing_failure_does_not_prevent_reply():
    adapter = make_adapter()
    adapter._client.send_action.side_effect = MaxApiError("synthetic action error", status_code=400)
    await adapter.on_processing_start(make_event())
    await adapter.send_typing("42")
    assert (await adapter.send("42", "reply")).success


@pytest.mark.asyncio
async def test_stop_does_not_send_undocumented_typing_off():
    adapter = make_adapter()
    adapter._chat_target_types["-9001"] = "chat"
    await adapter.stop_typing("-9001::user::42")
    await adapter.stop_typing("42")
    adapter._client.send_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_typing_cancellation_is_not_swallowed():
    adapter = make_adapter()
    adapter._client.send_action.side_effect = asyncio.CancelledError
    await adapter.on_processing_start(make_event())
    with pytest.raises(asyncio.CancelledError):
        await adapter.send_typing("42")


@pytest.mark.asyncio
async def test_group_turn_cannot_seed_a_dm_action_mapping():
    adapter = make_adapter()
    event = make_event(recipient={"chat_type": "chat", "chat_id": -9001})
    await adapter.on_processing_start(event)
    await adapter.send_typing("42")
    adapter._client.send_action.assert_not_awaited()

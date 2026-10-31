"""Command UI contracts, with synthetic targets and no external API."""
from __future__ import annotations

from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from plugins.max.adapter import MaxAdapter
from plugins.max.adapter import BasePlatformAdapter
from plugins.max.common import AccessPolicy
from plugins.max.interactive import MaxCallbackStore
from plugins.max.models import MaxCallback, MaxMessage
from plugins.max.rate_limit import MaxRateLimiter
from plugins.max.command_ui import COMMAND_CONTEXT


def adapter():
    obj = object.__new__(MaxAdapter)
    obj._client = SimpleNamespace(
        send_message=AsyncMock(return_value={'message': {'body': {'mid': 'prompt-1'}}}),
        answer_callback=AsyncMock(return_value={'success': True}))
    obj._chat_target_types = {}
    obj._target_store = None
    obj._rate_limiter = MaxRateLimiter()
    obj._callbacks = MaxCallbackStore()
    obj._access = AccessPolicy(allowed_users={'user-1', 'user-2'})
    obj._extra = {}
    obj.handle_message = AsyncMock(side_effect=AssertionError('Choice must not enter LLM'))
    return obj


def click(payload, *, user='user-1', chat='user-1', mid='prompt-1', group=False):
    return MaxCallback(callback_id='synthetic-click', payload=payload, user_id=user, user_name=None,
        chat_id=chat, chat_type='chat' if group else 'dialog', message_id=mid)


async def picker(obj, callback=None, **kwargs):
    callback = callback or AsyncMock(return_value='Уровень изменён')
    result = await obj.send_choice_picker('user-1', 'Уровень рассуждений',
        [{'value': 'low', 'label': 'low'}, {'value': 'high', 'label': 'high', 'is_current': True}],
        'synthetic-session', callback, **kwargs)
    return result, callback


def buttons(obj):
    return [b for row in obj._client.send_message.call_args.kwargs['attachments'][0]['payload']['buttons'] for b in row]


@pytest.mark.asyncio
async def test_choice_picker_uses_russian_labels_and_opaque_bound_tokens():
    obj = adapter()
    result, _ = await picker(obj)
    assert result.success
    assert [b['text'] for b in buttons(obj)] == ['Низкий', '✓ Высокий', 'Отмена']
    assert all(b['payload'].startswith('hmx:choice:') and 'synthetic-session' not in b['payload'] for b in buttons(obj))


@pytest.mark.asyncio
async def test_choice_calls_native_callback_once_and_invalidates_alternatives():
    obj = adapter()
    _, selected = await picker(obj)
    low, high, _ = buttons(obj)
    await obj._dispatch_callback(click(high['payload']))
    await obj._dispatch_callback(click(high['payload']))
    await obj._dispatch_callback(click(low['payload']))
    selected.assert_awaited_once_with('user-1', 'high')
    obj.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_choice_rejects_other_user_chat_and_message():
    obj = adapter()
    _, selected = await picker(obj)
    payload = buttons(obj)[0]['payload']
    for cb in (click(payload, user='user-2'), click(payload, chat='other-chat'), click(payload, mid='other-prompt')):
        await obj._dispatch_callback(cb)
    selected.assert_not_awaited()
    await obj._dispatch_callback(click(payload))
    selected.assert_awaited_once_with('user-1', 'low')


@pytest.mark.asyncio
async def test_cancel_does_not_apply_native_selection():
    obj = adapter()
    _, selected = await picker(obj)
    await obj._dispatch_callback(click(buttons(obj)[-1]['payload']))
    selected.assert_not_awaited()
    assert obj._client.answer_callback.call_args.kwargs['message']['attachments'] == []


@pytest.mark.asyncio
async def test_next_authorized_request_invalidates_picker():
    obj = adapter()
    _, selected = await picker(obj)
    payload = buttons(obj)[0]['payload']
    obj._note_choice_request('user-1', 'user-1')
    await obj._dispatch_callback(click(payload))
    selected.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_picker_does_not_apply():
    obj = adapter()
    _, selected = await picker(obj)
    payload = buttons(obj)[0]['payload']
    next(iter(obj._choice_pickers.values()))['expires_at'] = 0
    await obj._dispatch_callback(click(payload))
    selected.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_send_drops_pending_choice_state():
    obj = adapter()
    obj._send_interactive_prompt = AsyncMock(return_value=SimpleNamespace(success=False))
    result, _ = await picker(obj)
    assert not result.success
    assert not obj._choice_pickers


@pytest.mark.asyncio
async def test_empty_or_excessive_choices_fail_without_send():
    obj = adapter()
    for values in ([], [{'value': str(i), 'label': str(i)} for i in range(59)]):
        result = await obj.send_choice_picker('user-1', 'Выбор', values, 'session', lambda *a: '')
        assert not result.success
    obj._client.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_commands_is_not_intercepted_and_pagination_survives():
    obj = adapter()
    obj._send_menu = AsyncMock()
    for text in ('/commands', '/commands 2'):
        msg = MaxMessage(message_id='synthetic', user_id='user-1', user_name=None,
            chat_id='user-1', chat_type='dialog', chat_title=None, text=text)
        assert not await obj._handle_local_command(msg)
    obj._send_menu.assert_not_awaited()


def test_menu_includes_goal_reasoning_from_registry(monkeypatch):
    import sys
    mod = ModuleType('hermes_cli.commands')
    names = ('help', 'commands', 'status', 'new', 'stop', 'model', 'goal', 'reasoning',
             'compress', 'sessions', 'resume', 'retry', 'undo', 'agents', 'whoami', 'queue')
    mod.COMMAND_REGISTRY = [SimpleNamespace(name=n, cli_only=False, description=n) for n in names]
    mod._is_gateway_available = lambda c: True
    monkeypatch.setitem(sys.modules, 'hermes_cli.commands', mod)
    menu = adapter()._max_commands()
    assert len(menu) == 18
    assert {c['name'] for c in menu} == set(names) | {'menu', 'maxstatus'}
    assert 'Выбрать' in next(c['description'] for c in menu if c['name'] == 'reasoning')


@pytest.mark.asyncio
async def test_failed_native_callback_is_not_retried():
    obj = adapter()
    callback = AsyncMock(side_effect=RuntimeError('synthetic failure'))
    await picker(obj, callback)
    payload = buttons(obj)[0]['payload']
    await obj._dispatch_callback(click(payload))
    await obj._dispatch_callback(click(payload))
    callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_command_cannot_create_picker_for_newer_request():
    obj = adapter()
    old = obj._note_choice_request('user-1', 'user-1')
    obj._note_choice_request('user-1', 'user-1')
    token = COMMAND_CONTEXT.set({'adapter': id(obj), 'user_id': 'user-1',
                                'metadata': {'max_request_version': old}})
    try:
        result, _ = await picker(obj)
    finally:
        COMMAND_CONTEXT.reset(token)
    assert not result.success
    obj._client.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_group_picker_is_bound_to_initiating_admin_not_any_admin():
    obj = adapter()
    obj._access = AccessPolicy(admin_users={'admin-1', 'admin-2'}, group_allowed_chats={'group-1'})
    selected = AsyncMock(return_value='Готово')
    result = await obj.send_choice_picker('group-1', 'Выбор', [{'value': 'high', 'label': 'high'}],
        'group-session', selected, metadata={'max_target_type': 'chat', 'max_user_id': 'admin-1'})
    assert result.success
    payload = buttons(obj)[0]['payload']
    await obj._dispatch_callback(click(payload, user='admin-2', chat='group-1', group=True))
    selected.assert_not_awaited()
    await obj._dispatch_callback(click(payload, user='admin-1', chat='group-1', group=True))
    selected.assert_awaited_once_with('group-1', 'high')


@pytest.mark.asyncio
async def test_unprivileged_group_picker_is_not_sent():
    obj = adapter()
    result = await obj.send_choice_picker('group-1', 'Выбор', [{'value': 'high'}], 'session', lambda *a: '',
        metadata={'max_target_type': 'chat', 'max_user_id': 'user-1'})
    assert not result.success
    obj._client.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_queue_management_is_never_forwarded_as_model_prompt():
    obj = adapter()
    obj.gateway_runner = SimpleNamespace(_session_key_for_source=lambda source: 'own-session',
        _queue_depth=lambda key, **kw: 3)
    obj.build_source = lambda **kw: SimpleNamespace(**kw)
    obj.send = AsyncMock()
    for text in ('/queue', '/queue list', '/queue clear', '/queue edit 1 example'):
        message = MaxMessage(message_id='synthetic', user_id='user-1', user_name=None,
            chat_id='user-1', chat_type='dialog', chat_title=None, text=text)
        assert await obj._handle_local_command(message)
    assert '3' in obj.send.call_args.args[1]
    assert 'не поддерживает' in obj.send.call_args.args[1]
    obj.handle_message.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('busy', [False, True])
async def test_queue_text_uses_native_turn_or_native_fifo(monkeypatch, busy):
    obj = adapter()
    # Remove this fixture's LLM tripwire to exercise the actual adapter method.
    del obj.handle_message
    obj.gateway_runner = SimpleNamespace(_is_session_running=lambda key: busy)
    obj._event_session_key = lambda event: 'own-session'
    obj._active_sessions = {'own-session': object()} if busy else {}
    seen = []
    async def native(self, event):
        seen.append(event.text)
    monkeypatch.setattr(BasePlatformAdapter, 'handle_message', native)
    event = SimpleNamespace(text='/queue add synthetic prompt', get_command=lambda: 'queue',
        get_command_args=lambda: 'add synthetic prompt')
    await obj.handle_message(event)
    assert seen == ['/queue synthetic prompt' if busy else 'synthetic prompt']

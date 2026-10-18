from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from plugins.max.adapter import MaxAdapter, apply_yaml_config
from plugins.max.common import AccessPolicy
from plugins.max.polling_state import PollingMarkerStore


def _policy(admins: str | None) -> AccessPolicy:
    return AccessPolicy.from_env_and_extra(
        allowed_users_env="42", group_allowed_users_env=None,
        group_allowed_chats_env=None, allow_all_env=None, guest_mode_env=None,
        admin_users_env=admins,
    )


def test_empty_yaml_admin_list_does_not_disable_owner_commands(monkeypatch) -> None:
    monkeypatch.delenv("MAX_ADMIN_USERS", raising=False)
    extra = apply_yaml_config({}, {"extra": {"allow_admin_from": []}})
    policy = _policy(os.environ.get("MAX_ADMIN_USERS"))

    assert extra["allow_admin_from"] == []
    assert policy.admin_users == set()
    assert policy.can_dm("42")
    assert policy.can_run_command("42", "/status", is_group=False)
    assert policy.can_run_command("42", "/new", is_group=False)


@pytest.mark.parametrize("raw", ["[]", "[ ]"])
def test_legacy_empty_admin_list_is_empty(raw) -> None:
    assert _policy(raw).admin_users == set()
    assert _policy(raw).can_run_command("42", "/status", is_group=False)


@pytest.mark.parametrize("raw", ['["42", "7"]', "['42', '7']", "42,7"])
def test_list_and_csv_admins_preserve_explicit_permissions(raw) -> None:
    policy = _policy(raw)
    assert policy.admin_users == {"42", "7"}
    assert policy.can_run_command("42", "/new", is_group=False)
    assert not policy.can_run_command("other", "/new", is_group=False)


@pytest.mark.parametrize("raw", ["[null]", "[true]", "[{}]", "[invalid", '["42", []]'])
def test_invalid_structured_admins_fail_closed(raw) -> None:
    with pytest.raises(ValueError):
        _policy(raw)


@pytest.mark.parametrize("key,env", [
    ("allow_from", "MAX_ALLOWED_USERS"),
    ("group_allow_from", "MAX_GROUP_ALLOWED_USERS"),
    ("group_allowed_chats", "MAX_GROUP_ALLOWED_CHATS"),
    ("allow_admin_from", "MAX_ADMIN_USERS"),
])
def test_yaml_id_lists_are_bridged_as_csv(monkeypatch, key, env) -> None:
    monkeypatch.delenv(env, raising=False)
    extra = apply_yaml_config({}, {"extra": {key: [42, "7"]}})
    assert os.environ[env] == "42,7"
    assert extra[key] == [42, "7"]


@pytest.mark.asyncio
async def test_denied_command_is_explained_and_recorded_as_denied(tmp_path) -> None:
    adapter = object.__new__(MaxAdapter)
    adapter.platform = "max"
    adapter._bot_user_id = None
    adapter._bot_username = None
    adapter._chat_target_types = {}
    adapter._target_store = None
    adapter._access = AccessPolicy(allowed_users={"42"}, admin_users={"admin"})
    adapter._webhook_mode = False
    adapter._marker_store = PollingMarkerStore(tmp_path / "inbox.sqlite3")
    adapter._receiver = None
    adapter._inbox_wakeup = asyncio.Event()
    adapter._running = True
    adapter._require_mention = True
    adapter.send = AsyncMock(return_value=SimpleNamespace(success=True, message_id="notice"))
    adapter.handle_message = AsyncMock()
    update = {"update_type": "message_created", "message": {
        "sender": {"user_id": "42"}, "recipient": {"chat_type": "dialog"},
        "body": {"mid": "denied-command", "text": "/status"},
    }}
    adapter._marker_store.accept_batch([update], 1)
    worker = asyncio.create_task(adapter._consume_durable_updates(control_only=True))
    for _ in range(100):
        row = adapter._marker_store._conn.execute("SELECT status FROM max_polling_inbox").fetchone()
        if row[0] != "pending" and row[0] != "dispatching":
            break
        await asyncio.sleep(0.01)
    adapter._running = False
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    row = adapter._marker_store._conn.execute("SELECT status, last_error FROM max_polling_inbox").fetchone()
    assert row[0] == "failed"
    assert "доступ" in row[1].lower()
    adapter.handle_message.assert_not_awaited()
    adapter.send.assert_awaited_once()
    assert "/status" in adapter.send.await_args.args[1]
    assert "ограничена" in adapter.send.await_args.args[1]
    adapter._marker_store.close()

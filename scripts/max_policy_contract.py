"""Offline native-Hermes regression for MAX command policy and two bot transports."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import AsyncMock


def _no_network(event, _args):
    if event in {"socket.connect", "socket.getaddrinfo", "socket.bind"}:
        raise RuntimeError("MAX policy contract prohibits network access")


async def _verify(plugin_dir: Path, home: Path) -> dict:
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platform_registry import PlatformEntry, platform_registry
    from gateway.platforms.base import BasePlatformAdapter
    from gateway.run import GatewayRunner
    from agent.secret_scope import reset_secret_scope, set_multiplex_active, set_secret_scope
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    name = "max_policy_contract_plugin"
    spec = importlib.util.spec_from_file_location(
        name, plugin_dir / "__init__.py", submodule_search_locations=[str(plugin_dir)])
    if spec is None or spec.loader is None:
        raise RuntimeError("MAX plugin cannot be imported")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    if not issubclass(module.MaxAdapter, BasePlatformAdapter):
        raise RuntimeError("MAX adapter did not import the native Hermes base class")

    class Context:
        def register_platform(self, **kwargs):
            platform_registry.register(PlatformEntry(**kwargs))

    module.register(Context())

    class SecondaryAdapter(module.MaxAdapter):
        def __init__(self, config):
            super().__init__(config)
            self.platform = Platform("max_contract_secondary")

    platform_registry.register(PlatformEntry(
        name="max_contract_secondary", label="MAX contract secondary",
        adapter_factory=SecondaryAdapter, check_fn=lambda: True,
        allowed_users_env="MAX_ALLOWED_USERS", allow_all_env="MAX_ALLOW_ALL_USERS"))
    os.environ["MAX_ALLOWED_USERS"] = "42"
    module.adapter.apply_yaml_config({}, {"extra": {"allow_admin_from": []}})
    policy_adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="offline-primary"))
    assert policy_adapter._access.admin_users == set(), "Empty admin list became a phantom admin"
    assert policy_adapter._access.can_run_command("42", "/status", is_group=False)
    assert policy_adapter._access.can_run_command("42", "/new", is_group=False)
    os.environ["MAX_ADMIN_USERS"] = "[]"
    legacy_adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="offline-primary"))
    assert legacy_adapter._access.admin_users == set(), "Legacy empty list still blocks commands"
    os.environ.pop("MAX_ADMIN_USERS", None)

    configs = {
        Platform("max"): PlatformConfig(enabled=True, token="offline-primary"),
        Platform("max_contract_secondary"): PlatformConfig(enabled=True, token="offline-secondary", extra={
            "marker_path": str(home / "secondary" / "polling.sqlite3"),
            "target_path": str(home / "secondary" / "targets.sqlite3"),
        }),
    }
    runner = GatewayRunner(GatewayConfig(
        platforms=configs, sessions_dir=home / "sessions", multiplex_profiles=False))
    runner._resolve_route_context = AsyncMock(return_value=None)
    runner._handle_message_with_agent = AsyncMock(side_effect=AssertionError("Model work forbidden"))
    sent = []

    class Client:
        def __init__(self, platform):
            self.platform = platform

        async def send_message(self, target, content, **_kwargs):
            assert str(target) == "42", "Response was routed to the wrong MAX recipient"
            assert content, "Native command produced an empty response"
            sent.append(self.platform)
            return {"message": {"body": {"mid": f"offline-{self.platform}-{len(sent)}"}}}

        async def send_action(self, *_args, **_kwargs):
            return {"success": True}

    adapters = {
        Platform("max"): module.MaxAdapter(configs[Platform("max")]),
        Platform("max_contract_secondary"): SecondaryAdapter(configs[Platform("max_contract_secondary")]),
    }
    runner.adapters = adapters
    for platform, adapter in adapters.items():
        adapter.gateway_runner = runner
        adapter._client = Client(platform.value)
        adapter.set_message_handler(runner._primary_message_handler())
    keys = {adapter._source_session_key(adapter.build_source(chat_id="42", user_id="42", chat_type="dm"))
            for adapter in adapters.values()}
    assert len(keys) == 2, "Two MAX bot transports shared a Hermes session key"
    for command in ("status", "new"):
        before = len(sent)
        dispositions = await asyncio.gather(*(
            adapter._dispatch_update({"update_type": "message_created", "message": {
                "sender": {"user_id": "42", "is_bot": False},
                "recipient": {"chat_type": "dialog", "chat_id": 123},
                "body": {"mid": f"offline-{platform.value}-{command}", "text": f"/{command}"},
            }}) for platform, adapter in adapters.items()))
        assert dispositions == ["accepted", "accepted"], "Native commands were rejected at intake"
        tasks = [task for adapter in adapters.values() for task in adapter._session_tasks.values()]
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=15)
        assert sorted(sent[before:]) == ["max", "max_contract_secondary"], "A native command failed to deliver on both transports"

    scoped_modes = []
    for multiplex in (False, True):
        set_multiplex_active(multiplex)
        child = home / ("multiplex-child" if multiplex else "standalone-child")
        os.environ["MAX_BOT_TOKEN"] = "offline-parent-only-token"
        os.environ["MAX_API_BASE_URL"] = "https://parent.example.invalid"
        os.environ["MAX_MARKER_PATH"] = str(home / "parent-only.sqlite3")
        os.environ.pop("MAX_ADMIN_USERS", None)
        scope_token = set_secret_scope({"MAX_ALLOWED_USERS": "99"}, profile_home=str(child))
        home_token = set_hermes_home_override(child)
        try:
            extra = module.adapter.apply_yaml_config({}, {"extra": {
                "allow_admin_from": ["99"], "api_base_url": "https://child.example.invalid"}})
            assert "MAX_ADMIN_USERS" not in os.environ, "A foreign YAML bridge mutated the parent policy"
            assert os.environ["MAX_API_BASE_URL"] == "https://parent.example.invalid"
            assert extra["api_base_url"] == "https://child.example.invalid"
            scoped = module.MaxAdapter(PlatformConfig(enabled=True, extra=extra))
            assert scoped._access.can_dm("99") and not scoped._access.can_dm("42")
            assert scoped._access.admin_users == {"99"}
            assert scoped._token == "", "A child inherited the parent MAX bot token"
            assert scoped._marker_path == str(child / "max" / "polling-marker.sqlite3")
            scoped_modes.append("multiplex" if multiplex else "standalone")
        finally:
            reset_hermes_home_override(home_token)
            reset_secret_scope(scope_token)
            set_multiplex_active(False)
    return {"native_base": BasePlatformAdapter.__module__, "empty_admin_list": "ok",
            "legacy_empty_admin_list": "ok", "concurrent_status_and_new": "ok",
            "distinct_session_keys": len(keys), "scope_modes": scoped_modes,
            "network_access": "prohibited", "hermes_core_writes": False}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-root", type=Path, required=True)
    parser.add_argument("--plugin-dir", type=Path, default=Path(__file__).resolve().parents[1] / "plugins/max")
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.hermes_root.resolve()))
    logging.basicConfig(level=logging.ERROR)
    for key in list(os.environ):
        if key.startswith("MAX_") or key in {"GATEWAY_ALLOWED_USERS", "GATEWAY_ALLOW_ALL_USERS"}:
            os.environ.pop(key)
    with tempfile.TemporaryDirectory(prefix="max-native-policy-") as directory:
        os.environ["HERMES_HOME"] = directory
        os.environ["GATEWAY_MULTIPLEX_PROFILES"] = "false"
        sys.addaudithook(_no_network)
        result = asyncio.run(_verify(args.plugin_dir.resolve(), Path(directory)))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

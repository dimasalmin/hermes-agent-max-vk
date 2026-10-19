"""Read-only compatibility smoke for the current Hermes plugin loader.

Usage from WSL:
    python3 scripts/max_loader_smoke.py --hermes-root /home/user/.hermes/hermes-agent

The script imports the plugin as Hermes does, captures registration metadata,
and never writes to Hermes home, config, services, or the running gateway.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
import logging
import os
import sys
import types
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-root", type=Path, required=True)
    parser.add_argument(
        "--plugin-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "plugins" / "max",
    )
    args = parser.parse_args()

    hermes_root = args.hermes_root.resolve()
    plugin_dir = args.plugin_dir.resolve()
    if not (hermes_root / "gateway").is_dir():
        raise SystemExit(f"Hermes root is invalid: {hermes_root}")
    if not (plugin_dir / "plugin.yaml").is_file():
        raise SystemExit(f"MAX plugin manifest is missing: {plugin_dir / 'plugin.yaml'}")

    sys.path.insert(0, str(hermes_root))
    parent = types.ModuleType("hermes_plugins")
    parent.__path__ = []  # type: ignore[attr-defined]
    sys.modules["hermes_plugins"] = parent
    name = "hermes_plugins.max_platform"
    spec = importlib.util.spec_from_file_location(
        name,
        plugin_dir / "__init__.py",
        submodule_search_locations=[str(plugin_dir)],
    )
    if spec is None or spec.loader is None:
        raise SystemExit("Could not create plugin import spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    context = types.SimpleNamespace()
    context.register_platform = lambda **kwargs: setattr(context, "entry", kwargs)
    module.register(context)
    entry = context.entry
    required = {
        "apply_yaml_config_fn",
        "env_enablement_fn",
        "is_connected",
        "standalone_sender_fn",
    }
    missing = sorted(key for key in required if not entry.get(key))
    if missing:
        raise SystemExit(f"MAX registration hooks missing: {', '.join(missing)}")

    # The real PluginContext writes a PlatformEntry to Hermes' registry before
    # any adapter factory is instantiated. Mirror that small lifecycle step so
    # Platform("max") exercises the same dynamic-enum path as gateway startup.
    from gateway.platform_registry import PlatformEntry, platform_registry
    from gateway.platforms.base import BasePlatformAdapter, SendResult

    platform_registry.register(PlatformEntry(**entry))
    signature = inspect.signature(module.MaxAdapter.connect)
    if "is_reconnect" not in signature.parameters:
        raise SystemExit("MaxAdapter.connect lacks is_reconnect")
    try:
        from gateway.config import PlatformConfig

        adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="loader-smoke-token"))
    except Exception as exc:  # noqa: BLE001 - report the contract failure clearly
        raise SystemExit(f"MaxAdapter cannot instantiate against Hermes: {exc}") from exc

    if not isinstance(adapter, BasePlatformAdapter):
        raise SystemExit("MaxAdapter does not inherit Hermes BasePlatformAdapter")
    saved_policy_env = {key: os.environ.get(key) for key in ("MAX_ALLOWED_USERS", "MAX_ADMIN_USERS")}
    try:
        os.environ["MAX_ALLOWED_USERS"] = "loader-owner"
        os.environ.pop("MAX_ADMIN_USERS", None)
        policy_extra = entry["apply_yaml_config_fn"]({}, {"extra": {"allow_admin_from": []}})
        policy_adapter = module.MaxAdapter(PlatformConfig(
            enabled=True, token="loader-smoke-token", extra=policy_extra))
        if policy_adapter._access.admin_users:
            raise SystemExit("MAX empty YAML admin list became a phantom administrator")
        for command in ("/status", "/new"):
            if not policy_adapter._access.can_run_command("loader-owner", command, is_group=False):
                raise SystemExit(f"MAX empty admin list blocks {command}")
    finally:
        for key, value in saved_policy_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    for hook in ("on_processing_start", "on_processing_complete"):
        method = getattr(adapter, hook, None)
        if not callable(method) or not inspect.iscoroutinefunction(method):
            raise SystemExit(f"MAX adapter is missing Hermes processing hook {hook}")
    for method in (
        "send_image", "send_image_file", "send_document", "send_voice",
        "send_video", "send_animation", "send_multiple_images",
    ):
        if not callable(getattr(adapter, method, None)):
            raise SystemExit(f"MAX adapter is missing Hermes media method {method}")

    from gateway.platforms.base import MessageType
    audio_message = module.adapter.MaxMessage.from_update({
        "update_type": "message_created", "message": {
            "sender": {"user_id": 123},
            "recipient": {"chat_id": 456, "chat_type": "dialog"},
            "body": {"mid": "offline-audio", "text": "", "attachments": [
                {"type": "audio", "payload": {"url": "https://maxvd1.okcdn.ru/voice.ogg"}}]},
        },
    })
    if module.adapter._message_type(audio_message) != MessageType.VOICE:
        raise SystemExit("MAX audio bypasses the native Hermes STT pipeline")

    async def verify_send_result_contract() -> None:
        results = [
            SendResult(success=True, message_id="first"),
            SendResult(success=False, error="synthetic failure"),
            SendResult(success=True, message_id="last"),
        ]

        async def fake_send_image(chat_id, image_url, **kwargs):
            del chat_id, image_url, kwargs
            return results.pop(0)

        adapter.send_image = fake_send_image
        result = await adapter.send_multiple_images(
            "test-chat",
            [("https://example.invalid/1.png", ""),
             ("https://example.invalid/2.png", ""),
             ("https://example.invalid/3.png", "")],
        )
        if not isinstance(result, SendResult):
            raise RuntimeError("send_multiple_images did not return Hermes SendResult")
        if not result.success or result.message_id != "last":
            raise RuntimeError("send_multiple_images returned an invalid partial-success result")
        if tuple(result.continuation_message_ids) != ("first",):
            raise RuntimeError("send_multiple_images lost continuation message IDs")
        if not result.error or result.error_kind != "partial_media":
            raise RuntimeError("send_multiple_images hid a partial image-delivery failure")

    async def verify_typing_contract() -> None:
        typing_adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="offline-typing-token"))
        actions = []
        repeated = asyncio.Event()

        async def fake_action(chat_id, action):
            actions.append((chat_id, action))
            if len(actions) >= 2:
                repeated.set()
            return {"success": True}

        async def no_wait(chat_id):
            pass

        typing_adapter._client = types.SimpleNamespace(send_action=fake_action)
        typing_adapter._rate_limiter = types.SimpleNamespace(acquire=no_wait)
        event = module.adapter._build_message_event(typing_adapter, audio_message)
        await typing_adapter.on_processing_start(event)
        if event.source.chat_id != "123":
            raise RuntimeError("Typing mapping changed the Hermes DM session ID")
        task = asyncio.create_task(typing_adapter._keep_typing("123", interval=0.01))
        try:
            await asyncio.wait_for(repeated.wait(), timeout=2)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        count = len(actions)
        await asyncio.sleep(0.03)
        if len(actions) != count or any(item != ("456", "typing_on") for item in actions):
            raise RuntimeError("Native typing refresh used an invalid target/action or continued after stop")
        typing_adapter._client = None

    previous_log_disable = logging.root.manager.disable
    try:
        logging.disable(logging.WARNING)
        asyncio.run(verify_send_result_contract())
        asyncio.run(verify_typing_contract())
    except Exception as exc:  # noqa: BLE001 - report the contract failure clearly
        raise SystemExit(f"MAX native adapter contract failed: {exc}") from exc
    finally:
        logging.disable(previous_log_disable)

    print(f"plugin_import=ok module={name}")
    print(f"platform_name={entry['name']}")
    print("adapter_instantiation=ok")
    print("native_send_result_contract=ok")
    print("native_processing_hooks=ok")
    print("native_empty_admin_policy=ok")
    print("native_audio_stt_routing=ok")
    print("native_typing_refresh_contract=ok")
    print(f"hooks={','.join(sorted(required))}")
    print("writes_hermes_core=no")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

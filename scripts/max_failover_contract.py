"""Offline failover contract with the installed Hermes BasePlatformAdapter."""

import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace


async def exercise(plugin_dir, home, auto_return=False):
    os.environ["HERMES_HOME"] = str(home)
    spec = importlib.util.spec_from_file_location("max_failover_contract_plugin", plugin_dir / "__init__.py",
                                                submodule_search_locations=[str(plugin_dir)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    from gateway.config import PlatformConfig
    from gateway.platform_registry import PlatformEntry, platform_registry
    from gateway.platforms.base import BasePlatformAdapter
    class Context:
        def register_platform(self, **kwargs):
            platform_registry.register(PlatformEntry(**kwargs))
    module.register(Context())
    url = "https://example.test/hermes/max"
    waiting, finish, control = asyncio.Event(), asyncio.Event(), asyncio.Event()
    block = asyncio.Event()
    first = {"update_type": "message_created", "message": {"body": {"mid": "first", "text": "work"}}}
    second = {"update_type": "message_created", "message": {"body": {"mid": "second", "text": "/status"}}}
    seen = []
    class Client:
        def __init__(self, *args, **kwargs):
            self.urls, self.polls = [], 0
        async def get_me(self): return {"user_id": 123}
        async def set_bot_commands(self, *args): return {"success": True}
        async def get_subscriptions(self): return {"subscriptions": [{"url": value} for value in self.urls]}
        async def subscribe_webhook(self, *args, **kwargs):
            assert adapter._polling_task is None
            self.urls = [url]
            return {"success": True}
        async def unsubscribe_webhook(self, value):
            assert value == url
            self.urls = []
            return {"success": True}
        async def get_updates(self, **kwargs):
            assert not self.urls
            self.polls += 1
            if self.polls > 1: await block.wait()
            return {"marker": 5, "updates": [first, second]}
        async def close(self): pass
    module.adapter.MaxClient = Client
    extra = module.adapter.apply_yaml_config({}, {"extra": {
        "webhook_url": url, "webhook_secret": "synthetic-secret", "webhook_auto_fallback": True,
        "webhook_auto_return": auto_return,
        "inbox_path": str(home / "inbox.db"), "marker_path": str(home / "old.db"),
        "target_path": str(home / "targets.db")}})
    adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="synthetic-offline-token", extra=extra))
    assert adapter._auto_fallback and adapter._inbox_path == str(home / "inbox.db")
    assert adapter._auto_return is auto_return
    assert isinstance(adapter, BasePlatformAdapter)
    async def probe(): return False
    adapter._probe_webhook = probe
    async def dispatch(update):
        mid = update["message"]["body"]["mid"]
        seen.append(mid)
        event = SimpleNamespace(metadata={"max_update_event_key": module.adapter.max_update_event_key(update)})
        await adapter.on_processing_start(event)
        if mid == "first":
            waiting.set()
            await finish.wait()
        else: control.set()
        await adapter.on_processing_complete(event, "success")
        return "accepted"
    adapter._dispatch_update = dispatch
    try:
        assert await adapter.connect()
        workers = list(adapter._worker_tasks)
        await adapter.handle_webhook({"X-Max-Bot-Api-Secret": "synthetic-secret"}, first)
        await asyncio.wait_for(waiting.wait(), 3)
        for _ in range(3): await adapter._failover.check()
        assert adapter._worker_tasks == workers and not adapter._webhook_mode
        await asyncio.wait_for(control.wait(), 3)
        if auto_return:
            async def healthy(): return True
            adapter._probe_webhook = healthy
            adapter._recovery_min_seconds = 0
            for _ in range(3): await adapter._check_webhook_return()
            assert adapter._webhook_mode and adapter._polling_task is None
            assert adapter._worker_tasks == workers and not finish.is_set()
            await adapter.handle_webhook({"X-Max-Bot-Api-Secret": "synthetic-secret"}, first)
        finish.set()
        for _ in range(100):
            if adapter._receiver.status_summary()["processed"] == 2: break
            await asyncio.sleep(0.01)
        assert seen == ["first", "second"] and adapter._receiver.status_summary()["processed"] == 2
        return {"native_base": BasePlatformAdapter.__module__, "same_workers": True,
                "active_task_preserved": True, "cross_transport_dedup": True,
                "delete_verified_before_polling": True, "automatic_return": auto_return,
                "model_started": False, "network": False}
    finally:
        finish.set()
        await adapter.disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-root", required=True, type=Path)
    parser.add_argument("--plugin-dir", type=Path, default=Path(__file__).resolve().parents[1] / "plugins/max")
    parser.add_argument("--auto-return", action="store_true")
    args = parser.parse_args()
    if not (args.hermes_root / "gateway/platforms/base.py").is_file():
        raise SystemExit("Настоящий Hermes обязателен для контрактной проверки")
    sys.path.insert(0, str(args.hermes_root.resolve()))
    def prohibit_network(event, arguments):
        if event in {"socket.connect", "socket.getaddrinfo", "socket.bind"}:
            raise RuntimeError("Contract prohibits network access")
    sys.addaudithook(prohibit_network)
    with tempfile.TemporaryDirectory(prefix="max-failover-contract-") as directory:
        result = asyncio.run(exercise(args.plugin_dir.resolve(), Path(directory), args.auto_return))
        from gateway import status
        flush = getattr(status, "flush_runtime_status", None)
        if callable(flush):
            assert flush(timeout=5), "Native status writer did not drain"
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

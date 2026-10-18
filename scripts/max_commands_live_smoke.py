"""Verify MAX command registration without polling or printing secrets."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Mapping

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))
try:
    import plugins as _hermes_plugins
except ImportError:
    _hermes_plugins = None
else:
    plugin_root = str(REPOSITORY_ROOT / "plugins")
    namespace_path = getattr(_hermes_plugins, "__path__", None)
    if namespace_path is not None and plugin_root not in namespace_path:
        namespace_path.append(plugin_root)

from plugins.max.adapter import MaxAdapter
from plugins.max.client import DEFAULT_API_BASE, MaxClient
from plugins.max.tls import tls_verify_from_env


def _names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        str(item.get("name"))
        for item in value
        if isinstance(item, Mapping) and item.get("name")
    ]


async def _run() -> int:
    token = os.environ.get("MAX_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("MAX_BOT_TOKEN is required")
    client = MaxClient(
        token,
        base_url=os.environ.get("MAX_API_BASE_URL", DEFAULT_API_BASE),
        verify=tls_verify_from_env(),
    )
    try:
        adapter = object.__new__(MaxAdapter)
        commands = adapter._max_commands()
        before = await client.get_me()
        patch_result = await client.set_bot_commands(commands)
        after = await client.get_me()
        print(f"local_count={len(commands)} local_names={','.join(_names(commands))}")
        print(
            "patch_status=ok "
            f"response_keys={','.join(sorted(str(key) for key in patch_result))}"
        )
        print(f"remote_before={','.join(_names(before.get('commands')))}")
        print(f"remote_after={','.join(_names(after.get('commands')))}")
    finally:
        await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))

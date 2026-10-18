"""Inspect the live MAX upload response without printing signed values."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

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

from plugins.max.client import DEFAULT_API_BASE, MaxClient
from plugins.max.tls import tls_verify_from_env


def _shape(value: Any, *, key: str = "") -> Any:
    if isinstance(value, Mapping):
        return {str(name): _shape(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, list):
        return [_shape(item, key=key) for item in value[:3]]
    if isinstance(value, (str, bytes)):
        lower_key = key.lower()
        if any(marker in lower_key for marker in ("token", "url", "sig", "secret")):
            return f"<redacted:{len(value)}>"
        return f"<{type(value).__name__}:{len(value)}>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return f"<{type(value).__name__}>"


async def _run(media_type: str) -> int:
    token = os.environ.get("MAX_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("MAX_BOT_TOKEN is required")
    client = MaxClient(
        token,
        base_url=os.environ.get("MAX_API_BASE_URL", DEFAULT_API_BASE),
        verify=tls_verify_from_env(),
    )
    try:
        upload_info = await client._request("POST", "/uploads", params={"type": media_type})
        upload_url = str(upload_info.get("url") or "")
        parsed = urlsplit(upload_url)
        print(f"upload_response_shape={json.dumps(_shape(upload_info), ensure_ascii=True, sort_keys=True)}")
        print(f"upload_url_host={parsed.hostname or ''} upload_url_has_query={bool(parsed.query)}")

        media_http = client._get_media_http()
        data = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )
        response = await media_http.post(
            upload_url,
            files={"data": ("probe.png", data, "image/png")},
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )
        try:
            payload: Any = response.json()
        except ValueError:
            payload = {"non_json_body_bytes": len(response.content)}
        print(
            f"multipart_status={response.status_code} "
            f"content_type={response.headers.get('content-type', '')}"
        )
        print(f"multipart_response_shape={json.dumps(_shape(payload), ensure_ascii=True, sort_keys=True)}")
    finally:
        await client.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--type", default="image", choices=("image", "file", "audio", "video"))
    return asyncio.run(_run(parser.parse_args().type))


if __name__ == "__main__":
    raise SystemExit(main())

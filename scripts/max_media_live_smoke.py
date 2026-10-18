"""Send one bounded image+document attachment batch through MAX.

This script uses the API client directly and never calls ``get_updates``. Do
not run it with a second polling consumer against a production bot unless the
gateway is stopped or the bot is disposable.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
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

from plugins.max.client import DEFAULT_API_BASE, MaxClient
from plugins.max.adapter import _is_media_send_retryable, _media_retry_after
from plugins.max.rate_limit import with_backoff
from plugins.max.tls import tls_verify_from_env


def _message_id(response: Mapping[str, Any]) -> str:
    message = response.get("message")
    if isinstance(message, Mapping):
        body = message.get("body")
        if isinstance(body, Mapping) and body.get("mid"):
            return str(body["mid"])
    return str(response.get("message_id") or response.get("id") or "")


async def _run(user_id: str) -> int:
    token = os.environ.get("MAX_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("MAX_BOT_TOKEN is required")
    client = MaxClient(
        token,
        base_url=os.environ.get("MAX_API_BASE_URL", DEFAULT_API_BASE),
        verify=tls_verify_from_env(),
    )
    try:
        image = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )
        document = "MAX Hermes media smoke: control text inside a document.\n".encode()
        image_upload = await client.upload_media_bytes(
            image,
            filename="тестовая-картинка.png",
            media_type="image",
            mime_type="image/png",
        )
        document_upload = await client.upload_media_bytes(
            document,
            filename="тестовый-документ.txt",
            media_type="file",
            mime_type="text/plain",
        )
        attachments = [
            {"type": "image", "payload": {"token": str(image_upload["token"])}},
            {"type": "file", "payload": {"token": str(document_upload["token"])}},
        ]
        image_response = await with_backoff(
            lambda: client.send_message(
                str(user_id),
                "MAX Hermes media smoke: изображение.",
                target_type="user",
                attachments=[attachments[0]],
            ),
            is_rate_limit=_is_media_send_retryable,
            extract_retry_after=_media_retry_after,
            max_attempts=8,
        )
        document_response = await with_backoff(
            lambda: client.send_message(
                str(user_id),
                "MAX Hermes media smoke: документ.",
                target_type="user",
                attachments=[attachments[1]],
            ),
            is_rate_limit=_is_media_send_retryable,
            extract_retry_after=_media_retry_after,
            max_attempts=8,
        )
        print(
            "media_send_status=ok "
            f"image_mid={_message_id(image_response)} "
            f"document_mid={_message_id(document_response)}"
        )
    finally:
        await client.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", required=True)
    return asyncio.run(_run(parser.parse_args().user_id))


if __name__ == "__main__":
    raise SystemExit(main())

"""Проверка медиа установленного MAX без подключения второго polling-получателя.

По умолчанию только читает inbox. --send явно разрешает отправить владельцу
четыре небольших контрольных вложения штатными методами адаптера Hermes.
Токены, переписка и подписанные URL в отчёт не попадают.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from urllib.parse import quote, urlsplit


def load_plugin(path: Path):
    name = "max_media_acceptance_plugin"
    spec = importlib.util.spec_from_file_location(
        name, path / "__init__.py", submodule_search_locations=[str(path)])
    if spec is None or spec.loader is None:
        raise RuntimeError("Не найден установленный плагин MAX")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def failure(exc: Exception) -> dict:
    return {"category": type(exc).__name__, "status": getattr(exc, "status_code", None),
            "code": getattr(exc, "code", None)}


def readback_verified(record: dict, kind: str) -> bool:
    cached = record.get("cache_contract") or {}
    if record.get("download") != "ok" or not record.get("bytes") or cached.get("count") != 1 or not cached.get("files_exist"):
        return False
    if kind == "file":
        return record.get("original_bytes_match") is True
    if kind == "image":
        return len(record.get("dimensions") or []) == 2
    return kind in {s.get("codec_type") for s in record.get("streams", [])}


def inbox_summary(path: Path) -> dict:
    if not path.is_file():
        return {"exists": False}
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1) as conn:
        states = dict(conn.execute("SELECT status, COUNT(*) FROM max_polling_inbox GROUP BY status"))
        rows = conn.execute(
            "SELECT status, payload FROM max_polling_inbox ORDER BY created_at DESC LIMIT 100").fetchall()
    media = []
    for status, payload in rows:
        update = json.loads(payload)
        message = update.get("message") or {}
        body = message.get("body") or {}
        for attachment in body.get("attachments") or []:
            data = attachment.get("payload") or {}
            url = data.get("url") or data.get("download_url") or ""
            media.append({"status": status, "timestamp": message.get("timestamp"),
                          "kind": attachment.get("type"), "keys": sorted(attachment),
                          "payload_keys": sorted(data), "host": urlsplit(url).hostname,
                          "mime": data.get("mime_type") or data.get("mime"),
                          "has_caption": bool(body.get("text"))})
    return {"exists": True, "states": states, "recent_media": media[:8]}


def make_fixtures() -> dict[str, Path]:
    from PIL import Image, ImageDraw
    from gateway.platforms.base import get_document_cache_dir

    root = Path(tempfile.mkdtemp(prefix="max-media-check-", dir=get_document_cache_dir()))
    files = {"image": root / "проверка-картинки.png", "file": root / "проверка-документа.txt",
             "audio": root / "проверка-звука.mp3", "video": root / "проверка-видео.mp4"}
    image = Image.new("RGB", (640, 360), "white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((40, 90, 180, 230), fill="red")
    draw.ellipse((240, 90, 380, 230), fill="green")
    draw.polygon(((490, 90), (420, 230), (560, 230)), fill="blue")
    draw.text((40, 25), "MAX MEDIA CHECK 20261017", fill="black")
    image.save(files["image"])
    files["file"].write_text("Контрольная строка: МАКС-7319.\nЭто тестовый документ Hermes.\n", encoding="utf-8")
    commands = [
        ["ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i", "sine=frequency=660:duration=2",
         "-codec:a", "libmp3lame", str(files["audio"])],
        ["ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=15",
         "-f", "lavfi", "-i", "sine=frequency=440", "-t", "2", "-c:v", "libx264", "-pix_fmt",
         "yuv420p", "-c:a", "aac", "-movflags", "+faststart", str(files["video"])],
    ]
    for command in commands:
        subprocess.run(command, check=True, capture_output=True, timeout=30)
    return files


async def readback(plugin, adapter, message_id: str, expected: str, original: Path) -> dict:
    client = adapter._client
    message = await client._request("GET", "/messages/" + quote(message_id, safe=""))
    message = message.get("message", message)
    attachments = (message.get("body") or {}).get("attachments") or []
    record = {"remote_message": True, "types": [a.get("type") for a in attachments]}
    found = next((a for a in attachments if a.get("type") == expected), None)
    if found is None:
        record["attachment_verified"] = False
        return record
    item = plugin.adapter.attachment_from_payload(found)
    url = item.url
    if not url and item.kind == "video" and item.token:
        url = plugin.adapter._find_download_url(await client.get_video(item.token))
    if not url:
        record["download"] = "no_url"
        return record
    data, mime = await client.download_media(url)
    record.update(download="ok", bytes=len(data), mime=mime, host=urlsplit(url).hostname)
    if expected == "file":
        record["original_bytes_match"] = hashlib.sha256(data).digest() == hashlib.sha256(original.read_bytes()).digest()
    with tempfile.TemporaryDirectory(prefix="max-media-readback-") as directory:
        path = Path(directory) / ("received" + original.suffix)
        path.write_bytes(data)
        if expected == "image":
            from PIL import Image
            with Image.open(path) as image:
                image.load()
                record["dimensions"] = list(image.size)
        if expected in {"audio", "video"}:
            result = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "stream=codec_type,codec_name",
                                     "-of", "json", str(path)], capture_output=True, check=True, timeout=15)
            record["streams"] = json.loads(result.stdout)["streams"]
    # This is a transport/cache contract, not a real owner message or LLM test.
    max_message = plugin.adapter.MaxMessage.from_update({"update_type": "message_created", "message": message})
    if max_message is not None:
        event = plugin.adapter._build_message_event(adapter, max_message)
        await adapter._populate_message_media(max_message, event)
        record["cache_contract"] = {"count": len(event.media_urls), "mime": event.media_types,
                                    "message_type": event.message_type.value,
                                    "files_exist": all(Path(p).is_file() for p in event.media_urls)}
    return record


async def run(args) -> int:
    import hermes_yaml
    from agent.secret_scope import load_env_file
    from gateway.config import PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter
    from gateway.platform_registry import PlatformEntry, platform_registry

    home = args.home.resolve()
    config = hermes_yaml.safe_load((home / "config.yaml").read_text())
    values = load_env_file(home / ".env")
    # Mirror the gateway's root .env load only in this disposable process.
    # A secondary bot still requires its own explicit token below.
    for key, value in values.items():
        if key.startswith("MAX_"):
            os.environ[key] = str(value)
    plugin = load_plugin(args.plugin_root or home / "plugins/max")
    if not issubclass(plugin.MaxAdapter, BasePlatformAdapter):
        raise RuntimeError("Плагин не использует настоящий BasePlatformAdapter Hermes")
    plugin.register(SimpleNamespace(register_platform=lambda **kw: platform_registry.register(PlatformEntry(**kw))))
    platform = config["platforms"][args.platform]
    extra = dict(platform.get("extra") or {})
    # The personal gateway resolves MAX_BOT_TOKEN from the root secret file;
    # a YAML placeholder must not override that effective credential.
    token = (values.get("MAX_BOT_TOKEN") or platform.get("token")) if args.platform == "max" else platform.get("token")
    if args.platform != "max" and not platform.get("token"):
        raise RuntimeError("Для дополнительного бота нужен собственный явно настроенный токен")
    adapter = plugin.MaxAdapter(PlatformConfig(enabled=True, token=token, extra=extra))
    report = {"platform": args.platform, "route": args.route, "native_base": BasePlatformAdapter.__module__,
              "polling_started": False, "model_started": False,
              "inbox": inbox_summary(Path(extra.get("marker_path") or home / "max/polling-marker.sqlite3"))}
    if args.send:
        if not args.owner_id or args.owner_id not in adapter._access.allowed_users or not adapter._access.can_dm(args.owner_id):
            raise RuntimeError("Получатель должен быть явно разрешён политикой этого бота")
        tls_values = dict(values)
        if extra.get("ca_bundle"):
            tls_values["MAX_CA_BUNDLE"] = extra["ca_bundle"]
        client = plugin.adapter.MaxClient(token, base_url=adapter._api_base,
                                         verify=plugin.adapter.tls_verify_from_env(tls_values))
        adapter._client = client
        try:
            await client.get_me()
            fixtures = make_fixtures() if not args.file else {str(index): path for index, path in enumerate(args.file)}
            report["outbound"] = []
            methods = {"image": adapter.send_image_file, "file": adapter.send_document,
                       "audio": adapter.send_voice, "video": adapter.send_video}
            labels = {"image": "картинка: три цветные фигуры", "file": "TXT с контрольной строкой",
                      "audio": "звук: двухсекундный сигнал", "video": "видео: движущаяся таблица и звук"}
            for fixture_key, path in fixtures.items():
                kind = ("file" if args.as_documents else plugin.adapter.media_type_for_file(path)) if args.file else fixture_key
                record = {"kind": kind, "name": path.name, "source_bytes": path.stat().st_size}
                report["outbound"].append(record)
                try:
                    caption = "Проверка MAX — " + (path.name if args.file else labels[kind])
                    if args.route == "media":
                        directive = ("[[as_document]]\n" if args.as_documents else "") + "MEDIA:" + str(path)
                        result = await asyncio.wait_for(adapter.send(args.owner_id, caption + "\n" + directive,
                            metadata={"max_target_type": "user"}), timeout=100)
                    elif args.route == "standalone":
                        with tempfile.TemporaryDirectory(prefix="max-standalone-check-") as state:
                            isolated_extra = {**extra, "target_path": str(Path(state) / "targets.sqlite3")}
                            response = await asyncio.wait_for(plugin.adapter.standalone_send(
                                PlatformConfig(enabled=True, token=token, extra=isolated_extra), args.owner_id,
                                caption, media_files=[str(path)], force_document=args.as_documents), timeout=100)
                            result = SimpleNamespace(success=response.get("success", False), message_id=response.get("message_id"))
                    else:
                        result = await asyncio.wait_for(methods[kind](args.owner_id, str(path),
                            caption=caption, metadata={"max_target_type": "user"}), timeout=100)
                    record.update(success=result.success, message_id=result.message_id,
                                  error_kind=getattr(result, "error_kind", None))
                    if result.success and result.message_id:
                        record["readback"] = await asyncio.wait_for(
                            readback(plugin, adapter, result.message_id, kind, path), timeout=90)
                        record["readback_verified"] = readback_verified(record["readback"], kind)
                except Exception as exc:
                    record["error"] = failure(exc)
                # Checkpoints prevent blindly repeating successful sends after a later failure.
                if args.receipt:
                    args.receipt.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            report["error"] = failure(exc)
        finally:
            await client.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.receipt:
        args.receipt.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return int(bool(report.get("error")) or any(not r.get("success") or not r.get("readback_verified") or r.get("error")
                                               for r in report.get("outbound", [])))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--plugin-root", type=Path)
    parser.add_argument("--platform", default="max")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--owner-id")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--file", type=Path, action="append", default=[], help="Вместо набора: файл из разрешённого кеша Hermes")
    parser.add_argument("--as-documents", action="store_true", help="Отправить --file оригиналом-документом")
    parser.add_argument("--route", choices=("direct", "media", "standalone"), default="direct")
    args = parser.parse_args()
    if args.as_documents and not args.file:
        parser.error("--as-documents требует явно указанных --file")
    if args.send and (args.receipt is None or args.receipt.exists()):
        parser.error("Для отправки нужен новый --receipt; существующая квитанция защищает от повторной отправки")
    if args.send:
        try:
            with args.receipt.open("x", encoding="utf-8") as stream:
                json.dump({"state": "started", "platform": args.platform, "polling_started": False}, stream)
        except FileExistsError:
            parser.error("Эта проверка уже запускалась; квитанция защищает от повторной отправки")
    sys.dont_write_bytecode = True
    os.environ.setdefault("HERMES_HOME", str(args.home))
    logging.disable(logging.CRITICAL)
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())

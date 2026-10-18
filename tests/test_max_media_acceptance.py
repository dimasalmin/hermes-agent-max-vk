"""The live harness must not confuse an API acknowledgement with usable media."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sqlite3
import json

import pytest

spec = importlib.util.spec_from_file_location(
    "max_media_acceptance", Path(__file__).resolve().parents[1] / "scripts/max_media_acceptance.py")
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


@pytest.mark.parametrize("kind", ["image", "file", "audio", "video"])
def test_api_creation_alone_is_not_media_acceptance(kind):
    assert not acceptance.readback_verified({"remote_message": True, "types": [kind]}, kind)


@pytest.mark.parametrize("kind", ["image", "file", "audio", "video"])
def test_download_without_native_cache_contract_is_not_accepted(kind):
    assert not acceptance.readback_verified({"download": "ok", "bytes": 123}, kind)


def test_corrupted_document_is_not_accepted():
    record = {"download": "ok", "bytes": 123, "original_bytes_match": False,
              "cache_contract": {"count": 1, "files_exist": True}}
    assert not acceptance.readback_verified(record, "file")
    record["original_bytes_match"] = True
    assert acceptance.readback_verified(record, "file")


def test_thumbnail_is_not_accepted_as_video():
    record = {"download": "ok", "bytes": 123, "dimensions": [100, 100],
              "cache_contract": {"count": 1, "files_exist": True}}
    assert not acceptance.readback_verified(record, "video")
    record["streams"] = [{"codec_type": "video", "codec_name": "h264"}]
    assert acceptance.readback_verified(record, "video")


def test_inbox_summary_does_not_leak_caption_token_or_signed_url(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    payload = {"message": {"body": {"text": "private-caption", "attachments": [{"type": "image",
        "payload": {"url": "https://i.oneme.ru/image?sig=private-signature", "token": "private-token"}}]}}}
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE max_polling_inbox(status TEXT, payload TEXT, created_at REAL)")
        conn.execute("INSERT INTO max_polling_inbox VALUES (?, ?, ?)", ("processed", json.dumps(payload), 1))
    report = json.dumps(acceptance.inbox_summary(path))
    assert "private-" not in report
    assert "i.oneme.ru" in report

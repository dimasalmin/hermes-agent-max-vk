import asyncio
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

SPEC = importlib.util.spec_from_file_location(
    "edge_tts_command", Path(__file__).parents[1] / "scripts/edge_tts_command.py")
provider = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(provider)


@pytest.mark.parametrize("speed,rate", [(1, "+0%"), (1.25, "+25%"), (0.25, "-75%"), (4, "+300%")])
def test_speed(speed, rate):
    assert provider.rate_for_speed(speed) == rate


@pytest.mark.parametrize("speed", [0, -1, 4.1, float("nan"), float("inf")])
def test_invalid_speed(speed):
    with pytest.raises(ValueError):
        provider.rate_for_speed(speed)


def test_text_is_file_not_shell(tmp_path):
    text = "Привет; $(touch forbidden) ' \" {output_path}"
    file = tmp_path / "текст с пробелами.txt"
    file.write_text(text, encoding="utf-8")
    assert provider.read_text(file) == text
    file.write_bytes(b"a" * (provider.MAX_TEXT_BYTES + 1))
    with pytest.raises(ValueError):
        provider.read_text(file)
    file.write_text(" \n", encoding="utf-8")
    with pytest.raises(ValueError):
        provider.read_text(file)


def fake_edge(monkeypatch, save, version="7.3.0"):
    monkeypatch.setattr(provider, "version", lambda _: version)
    monkeypatch.setitem(sys.modules, "edge_tts", SimpleNamespace(
        Communicate=lambda text, **kwargs: SimpleNamespace(save=lambda path: save(text, kwargs, path))))


def test_atomic_output_and_voice(monkeypatch, tmp_path):
    seen = []
    async def save(text, kwargs, path):
        seen.append((text, kwargs))
        Path(path).write_bytes(b"ID3test")
    fake_edge(monkeypatch, save)
    out = tmp_path / "голос.mp3"
    asyncio.run(provider.synthesize("Контроль", out, "ru-RU-SvetlanaNeural", 1.25, 5))
    assert out.read_bytes() == b"ID3test"
    assert seen == [("Контроль", {"voice": "ru-RU-SvetlanaNeural", "rate": "+25%"})]
    assert list(tmp_path.iterdir()) == [out]


@pytest.mark.parametrize("failure", ["error", "timeout", "empty"])
def test_partial_cleanup(monkeypatch, tmp_path, failure):
    async def save(text, kwargs, path):
        if failure == "empty":
            Path(path).touch()
            return
        Path(path).write_bytes(b"partial")
        if failure == "timeout":
            await asyncio.sleep(1)
        raise RuntimeError("sensitive URL and speech")
    fake_edge(monkeypatch, save)
    with pytest.raises((RuntimeError, asyncio.TimeoutError)):
        asyncio.run(provider.synthesize("Контроль", tmp_path / "speech.mp3", "voice", 1, 0.01))
    assert not list(tmp_path.iterdir())


def test_wrong_version_and_container(monkeypatch, tmp_path):
    monkeypatch.setattr(provider, "version", lambda _: "7.2.7")
    with pytest.raises(RuntimeError):
        asyncio.run(provider.synthesize("test", tmp_path / "speech.mp3", "voice", 1, 5))
    with pytest.raises(ValueError):
        asyncio.run(provider.synthesize("test", tmp_path / "speech.ogg", "voice", 1, 5))


def test_sanitized_failure(monkeypatch, tmp_path, capsys):
    async def save(*args):
        raise RuntimeError("SECRET_SIGNED_URL")
    fake_edge(monkeypatch, save)
    source = tmp_path / "text.txt"
    source.write_text("PRIVATE_TEXT", encoding="utf-8")
    assert provider.main(["--input", str(source), "--output", str(tmp_path / "out.mp3"),
                          "--format", "mp3", "--voice", "voice", "--speed", "1"]) == 1
    output = capsys.readouterr()
    assert not output.out
    assert "SECRET" not in output.err and "PRIVATE_TEXT" not in output.err

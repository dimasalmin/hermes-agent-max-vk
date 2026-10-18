"""External Edge TTS command provider; run with an isolated, pinned interpreter."""

from __future__ import annotations

import argparse
import asyncio
from importlib.metadata import version
import math
from pathlib import Path
import sys
import tempfile

EDGE_VERSION = "7.3.0"
MAX_TEXT_BYTES = 100_000


def read_text(path: Path) -> str:
    with path.open("rb") as stream:
        raw = stream.read(MAX_TEXT_BYTES + 1)
    if len(raw) > MAX_TEXT_BYTES:
        raise ValueError("input_size")
    text = raw.decode("utf-8")
    if not text.strip():
        raise ValueError("empty_input")
    return text


def rate_for_speed(speed: float) -> str:
    if not math.isfinite(speed) or not 0.25 <= speed <= 4.0:
        raise ValueError("invalid_speed")
    return f"{round((speed - 1.0) * 100):+d}%"


async def synthesize(text: str, output: Path, voice: str, speed: float, timeout: float) -> None:
    rate = rate_for_speed(speed)
    if output.suffix.lower() != ".mp3" or not voice.strip():
        raise ValueError("invalid_output_or_voice")
    if not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise ValueError("invalid_timeout")
    if version("edge-tts") != EDGE_VERSION:
        raise RuntimeError("unsupported_edge_version")
    import edge_tts

    # Never publish a partial download, and never label MP3 bytes as Ogg.
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".edge-", dir=output.parent) as temporary:
        partial = Path(temporary) / "speech.mp3"
        await asyncio.wait_for(
            edge_tts.Communicate(text, voice=voice, rate=rate).save(str(partial)),
            timeout=timeout,
        )
        if not partial.is_file() or partial.stat().st_size == 0:
            raise RuntimeError("empty_audio")
        partial.chmod(0o600)
        partial.replace(output)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Внешний командный провайдер Edge TTS 7.3.0")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--format", required=True, choices=("mp3",))
    parser.add_argument("--voice", required=True)
    parser.add_argument("--speed", required=True, type=float)
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args(argv)
    try:
        if args.input.resolve() == args.output.resolve():
            raise ValueError("input_equals_output")
        asyncio.run(synthesize(read_text(args.input), args.output, args.voice, args.speed, args.timeout))
    except Exception:
        # Exceptions from remote services may include text or signed URLs.
        print("Не удалось создать речь через Edge TTS; проверьте сеть, версию и параметры.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

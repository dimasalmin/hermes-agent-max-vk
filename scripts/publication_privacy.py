"""Check tracked publication text; report locations/categories, never matched values."""
from __future__ import annotations

import argparse
import ipaddress
from pathlib import Path
import re
import subprocess

RULES = {
    "private-home-path": re.compile(r"/(?:home|Users)/(?!runner(?:/|\b)|user(?:/|\b)|example(?:/|\b)|hermes(?:/|\b))([A-Za-z0-9_.-]+)/"),
    "windows-user-path": re.compile(r"(?i)[a-z]:[\\/]Users[\\/](?!user[\\/]|example[\\/])[^\\/\s]+[\\/]"),
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "provider-token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,}|sk-[A-Za-z0-9_-]{30,})\b"),
}
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
DOCUMENTATION_NETS = tuple(ipaddress.ip_network(value) for value in
                          ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"))
FORBIDDEN_SUFFIXES = {".sqlite", ".sqlite3", ".db", ".key", ".p12", ".pfx", ".har", ".bundle"}


def scan_text(text: str, private_patterns: tuple[str, ...] = ()) -> list[tuple[int, str]]:
    hits = []
    for name, pattern in RULES.items():
        hits.extend((text.count("\n", 0, match.start()) + 1, name) for match in pattern.finditer(text))
    for value in private_patterns:
        for number, line in enumerate(text.splitlines(), 1):
            if value.casefold() in line.casefold(): hits.append((number, "local-private-pattern"))
    for match in IPV4.finditer(text):
        try: address = ipaddress.ip_address(match.group())
        except ValueError: continue
        if address.is_loopback or address.is_unspecified or any(address in net for net in DOCUMENTATION_NETS): continue
        if re.search(r"(?:Chrome|Firefox|Version)/$", text[max(0, match.start()-12):match.start()]): continue
        hits.append((text.count("\n", 0, match.start()) + 1, "non-documentation-ip"))
    return sorted(set(hits))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    def git(*arguments: str) -> bytes:
        return subprocess.check_output(["git", "-C", str(root), *arguments])
    private_file = root / ".publication-private-patterns"
    private_patterns = tuple(line.strip() for line in private_file.read_text().splitlines()
                             if line.strip() and not line.startswith("#")) if private_file.exists() else ()
    selected_ref = git("write-tree").decode().strip() if args.staged else args.ref
    records = git("ls-tree", "-rz", selected_ref).split(b"\0")
    violations = 0
    for record in records:
        if not record: continue
        metadata, raw_path = record.split(b"\t", 1)
        _, kind, sha = metadata.split()
        if kind != b"blob": continue
        path = raw_path.decode("utf-8")
        name = Path(path).name
        if (name == ".env" or name.startswith(".env.") and name != ".env.example"
                or Path(path).suffix in FORBIDDEN_SUFFIXES or name == ".publication-private-patterns"):
            print(f"{path}: forbidden-publication-file")
            violations += 1
        raw = git("cat-file", "blob", sha.decode())
        if b"\0" in raw: continue
        for number, category in scan_text(raw.decode("utf-8", errors="replace"), private_patterns):
            print(f"{path}:{number}: {category}")
            violations += 1
    print(f"Publication privacy: {'FAILED' if violations else 'passed'}; findings={violations}")
    return bool(violations)


if __name__ == "__main__": raise SystemExit(main())

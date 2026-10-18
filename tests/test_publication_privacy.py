import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("publication_privacy", Path(__file__).parents[1] / "scripts/publication_privacy.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_private_path_is_reported_without_value():
    path = "/home/" + "synthetic-person" + "/notes"
    assert module.scan_text(path) == [(1, "private-home-path")]


def test_public_documentation_and_browser_versions_are_allowed():
    assert module.scan_text("https://example.com 203.0.113.10 127.0.0.1 Chrome/120.0.0.0 /home/user/cache") == []


def test_local_private_pattern_is_not_printed():
    assert module.scan_text("synthetic nickname", ("synthetic nickname",)) == [(1, "local-private-pattern")]


def test_private_network_address_is_flagged():
    text = ".".join(map(str, (10, 20, 30, 40)))
    assert module.scan_text(text) == [(1, "non-documentation-ip")]

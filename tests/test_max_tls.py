from __future__ import annotations

import ssl

import pytest

from plugins.max import tls as tls_module
from plugins.max.tls import tls_verify_from_env


def test_tls_defaults_to_system_verification() -> None:
    assert tls_verify_from_env({}) is True


def test_tls_accepts_existing_explicit_bundle(tmp_path, monkeypatch) -> None:
    bundle = tmp_path / "max-ca.pem"
    bundle.write_text(
        "-----BEGIN CERTIFICATE-----\n" + "A" * 32 + "\n-----END CERTIFICATE-----\n",
        encoding="ascii",
    )

    class _Context:
        check_hostname = True
        verify_mode = ssl.CERT_REQUIRED

        def __init__(self) -> None:
            self.loaded = None

        def load_verify_locations(self, *, cafile):
            self.loaded = cafile

    context = _Context()
    monkeypatch.setattr(tls_module.ssl, "create_default_context", lambda: context)

    result = tls_verify_from_env({"MAX_CA_BUNDLE": str(bundle)})
    assert result is context
    assert context.loaded == str(bundle)


def test_tls_rejects_disabled_verification() -> None:
    with pytest.raises(ValueError, match="cannot be disabled"):
        tls_verify_from_env({"MAX_CA_BUNDLE": "false"})


def test_tls_rejects_missing_bundle() -> None:
    with pytest.raises(ValueError, match="does not exist"):
        tls_verify_from_env({"MAX_CA_BUNDLE": "missing.pem"})


def test_tls_rejects_empty_pem_bundle(tmp_path) -> None:
    bundle = tmp_path / "empty.pem"
    bundle.write_text(
        "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n", encoding="ascii"
    )

    with pytest.raises(ValueError, match="empty"):
        tls_verify_from_env({"MAX_CA_BUNDLE": str(bundle)})

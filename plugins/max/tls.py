"""TLS policy for MAX API connections."""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from typing import Mapping


def tls_verify_from_env(env: Mapping[str, str] | None = None) -> bool | ssl.SSLContext:
    """Return a verified httpx CA setting without allowing insecure mode.

    ``True`` delegates to the host trust store.  ``MAX_CA_BUNDLE`` adds a
    deployment-managed PEM bundle to a context that already contains the
    host roots.  MAX API and upload CDN endpoints can use different public
    trust chains, so replacing the system roots with a small custom bundle is
    not sufficient.
    """

    values = env if env is not None else os.environ
    raw = str(values.get("MAX_CA_BUNDLE", "")).strip()
    if not raw or raw.lower() in {"system", "default", "true", "1", "yes"}:
        return True
    if raw.lower() in {"false", "0", "no", "insecure", "none"}:
        raise ValueError("MAX TLS verification cannot be disabled")
    path = Path(raw).expanduser()
    if not path.is_file():
        raise ValueError(f"MAX CA bundle does not exist: {path}")
    try:
        pem = path.read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"MAX CA bundle cannot be read as ASCII PEM: {path}") from exc
    begin = "-----BEGIN CERTIFICATE-----"
    end = "-----END CERTIFICATE-----"
    if begin not in pem or end not in pem:
        raise ValueError(f"MAX CA bundle is not a PEM certificate bundle: {path}")
    for block in pem.split(begin)[1:]:
        body = block.split(end, 1)[0].strip()
        if not body:
            raise ValueError(f"MAX CA bundle contains an empty certificate: {path}")
    context = ssl.create_default_context()
    try:
        context.load_verify_locations(cafile=str(path))
    except ssl.SSLError as exc:
        raise ValueError(f"MAX CA bundle contains invalid certificates: {path}") from exc
    return context

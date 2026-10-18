# MAX certificates and TLS diagnosis

The plugin keeps TLS verification enabled. `MAX_CA_BUNDLE` is an optional
per-client PEM bundle and must be managed outside this repository.

## Policy

- Do not set `MAX_CA_BUNDLE` to `false`, `0`, `none` or `insecure`.
- Do not use `verify=False` in a local smoke or deployment.
- A configured bundle must exist, be readable as ASCII PEM, and contain a
  non-empty `CERTIFICATE` block.
- The plugin starts with the host trust store and adds certificates from
  `MAX_CA_BUNDLE`; a MAX-only bundle is therefore valid. Keep the bundle
  deployment-managed and limited to the current official MAX chain.
- Record the bundle path, SHA-256 fingerprint, date and source in the operator
  change log. Do not commit the bundle itself.

## Diagnosis order

Run checks from the Hermes host and classify the first failing layer:

1. DNS resolution of `platform-api2.max.ru`.
2. TCP connection to port 443.
3. TLS handshake with the system trust store.
4. TLS handshake with `MAX_CA_BUNDLE`.
5. HTTPS `/me`, distinguishing `401` from transport failure.
6. Webhook subscription and delivery, if Webhook mode is configured.

A `401` without a token is evidence that DNS/TCP/HTTP reached the endpoint; it
does not validate the bot token or prove that a mobile-network restriction will
allow the endpoint.

## Deployment example

```env
MAX_CA_BUNDLE=/etc/hermes/max-ca-bundle.pem
```

The same policy applies to the optional Webhook ingress. TLS termination for
public port 443 belongs to the reverse proxy; the ingress should receive only
traffic from that proxy or a network boundary with equivalent controls.

## 2026-08-08 host evidence

The WSL system trust store did not contain the MAX chain. The endpoint served
`*.max.ru -> Russian Trusted Sub CA -> Russian Trusted Root CA`. A per-client
bundle built from the official Russian certificates passed the TLS handshake
and MAX `/me` returned HTTP 200. The observed SHA-256 fingerprints were:

```text
Russian Trusted Sub CA:  BB:BD:E2:10:3E:79:0B:99:9E:C6:2B:D0:3C:F6:25:A5:A2:E7:C3:16:E1:0A:FE:6A:49:0E:ED:EA:D8:B3:FD:9B
Russian Trusted Root CA: D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31
```

This validates the current host path only; it is not a universal network or
mobile-operator availability result.

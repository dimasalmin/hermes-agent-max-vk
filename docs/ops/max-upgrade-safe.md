# Upgrade-safe MAX plugin operations

## Boundary

Hermes core and the MAX integration have independent lifecycles:

```text
Hermes installation  ->  ~/.hermes/hermes-agent or packaged release
MAX plugin repository ->  ~/.hermes/plugins/max
MAX configuration     ->  ~/.hermes/config.yaml and environment/secrets
```

The MAX plugin must not be copied into Hermes `gateway/`, `agent/`,
`hermes_cli/`, or built-in `plugins/platforms/`. This preserves normal Hermes
upgrade, rollback and release switching.

## Before a Hermes upgrade

1. Run the plugin test suite from this repository:

   ```bash
   python -m pytest -q
   ```

2. Run the read-only loader smoke against the target Hermes release:

   ```bash
   python3 scripts/max_loader_smoke.py \
     --hermes-root /home/user/.hermes/hermes-agent
   ```

3. Record the Hermes version, plugin version, Python version, API base URL,
   CA bundle path and the smoke output in the change log.

4. Check `hermes gateway status`. Do not restart the active gateway merely to
   test import compatibility.

## Applying an upgrade

- Keep the current plugin junction/symlink unchanged while installing the new
  Hermes release.
- Run the loader smoke against the new release path.
- Restart Hermes only during the planned maintenance window.
- Verify `/me` and one disposable MAX message after restart.
- Confirm the gateway logs identify the expected plugin and transport.

## Rollback

1. Stop or drain the gateway using the normal Hermes command.
2. Retarget only `~/.hermes/plugins/max` to the previous plugin directory, or
   remove the plugin junction temporarily.
3. Start the previous Hermes release with its unchanged config.
4. Do not delete `~/.hermes`, session data, memory, or Hermes source files.
5. Record the failure category separately: plugin import, TLS, MAX auth,
   Webhook subscription, transport delivery, model execution, or outbound send.

## Secrets and TLS

- Keep `MAX_BOT_TOKEN` and `MAX_WEBHOOK_SECRET` outside the repository.
- Never put tokens into `config.yaml` committed to source control.
- Keep TLS verification enabled. `MAX_CA_BUNDLE` may point to a deployment-
  managed PEM bundle; a missing bundle fails closed.
- Rotate the MAX token independently from Hermes upgrades.

## Current release limitation

The plugin does not start a public HTTP listener inside the Hermes gateway.
Production Webhook uses the separately managed
`scripts/max_webhook_server.py` or another HTTPS/443 ingress. Both processes
must use the same `MAX_INBOX_PATH`; the ingress only validates and durably
stores updates, while Hermes performs dispatch and model execution.
Development Long Polling remains the simplest transport until the ingress has
passed a live field test.

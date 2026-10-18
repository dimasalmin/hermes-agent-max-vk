# Changelog

## 0.1.0 - 2026-08-09

- First public packaging of the external MAX and VK Hermes platform plugins.
- Added deterministic contract coverage for API clients, callbacks, media,
  allowlists, durable state, and standalone VK export.
- Local verification: 133 tests passed; live MAX/VK acceptance remains open.
- Added public installation, security, contribution, and CI documentation.

## Unreleased

- Добавлены inline-кнопки MAX для уточнений Hermes, подтверждений exec и
  slash-команд.
- Добавлен двухшаговый выбор провайдера и модели через `/model`.
- Добавлено подтверждение callback через `/answers`, привязка к пользователю и
  чату, срок действия и защита от повторного нажатия.
- Добавлено ограниченное кеширование входящих вложений, исходящие загрузки
  `MEDIA:`, standalone-доставка и настройка `MAX_MEDIA_MAX_BYTES`.
- Добавлены нативные методы MAX для изображений, документов, аудио и видео,
  обработка upload-токенов, группировка медиа, разрешение video-токенов и
  отчёт о частичных ошибках внешнего плагина.
- Добавлены `/menu`, `/start`, `/commands` и `/maxstatus`; при скрытом
  системном меню список команд возвращается обычным текстом.
- Добавлена обезличенная проверка `max_commands_live_smoke.py` и запись в
  журнал точного списка команд, зарегистрированного через `PATCH /me/commands`.
- Реализация остаётся внешним плагином; ядро Hermes не изменяется.

## 0.2.0 - 2026-08-08

- Rebuilt MAX integration as an external Hermes plugin using the current v0.20.0
  adapter contract.
- Added direct MAX Bot API v2 client, Long Polling marker persistence, Webhook
  secret validation, durable SQLite inbox and optional standalone ASGI ingress.
- Added DM/group allowlist policy, MAX reply `link.mid`, persistent target type,
  TLS fail-closed checks and global/per-dialog rate limiting.
- Marked live media acceptance, streaming edit coalescing, health metrics and
  live no-VPN validation as remaining release gates.

This release does not modify Hermes core and is not a claim of universal MAX
availability during regional network restrictions.

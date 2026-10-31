"""MAX Bot API v2 platform adapter for Hermes Agent.

This module is intentionally a third-party plugin boundary.  It does not add
MAX to Hermes core, so upgrading Hermes replaces the core installation without
overwriting this adapter.  Transport is handled by :mod:`.client`; the
adapter only translates MAX events into Hermes' normalized event contract.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from secrets import token_urlsafe
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import unquote, urlsplit

from .command_ui import COMMAND_CONTEXT, choice_label, russian_commands


def _fallback_validate_media_delivery_path(path: str) -> Optional[str]:
    """Validate a local file when Hermes is not installed for standalone tests."""

    try:
        candidate = Path(path).expanduser().resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return None
    return str(candidate) if candidate.is_file() else None


def _max_env(name: str, default: Any = None) -> Any:
    try:
        from gateway.platforms._shared import get_scoped_secret
    except ImportError:
        return os.environ.get(name, default)
    return get_scoped_secret(name, default)


def _max_home() -> Path:
    try:
        from hermes_constants import get_hermes_home
    except ImportError:
        return Path(os.environ.get("HERMES_HOME", "~/.hermes")).expanduser()
    return get_hermes_home()


try:
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.base import (
        BasePlatformAdapter,
        MessageEvent,
        MessageType,
        SendResult,
    )
except ImportError:  # pragma: no cover - used only by standalone unit tests
    @dataclass
    class _FallbackMessageEvent:
        text: str
        message_type: Any = None
        source: Any = None
        raw_message: Any = None
        message_id: Optional[str] = None
        reply_to_message_id: Optional[str] = None
        media_urls: List[str] = None
        media_types: List[str] = None
        metadata: Dict[str, Any] = None

    @dataclass
    class _FallbackSendResult:
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None
        raw_response: Any = None
        retryable: bool = False
        retry_after: Optional[float] = None
        continuation_message_ids: tuple = ()
        error_kind: Optional[str] = None

    class _FallbackBase:
        def __init__(self, config: Any, platform: Any) -> None:
            self.config = config
            self.platform = platform
            self._running = False

        def build_source(self, **kwargs: Any) -> Any:
            return SimpleNamespace(platform=self.platform, **kwargs)

        def _mark_connected(self) -> None:
            self._running = True

        def _mark_disconnected(self) -> None:
            self._running = False

        async def handle_message(self, _event: Any) -> None:
            return None

        def set_message_handler(self, handler) -> None:
            self._message_handler = handler

        @staticmethod
        def validate_media_delivery_path(
            path: str, session_key: str = ""
        ) -> Optional[str]:
            del session_key
            return _fallback_validate_media_delivery_path(path)

    BasePlatformAdapter = _FallbackBase  # type: ignore[misc,assignment]
    Platform = None  # type: ignore[assignment]
    PlatformConfig = Any  # type: ignore[assignment,misc]
    MessageEvent = _FallbackMessageEvent  # type: ignore[assignment]
    MessageType = SimpleNamespace(
        TEXT="text", COMMAND="command", PHOTO="photo", VIDEO="video", AUDIO="audio",
        VOICE="voice", DOCUMENT="document"
    )
    SendResult = _FallbackSendResult  # type: ignore[assignment]

from .client import DEFAULT_API_BASE, DEFAULT_MEDIA_MAX_BYTES, MaxApiError, MaxClient
from .common import AccessPolicy, split_message
from .interactive import MaxCallbackStore, build_inline_keyboard
from .media import (
    OUTBOUND_MEDIA_HOSTS,
    attachment_from_payload,
    is_allowed_outbound_media_url,
    media_type_for_file,
    mime_type_for_file,
    validate_image_dimensions,
)
from .models import MaxCallback, MaxMessage
from .polling_state import MaxTargetStore, PollingMarkerStore, max_update_event_key
from .rate_limit import MAX_MESSAGE_LENGTH, MaxRateLimiter, with_backoff
from .rate_limit import MAX_ATTACHMENTS_PER_MESSAGE
from .tls import tls_verify_from_env
from .webhook import MaxWebhookReceiver, WebhookResult
from .failover import PollingFailover, WebhookPollingStore

logger = logging.getLogger(__name__)
UNSUPPORTED_AUDIO_FALLBACK_MESSAGE = (
    "Аудиоформат не поддержан MAX; исходный файл отправлен как документ."
)

PLATFORM_NAME = "max"
PLATFORM_LABEL = "MAX Messenger"
PLATFORM_EMOJI = "💬"
PLATFORM_HINT = (
    "You are chatting via MAX. Keep a response within MAX's 4000-character message limit. "
    "Use Markdown only where it improves readability; availability depends on the configured "
    "MAX client and network, not on a universal whitelist guarantee."
)
MAX_UPDATE_TYPES = (
    "message_created",
    "message_callback",
    "bot_started",
    "bot_stopped",
    "dialog_removed",
)
GROUP_ADMIN_COMMANDS = frozenset(
    {
        "new", "stop", "model", "compress", "undo", "rollback", "sethome",
        "restart", "approve", "deny", "background", "queue", "resume", "reasoning", "goal",
    }
)
GROUP_ADMIN_CALLBACKS = frozenset({"approval", "slash", "model", "choice"})
GROUP_SESSION_SEPARATOR = "::user::"


def _is_max_rate_limit(exc: BaseException) -> bool:
    return getattr(exc, "status_code", None) == 429 or getattr(exc, "code", None) in {
        "too_many_requests",
        "rate_limit",
    }


def _retry_after(exc: BaseException) -> Optional[float]:
    value = getattr(exc, "retry_after", None)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_media_send_retryable(exc: BaseException) -> bool:
    return _is_max_rate_limit(exc) or getattr(exc, "code", None) in {
        "attachment.not.ready",
        "attachment_not_ready",
    }


def _media_retry_after(exc: BaseException) -> Optional[float]:
    retry_after = _retry_after(exc)
    if retry_after is not None:
        return retry_after
    if getattr(exc, "code", None) in {"attachment.not.ready", "attachment_not_ready"}:
        return 1.0
    return None


def _is_retryable_media_error(exc: BaseException) -> bool:
    return _is_media_send_retryable(exc) or bool(getattr(exc, "retryable", False))


def _is_unsupported_audio_error(exc: MaxApiError) -> bool:
    if exc.status_code == 415:
        return True
    details = f"{exc.code or ''} {exc}".lower().replace("_", " ").replace("-", " ")
    return any(
        marker in details
        for marker in (
            "file extension forbidden",
            "file extension is forbidden",
            "unsupported audio format",
            "unsupported audio",
            "unsupported media format",
            "unsupported file type",
            "unsupported format",
            "invalid audio format",
        )
    )


def _safe_https_url(value: str) -> bool:
    try:
        parsed = urlsplit(str(value).strip())
        if parsed.scheme.lower() != "https" or not parsed.hostname:
            return False
        if parsed.username or parsed.password:
            return False
        return parsed.port in (None, 443)
    except ValueError:
        return False


def _find_download_url(value: Any) -> Optional[str]:
    """Select a video file, never its thumbnail or an HLS/DASH playlist."""

    if isinstance(value, str) and _safe_https_url(value):
        if Path(urlsplit(value).path).suffix.lower() in {
            ".jpg", ".jpeg", ".png", ".webp", ".gif", ".m3u8", ".mpd",
        }:
            return None
        return value
    if isinstance(value, Mapping):
        preferred = ("urls", "mp4", "mp4_720", "mp4_480", "mp4_360", "mp4_240",
                     "mp4_144", "download_url", "download", "url", "stream")
        for key in preferred:
            found = _find_download_url(value.get(key))
            if found:
                return found
        for key, nested in value.items():
            if key in preferred or str(key).lower() in {"thumbnail", "preview", "poster", "hls", "dash", "live_hls"}:
                continue
            found = _find_download_url(nested)
            if found:
                return found
    elif isinstance(value, (list, tuple)):
        for nested in value:
            found = _find_download_url(nested)
            if found:
                return found
    return None


def _message_type(message: MaxMessage) -> Any:
    if _is_command(message.text):
        return getattr(MessageType, "COMMAND", getattr(MessageType, "TEXT", None))
    for attachment in message.attachments:
        kind = str(attachment.get("type") or "").lower()
        if kind == "image":
            return getattr(MessageType, "PHOTO", getattr(MessageType, "TEXT", None))
        if kind in {"audio", "voice"}:
            # MAX exposes voice recordings as audio attachments. Hermes AUDIO
            # explicitly bypasses STT; keep file uploads as DOCUMENT instead.
            return getattr(MessageType, "VOICE", getattr(MessageType, "AUDIO", getattr(MessageType, "TEXT", None)))
        if kind == "video":
            return getattr(MessageType, "VIDEO", getattr(MessageType, "DOCUMENT", getattr(MessageType, "TEXT", None)))
        if kind == "file":
            return getattr(MessageType, "DOCUMENT", getattr(MessageType, "TEXT", None))
    return getattr(MessageType, "TEXT", None)


def _reply_message_id(link: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not link:
        return None
    for key in ("mid", "message_id"):
        value = link.get(key)
        if isinstance(value, (str, int)) and value:
            return str(value)
    linked_message = link.get("message") or link.get("linked_message")
    if isinstance(linked_message, Mapping):
        body = linked_message.get("body")
        if isinstance(body, Mapping) and body.get("mid"):
            return str(body["mid"])
        for key in ("mid", "message_id"):
            if linked_message.get(key):
                return str(linked_message[key])
    elif isinstance(linked_message, (str, int)) and linked_message:
        return str(linked_message)
    return None


def _is_reply_to_bot(
    link: Optional[Mapping[str, Any]], bot_user_id: Optional[str]
) -> bool:
    if not link or not bot_user_id or str(link.get("type") or "").lower() != "reply":
        return False
    linked_message = link.get("message") or link.get("linked_message")
    candidates = [link]
    if isinstance(linked_message, Mapping):
        candidates.append(linked_message)
        body = linked_message.get("body")
        if isinstance(body, Mapping):
            candidates.append(body)
    for candidate in candidates:
        for sender_key in ("sender", "user", "author"):
            sender = candidate.get(sender_key)
            if not isinstance(sender, Mapping):
                continue
            if bool(sender.get("is_bot")) or str(sender.get("user_id") or "") == str(bot_user_id):
                return True
    return False


def _reply_link(reply_to: Optional[str]) -> Optional[dict[str, str]]:
    return {"type": "reply", "mid": str(reply_to)} if reply_to else None


def _append_event_note(existing: Optional[str], note: str) -> str:
    if not note:
        return existing or ""
    if not existing:
        return note
    return f"{existing}\n\n{note}"


def _cache_media_bytes(
    data: bytes,
    *,
    filename: str,
    mime_type: str,
    default_kind: str,
) -> Any:
    """Delegate media persistence to Hermes without importing it at module load."""

    from gateway.platforms.base import cache_media_bytes

    return cache_media_bytes(
        data,
        filename=filename,
        mime_type=mime_type,
        default_kind=default_kind,
    )


def _build_message_event(
    adapter: "MaxAdapter",
    message: MaxMessage,
    *,
    text: Optional[str] = None,
    media_urls: Optional[List[str]] = None,
    media_types: Optional[List[str]] = None,
    update_event_key: Optional[str] = None,
) -> Any:
    """Translate a normalized MAX message through Hermes' public contract."""

    chat_type = "group" if message.is_group else "dm"
    session_chat_id = message.chat_id
    if message.is_group:
        session_chat_id = f"{message.chat_id}{GROUP_SESSION_SEPARATOR}{message.user_id}"
    source = adapter.build_source(
        chat_id=session_chat_id,
        chat_name=message.chat_title or message.user_name,
        chat_type=chat_type,
        user_id=message.user_id,
        user_name=message.user_name,
        message_id=message.message_id,
        role_authorized=False,
    )
    return MessageEvent(
        text=message.text if text is None else text,
        message_type=_message_type(message),
        source=source,
        raw_message=message.raw_message,
        message_id=message.message_id,
        reply_to_message_id=(
            None if message.link and message.link.get("type") == "forward" else _reply_message_id(message.link)
        ),
        media_urls=list(media_urls or []),
        media_types=list(media_types or []),
        metadata={
            "max_chat_type": message.chat_type,
            "max_chat_id": message.chat_id,
            "max_target_type": "chat" if message.is_group else "user",
            "max_user_id": message.user_id,
            "max_forwarded": bool(message.link and message.link.get("type") == "forward"),
            "max_request_version": getattr(adapter, "_choice_versions", {}).get((message.chat_id, message.user_id)),
            **({"max_update_event_key": update_event_key} if update_event_key else {}),
        },
    )


def _is_command(text: str) -> bool:
    return bool(text) and text.lstrip().startswith("/")


def _command_name(text: str) -> str:
    if not _is_command(text):
        return ""
    return text.lstrip().split(None, 1)[0].split("@", 1)[0].lstrip("/").lower()


def _transport_chat_id(
    chat_id: str,
    metadata: Optional[Mapping[str, Any]] = None,
) -> str:
    if metadata and metadata.get("max_chat_id"):
        return str(metadata["max_chat_id"])
    return str(chat_id).split(GROUP_SESSION_SEPARATOR, 1)[0]


def _response_message_id(response: Mapping[str, Any]) -> Optional[str]:
    message = response.get("message")
    if isinstance(message, Mapping):
        body = message.get("body")
        if isinstance(body, Mapping) and body.get("mid"):
            return str(body["mid"])
    for key in ("message_id", "id"):
        if response.get(key):
            return str(response[key])
    return None


def _send_result_from_ids(message_ids: Iterable[str]) -> Any:
    ids = [str(item) for item in message_ids if item]
    if not ids:
        return SendResult(success=False, error="MAX did not confirm message delivery")
    return SendResult(
        success=True,
        message_id=ids[-1],
        continuation_message_ids=tuple(ids[:-1]),
    )


def _split_preserving_content(text: str, max_len: int) -> list[str]:
    if max_len <= 0:
        raise ValueError("max_len must be positive")
    return [text[index:index + max_len] for index in range(0, len(text), max_len)]


def _attachment_batches(
    attachments: Iterable[Mapping[str, Any]],
) -> list[list[Mapping[str, Any]]]:
    """Split MAX media into API-compatible messages.

    MAX permits images/videos together, but a file cannot be combined with
    them and only one file is allowed in a message.  Audio is kept separate
    as well because the current API does not document mixed audio batches.
    """

    batches: list[list[Mapping[str, Any]]] = []
    media_batch: list[Mapping[str, Any]] = []
    for raw_attachment in attachments:
        attachment = dict(raw_attachment)
        attachment_type = str(attachment.get("type") or "").strip().lower()
        if attachment_type in {"file", "audio"}:
            if media_batch:
                batches.append(media_batch)
                media_batch = []
            batches.append([attachment])
            continue
        media_batch.append(attachment)
        if len(media_batch) >= MAX_ATTACHMENTS_PER_MESSAGE:
            batches.append(media_batch)
            media_batch = []
    if media_batch:
        batches.append(media_batch)
    return batches


def _config_extra(config: Any) -> dict[str, Any]:
    value = getattr(config, "extra", {}) or {}
    return dict(value) if isinstance(value, Mapping) else {}


class MaxAdapter(BasePlatformAdapter):  # type: ignore[misc]
    """Webhook-first MAX adapter using Hermes' external plugin contract."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    splits_long_messages = True
    supports_code_blocks = False
    SUPPORTS_MESSAGE_EDITING = True

    async def handle_message(self, event: Any) -> None:
        with russian_commands():
            runner = getattr(self, "gateway_runner", None)
            get_command = getattr(event, "get_command", None)
            if (callable(get_command) and get_command() == "queue"
                    and not callable(getattr(runner, "_handle_queue_command", None))):
                args = event.get_command_args().strip()
                if not args or args.split()[0].lower() in {"list", "status", "edit", "rm", "move", "clear"}:
                    await self._reply_queue_control(event, args)
                    event._max_gateway_consumed = True
                    return
                if args.split()[0].lower() == "add":
                    args = args[3:].strip()
                    if not args:
                        await self._reply_queue_control(event, "")
                        event._max_gateway_consumed = True
                        return
                    event.text = "/queue " + args
                key_fn = getattr(self, "_event_session_key", None)
                key = key_fn(event) if callable(key_fn) else None
                busy = key is not None and key in getattr(self, "_active_sessions", {})
                running = getattr(runner, "_is_session_running", None)
                busy = busy or bool(key is not None and callable(running) and running(key))
                if not busy:
                    # Idle queue items are normal turns. The native FIFO still owns pending work.
                    event.text = args
            return await super().handle_message(event)

    async def _reply_queue_control(self, event: Any, args: str) -> None:
        runner = getattr(self, "gateway_runner", None)
        count_fn = getattr(runner, "_queue_depth", None)
        key_fn = getattr(runner, "_session_key_for_source", None)
        if callable(count_fn) and callable(key_fn):
            count = count_fn(key_fn(event.source), adapter=self)
            text = f"В очереди запросов: {int(count)}.\nДобавить запрос: /queue <текст>."
        else:
            text = "Состояние очереди недоступно. Подключение к Hermes ещё не завершено."
        if args and args.split()[0].lower() in {"edit", "rm", "move", "clear"}:
            text += "\nЭта версия gateway не поддерживает редактирование очереди. Действие не выполнено."
        metadata = dict(getattr(event, "metadata", None) or {})
        await self.send(event.source.chat_id, text, metadata=metadata)

    def set_message_handler(self, handler) -> None:
        async def localized(event):
            context = {"adapter": id(self), "metadata": dict(getattr(event, "metadata", None) or {}),
                       "user_id": str(getattr(getattr(event, "source", None), "user_id", "") or "")}
            token = COMMAND_CONTEXT.set(context)
            try:
                with russian_commands():
                    return await handler(event)
            finally:
                COMMAND_CONTEXT.reset(token)
        super().set_message_handler(localized if handler else None)

    def set_busy_session_handler(self, handler) -> None:
        if handler is None:
            return super().set_busy_session_handler(None)

        async def observed(event, session_key):
            event._max_gateway_consumed = False
            token = COMMAND_CONTEXT.set({"adapter": id(self),
                "metadata": dict(getattr(event, "metadata", None) or {}),
                "user_id": str(getattr(getattr(event, "source", None), "user_id", "") or "")})
            try:
                with russian_commands():
                    handled = await handler(event, session_key)
            finally:
                COMMAND_CONTEXT.reset(token)
            if handled is True and getattr(event, "_bot_loop_admitted", False) is True:
                # "Handled" includes steer/inline consumption, not a promise of a new turn.
                event._max_gateway_consumed = True
            return handled

        return super().set_busy_session_handler(observed)

    def __init__(self, config: "PlatformConfig") -> None:
        platform = Platform(PLATFORM_NAME) if Platform is not None else PLATFORM_NAME
        super().__init__(config, platform)  # type: ignore[arg-type]
        self._config = config
        self._extra = _config_extra(config)
        self._token = str(
            getattr(config, "token", None)
            or self._extra.get("token")
            or _max_env("MAX_BOT_TOKEN", "")
        ).strip()
        self._api_base = str(
            self._extra.get("api_base_url")
            or _max_env("MAX_API_BASE_URL", DEFAULT_API_BASE)
        ).rstrip("/")
        self._webhook_url = str(
            self._extra.get("webhook_url") or _max_env("MAX_WEBHOOK_URL", "")
        ).strip() or None
        self._webhook_secret = str(
            self._extra.get("webhook_secret") or _max_env("MAX_WEBHOOK_SECRET", "")
        ).strip() or None
        self._polling_timeout = int(self._extra.get("polling_timeout", 30))
        self._access = AccessPolicy.from_env_and_extra(
            allowed_users_env=_max_env("MAX_ALLOWED_USERS"),
            group_allowed_users_env=_max_env("MAX_GROUP_ALLOWED_USERS"),
            group_allowed_chats_env=_max_env("MAX_GROUP_ALLOWED_CHATS"),
            allow_all_env=_max_env("MAX_ALLOW_ALL_USERS"),
            guest_mode_env=_max_env("MAX_GUEST_MODE"),
            admin_users_env=_max_env("MAX_ADMIN_USERS"),
            extra=self._extra,
        )
        self._require_mention = _truthy(
            self._extra.get("require_mention", _max_env("MAX_REQUIRE_MENTION", "true"))
        )
        self._client: Optional[MaxClient] = None
        self._receiver: Optional[MaxWebhookReceiver] = None
        self._polling_task: Optional[asyncio.Task] = None
        self._failover_task: Optional[asyncio.Task] = None
        self._failover = None
        self._failover_http = None
        self._auto_fallback = _truthy(self._extra.get("webhook_auto_fallback", False))
        self._auto_return = self._auto_fallback and _truthy(self._extra.get("webhook_auto_return", False))
        self._recovery_successes = 0
        self._fallback_since = 0.0
        self._recovery_min_seconds = 120.0
        self._poll_health: dict[str, Any] = {
            "task_state": "not_started",
            "last_poll_started": None,
            "last_poll_success": None,
            "request_in_flight": False,
            "consecutive_errors": 0,
            "last_error_category": None,
        }
        self._worker_tasks: list[asyncio.Task] = []
        self._inbox_wakeup = asyncio.Event()
        self._bot_user_id: Optional[str] = None
        self._bot_username: Optional[str] = None
        self._chat_target_types: dict[str, str] = {}
        self._dm_action_chat_ids: dict[str, str] = {}
        self._webhook_mode = bool(self._webhook_url)
        default_inbox = _max_home()
        self._inbox_path = str(
            self._extra.get("inbox_path")
            or _max_env("MAX_INBOX_PATH", default_inbox / "max" / "webhook-inbox.sqlite3")
        )
        self._marker_path = str(
            self._extra.get("marker_path")
            or _max_env("MAX_MARKER_PATH", default_inbox / "max" / "polling-marker.sqlite3")
        )
        self._target_path = str(
            self._extra.get("target_path")
            or _max_env("MAX_TARGET_PATH", default_inbox / "max" / "targets.sqlite3")
        )
        self._marker_store: Optional[PollingMarkerStore] = None
        self._target_store: Optional[MaxTargetStore] = None
        self._rate_limiter = MaxRateLimiter()
        self._media_download_semaphore = asyncio.Semaphore(2)
        self._chat_dispatch_locks: dict[str, asyncio.Lock] = {}
        self._chat_media_download_locks: dict[str, asyncio.Lock] = {}
        try:
            callback_ttl = float(
                self._extra.get(
                    "callback_ttl_seconds",
                    _max_env("MAX_CALLBACK_TTL_SECONDS", "600"),
                )
            )
        except (TypeError, ValueError):
            callback_ttl = 600.0
        self._callbacks = MaxCallbackStore(ttl_seconds=callback_ttl)
        self._model_pickers: dict[str, dict[str, Any]] = {}
        self._choice_pickers: dict[str, dict[str, Any]] = {}
        self._choice_versions: dict[tuple[str, str], str] = {}
        self._last_group_users: dict[str, tuple[str, float]] = {}
        try:
            self._media_max_bytes = int(
                self._extra.get(
                    "media_max_bytes",
                    _max_env("MAX_MEDIA_MAX_BYTES", str(DEFAULT_MEDIA_MAX_BYTES)),
                )
            )
        except (TypeError, ValueError):
            self._media_max_bytes = DEFAULT_MEDIA_MAX_BYTES
        if self._media_max_bytes <= 0:
            self._media_max_bytes = DEFAULT_MEDIA_MAX_BYTES
        self._media_max_bytes = min(self._media_max_bytes, DEFAULT_MEDIA_MAX_BYTES)
        self._lock_acquired = False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self._token:
            logger.error("MAX_BOT_TOKEN is not configured")
            return False
        self._webhook_mode = bool(self._webhook_url)
        try:
            acquire_lock = getattr(self, "_acquire_platform_lock", None)
            if callable(acquire_lock):
                if not acquire_lock("max_bot_token", self._token, "MAX bot token"):
                    return False
                self._lock_acquired = True
            verify = tls_verify_from_env(
                {"MAX_CA_BUNDLE": str(self._extra.get("ca_bundle", _max_env("MAX_CA_BUNDLE", "")))}
            )
            self._client = MaxClient(self._token, base_url=self._api_base, verify=verify)
            bot = await self._client.get_me()
            self._bot_user_id = str(bot.get("user_id") or bot.get("id") or "") or None
            self._bot_username = str(bot.get("username") or "") or None
            await self._register_commands()
            self._running = True
            self._target_store = MaxTargetStore(self._target_path)

            if self._webhook_mode:
                if not self._webhook_secret:
                    raise ValueError("MAX_WEBHOOK_SECRET is required in Webhook mode")
                self._receiver = MaxWebhookReceiver(
                    self._webhook_secret,
                    inbox_path=self._inbox_path,
                )
                await self._receiver.recover_inflight()
                if self._auto_fallback:
                    self._marker_store = WebhookPollingStore(self._inbox_path)
                    self._marker_store.seed_legacy(self._marker_path)
                    self._failover = PollingFailover(
                        self._client, self._webhook_url, self._probe_webhook, self._activate_fallback,
                    )
                if self._auto_fallback and self._marker_store.state("fallback") == "polling":
                    urls = PollingFailover.urls(await self._client.get_subscriptions())
                    if self._auto_return and urls == [self._webhook_url]:
                        self._finish_webhook_return()
                    elif urls:
                        raise MaxApiError("Cannot resume polling with active subscriptions", code="active_subscription")
                    else:
                        self._marker_store.cancel_return()
                        await self._activate_fallback()
                        self._failover.state = "polling"
                    if self._auto_return:
                        self._failover_task = asyncio.create_task(self._monitor_webhook(), name="max-webhook-watchdog")
                else:
                    if self._auto_fallback:
                        urls = PollingFailover.urls(await self._client.get_subscriptions())
                        if any(url != self._webhook_url for url in urls):
                            raise MaxApiError("Foreign Webhook subscription exists", code="foreign_subscription")
                    await self._client.subscribe_webhook(
                        self._webhook_url or "",
                        self._webhook_secret,
                        update_types=MAX_UPDATE_TYPES,
                    )
                    if self._auto_fallback:
                        urls = PollingFailover.urls(await self._client.get_subscriptions())
                        if urls != [self._webhook_url]:
                            raise MaxApiError("Webhook subscription readback mismatch", code="subscription_mismatch")
                        self._failover_task = asyncio.create_task(self._monitor_webhook(), name="max-webhook-watchdog")
                self._start_inbox_workers("webhook")
            else:
                self._marker_store = PollingMarkerStore(self._marker_path)
                self._marker_store.recover_inflight()
                self._start_inbox_workers("polling")
                self._poll_health["task_state"] = "running"
                self._polling_task = asyncio.create_task(
                    self._poll_updates(), name="max-polling"
                )
            self._mark_connected()
            logger.info(
                "MAX adapter connected transport=%s reconnect=%s",
                "webhook" if self._webhook_mode else "polling",
                is_reconnect,
            )
            return True
        except (MaxApiError, ValueError, OSError) as exc:
            logger.error("MAX adapter connection failed: %s", exc)
            await self.disconnect()
            return False

    async def disconnect(self) -> None:
        self._running = False
        tasks = [self._failover_task, self._polling_task, *self._worker_tasks]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        for task in tasks:
            if task:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._polling_task = None
        self._failover_task = None
        if self._failover_http is not None:
            await self._failover_http.aclose()
            self._failover_http = None
        if isinstance(getattr(self, "_poll_health", None), dict):
            self._poll_health["task_state"] = "stopped"
        self._worker_tasks.clear()
        self._chat_dispatch_locks.clear()
        self._chat_media_download_locks.clear()
        self._dm_action_chat_ids.clear()
        self._model_pickers.clear()
        getattr(self, "_choice_pickers", {}).clear()
        getattr(self, "_choice_versions", {}).clear()
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._receiver is not None:
            await self._receiver.close()
        self._receiver = None
        if self._marker_store is not None:
            self._marker_store.close()
            self._marker_store = None
        if self._target_store is not None:
            self._target_store.close()
            self._target_store = None
        if self._lock_acquired:
            release_lock = getattr(self, "_release_platform_lock", None)
            if callable(release_lock):
                release_lock()
            self._lock_acquired = False
        try:
            self._mark_disconnected()
        except AttributeError:
            pass

    def _max_commands(self) -> list[dict[str, str]]:
        """Build a bounded menu from the installed Hermes command registry."""

        descriptions = {
            "menu": "Показать кнопки Hermes",
            "commands": "Все команды и навыки (постранично)",
            "maxstatus": "Состояние очереди MAX",
            "help": "Список доступных команд",
            "status": "Статус сессии и модели",
            "new": "Новая сессия",
            "stop": "Остановить текущую задачу и фоновые процессы",
            "model": "Выбрать модель",
            "goal": "Цель: показать, задать или продолжить",
            "reasoning": "Выбрать уровень рассуждений",
            "compress": "Сжать контекст",
            "sessions": "Предыдущие сессии",
            "resume": "Список и возобновление именованных сессий",
            "retry": "Повторить последнее сообщение",
            "undo": "Отменить последний ход",
            "agents": "Активные задачи",
            "queue": "Показать очередь или добавить запрос",
            "background": "Запустить в фоне",
            "whoami": "Проверить доступ",
        }
        preferred = [
            "menu", "commands", "help", "status", "new", "stop", "model", "goal", "reasoning", "compress",
            "sessions", "resume", "retry", "undo", "agents", "whoami",
            "queue", "background", "maxstatus",
        ]
        registry: dict[str, Any] = {}
        registry_available = False
        try:
            from hermes_cli.commands import COMMAND_REGISTRY, _is_gateway_available

            registry_available = True
            for item in COMMAND_REGISTRY:
                if getattr(item, "cli_only", False):
                    continue
                if not _is_gateway_available(item):
                    continue
                registry[str(item.name).lower()] = item
        except Exception:  # pragma: no cover - only for reduced standalone runtimes
            registry = {}

        result: list[dict[str, str]] = []
        for name in preferred:
            if name in {"menu", "maxstatus"}:
                result.append({"name": name, "description": descriptions[name]})
                continue
            item = registry.get(name)
            if item is None:
                # Unit tests and standalone plugin tools intentionally run
                # without a Hermes checkout.  Keep only the stable session
                # status command visible there; an installed Hermes registry
                # remains authoritative in production.
                if not registry_available and name in {"help", "status", "commands"}:
                    result.append({"name": name, "description": descriptions[name]})
                continue
            result.append(
                {
                    "name": name,
                    "description": descriptions.get(name, str(item.description)),
                }
            )
        return result[:32]

    async def _register_commands(self) -> None:
        if self._client is None or not callable(getattr(self._client, "set_bot_commands", None)):
            return
        commands = self._max_commands()
        try:
            await self._client.set_bot_commands(commands)
            logger.info(
                "MAX command menu registered (%d): %s",
                len(commands),
                ",".join(item["name"] for item in commands),
            )
        except (MaxApiError, ValueError) as exc:
            # Menu registration is best effort.  A MAX API rollout must not
            # disable text/media delivery when PATCH /me/commands is down.
            logger.warning("MAX command menu registration failed: %s", exc)

    async def _poll_updates(self) -> None:
        marker: Optional[int] = self._marker_store.get() if self._marker_store else None
        backoff = 1.0
        health = getattr(self, "_poll_health", None)
        if not isinstance(health, dict):
            health = self._poll_health = {
                "task_state": "running",
                "last_poll_started": None,
                "last_poll_success": None,
                "request_in_flight": False,
                "consecutive_errors": 0,
                "last_error_category": None,
            }
        health["task_state"] = "running"
        while self._running and self._client is not None:
            health["last_poll_started"] = time.monotonic()
            health["request_in_flight"] = True
            try:
                result = await self._client.get_updates(
                    marker=marker,
                    timeout=self._polling_timeout,
                    types=MAX_UPDATE_TYPES,
                )
                next_marker = result.get("marker", marker)
                updates = [
                    item for item in (result.get("updates", []) or [])
                    if isinstance(item, Mapping)
                ]
                if self._marker_store is not None:
                    self._marker_store.accept_batch(
                        updates,
                        int(next_marker) if next_marker is not None else None,
                    )
                    if updates:
                        self._inbox_wakeup.set()
                marker = int(next_marker) if next_marker is not None else marker
                backoff = 1.0
                health["request_in_flight"] = False
                health["last_poll_success"] = time.monotonic()
                health["consecutive_errors"] = 0
                health["last_error_category"] = None
                if self._marker_store is None:
                    for update in updates:
                        await self._dispatch_update(update)
            except asyncio.CancelledError:
                health["request_in_flight"] = False
                health["task_state"] = "stopped" if not self._running else "cancelled"
                return
            except MaxApiError as exc:
                health["request_in_flight"] = False
                if not self._running:
                    health["task_state"] = "stopped"
                    return
                health["consecutive_errors"] = int(health.get("consecutive_errors", 0)) + 1
                health["last_error_category"] = (
                    exc.code
                    or (f"http_{exc.status_code}" if exc.status_code else "api_error")
                )
                delay = exc.retry_after or min(backoff, 30.0)
                logger.warning(
                    "MAX polling failed; retrying in %.1fs (%s)",
                    delay,
                    health["last_error_category"],
                )
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, 30.0)
            except Exception as exc:  # noqa: BLE001
                health["request_in_flight"] = False
                health["consecutive_errors"] = int(health.get("consecutive_errors", 0)) + 1
                health["last_error_category"] = type(exc).__name__
                logger.warning(
                    "MAX polling failed; retrying in %.1fs (%s)",
                    backoff,
                    health["last_error_category"],
                )
                await asyncio.sleep(min(backoff, 30.0))
                backoff = min(backoff * 2.0, 30.0)

    def _start_inbox_workers(self, transport: str) -> None:
        self._worker_tasks = [
            asyncio.create_task(
                self._consume_durable_updates(control_only=True),
                name=f"max-{transport}-control-worker",
            ),
            asyncio.create_task(
                self._consume_durable_updates(control_only=False),
                name=f"max-{transport}-media-worker-1",
            ),
            asyncio.create_task(
                self._consume_durable_updates(control_only=False),
                name=f"max-{transport}-media-worker-2",
            ),
        ]

    async def _probe_webhook(self) -> bool:
        import httpx
        if self._failover_http is None:
            self._failover_http = httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False)
        nonce = token_urlsafe(16)
        response = await self._failover_http.get(
            self._webhook_url.rstrip("/") + "/ready",
            headers={"X-Max-Bot-Api-Secret": self._webhook_secret, "X-Max-Health-Nonce": nonce},
        )
        if response.status_code != 200:
            return False
        data = response.json()
        return isinstance(data, Mapping) and data.get("ready") is True and data.get("nonce") == nonce

    async def _activate_fallback(self) -> None:
        # Keep the same inbox/workers and processing hooks: running turns are not cancelled.
        self._marker_store.latch()
        self._webhook_mode = False
        self._fallback_since = time.monotonic()
        self._recovery_successes = 0
        if self._polling_task is None or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._poll_updates(), name="max-fallback-polling")
        self._inbox_wakeup.set()
        logger.warning("MAX switched to direct polling fallback; automatic Webhook return=%s", self._auto_return)

    async def _pause_polling(self) -> None:
        task = self._polling_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._polling_task = None

    def _finish_webhook_return(self) -> None:
        self._marker_store.finish_return()
        self._webhook_mode = True
        self._recovery_successes = 0
        self._poll_health["task_state"] = "standby"
        self._failover.state, self._failover.error = "webhook", None
        self._failover.failures = 0
        logger.info("MAX automatically returned to Webhook; inbox workers preserved")

    async def _check_webhook_return(self) -> None:
        # A persisted intent fences polling while a subscription outcome is ambiguous.
        returning = self._marker_store.state("returning") == "1"
        try:
            urls = PollingFailover.urls(await self._client.get_subscriptions())
            if urls:
                self._marker_store.begin_return()
                await self._pause_polling()
                if urls == [self._webhook_url]:
                    self._finish_webhook_return()
                else:
                    self._failover.state, self._failover.error = "blocked", "foreign_subscription"
                return
            if returning:
                self._marker_store.cancel_return()
                await self._activate_fallback()
                self._failover.state, self._failover.error = "polling", None
                return
            healthy = await self._probe_webhook()
            self._recovery_successes = self._recovery_successes + 1 if healthy else 0
            if self._recovery_successes < 3 or time.monotonic() - self._fallback_since < self._recovery_min_seconds:
                return
            self._marker_store.begin_return()
            await self._pause_polling()
            self._failover.state = "returning"
            try:
                await self._client.subscribe_webhook(self._webhook_url, self._webhook_secret, update_types=MAX_UPDATE_TYPES)
            except Exception:
                # POST can fail after the server applied it; only GET resolves the outcome.
                pass
            urls = PollingFailover.urls(await self._client.get_subscriptions())
            if urls == [self._webhook_url]:
                self._finish_webhook_return()
            elif not urls:
                self._marker_store.cancel_return()
                await self._activate_fallback()
                self._failover.state, self._failover.error = "polling", "return_not_registered"
            else:
                self._failover.state, self._failover.error = "blocked", "foreign_subscription"
        except Exception as exc:
            self._recovery_successes = 0
            self._failover.state = "blocked" if self._marker_store.state("returning") else "polling"
            self._failover.error = type(exc).__name__

    async def _monitor_webhook(self) -> None:
        while self._running:
            await asyncio.sleep(20)
            if self._webhook_mode:
                switched = await self._failover.check()
                if switched and not self._auto_return:
                    return
            elif self._auto_return:
                await self._check_webhook_return()
            else:
                return

    async def _update_inbox(self, method_name: str, *args: Any) -> None:
        store = self._receiver if self._receiver is not None else self._marker_store
        if store is None:
            return
        method = getattr(store, method_name, None)
        if not callable(method):
            return
        result = method(*args)
        if inspect.isawaitable(result):
            await result

    async def _consume_durable_updates(self, *, control_only: Optional[bool]) -> None:
        while self._running:
            try:
                self._inbox_wakeup.clear()
                if self._receiver is not None:
                    await self._receiver.drain_wakeup_queue()
                    update = await self._receiver.claim_next(control_only=control_only)
                elif self._marker_store is not None:
                    update = self._marker_store.claim_next(control_only=control_only)
                else:
                    return
                if update is None:
                    try:
                        await asyncio.wait_for(self._inbox_wakeup.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                    continue
                try:
                    disposition = await self._dispatch_update(update)
                    if disposition == "accepted":
                        await self._update_inbox("mark_accepted", update)
                    elif disposition == "rejected":
                        await self._update_inbox(
                            "mark_failed", update, "Hermes не принял сообщение в очередь обработки"
                        )
                    elif disposition == "consumed":
                        await self._update_inbox("mark_consumed", update)
                    elif disposition == "denied":
                        await self._update_inbox(
                            "mark_failed", update, "Команда MAX отклонена политикой доступа"
                        )
                    elif disposition == "media_failed":
                        await self._update_inbox(
                            "mark_failed", update, "Не удалось получить вложения MAX"
                        )
                    elif disposition == "invalid_update":
                        await self._update_inbox(
                            "mark_failed", update, "MAX incomplete_update: событие message_created не содержит доступного сообщения"
                        )
                    else:
                        await self._update_inbox("mark_processed", update)
                except asyncio.CancelledError:
                    await self._update_inbox("mark_unknown_key", max_update_event_key(update))
                    raise
                except Exception as exc:  # noqa: BLE001
                    await self._update_inbox(
                        "mark_failed", update, f"Ошибка обработки: {type(exc).__name__}: {exc}"
                    )
                    logger.exception("MAX update dispatch failed; automatic replay is disabled")
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("MAX durable inbox worker failed")
                await asyncio.sleep(1.0)

    async def _consume_webhook_queue(self, *, control_only: Optional[bool] = None) -> None:
        """Compatibility entry point for tests and deployments using the webhook inbox."""

        await self._consume_durable_updates(control_only=control_only)

    async def handle_webhook(
        self, headers: Mapping[str, str], update: Mapping[str, Any]
    ) -> WebhookResult:
        """Accept one Webhook update for a reverse proxy/ASGI bridge.

        The method returns a status decision; the HTTP server must send that
        status immediately and leave model processing to the worker.
        """

        if self._receiver is None:
            return WebhookResult(status_code=503, accepted=False)
        result = await self._receiver.receive(headers, update)
        if result.durable and result.accepted:
            self._inbox_wakeup.set()
        return result

    async def _populate_message_media(self, message: MaxMessage, event: Any) -> None:
        """Download inbound MAX attachments into Hermes' local media cache."""

        if getattr(self, "_client", None) is None:
            return
        media_errors: list[dict[str, str]] = []
        for raw_attachment in message.attachments:
            attachment = attachment_from_payload(raw_attachment)
            if attachment is None:
                continue
            media_url = attachment.url
            if not media_url and attachment.kind == "video" and attachment.token:
                try:
                    video_info = await self._client.get_video(attachment.token)
                    media_url = _find_download_url(video_info)
                except (MaxApiError, ValueError) as exc:
                    logger.warning("MAX video token resolution failed: %s", exc)
            if not media_url:
                media_errors.append({"kind": attachment.kind, "category": "unavailable"})
                event.text = _append_event_note(
                    event.text,
                    f"[Вложение MAX «{attachment.filename}» не удалось получить для обработки.]",
                )
                continue
            try:
                data, remote_mime = await self._client.download_media(
                    media_url,
                    max_bytes=getattr(self, "_media_max_bytes", DEFAULT_MEDIA_MAX_BYTES),
                )
                if attachment.kind == "image":
                    validate_image_dimensions(data, filename=attachment.filename)
                cached = await asyncio.to_thread(
                    _cache_media_bytes,
                    data,
                    filename=attachment.filename,
                    mime_type=remote_mime or attachment.mime_type,
                    default_kind=attachment.kind,
                )
            except (MaxApiError, OSError, ValueError) as exc:
                media_errors.append({"kind": attachment.kind, "category": "download_or_format"})
                logger.warning(
                    "MAX media download/cache failed kind=%s error=%s",
                    attachment.kind,
                    exc,
                )
                event.text = _append_event_note(
                    event.text,
                    f"[Вложение MAX «{attachment.filename}» не удалось скачать: проверьте формат и размер.]",
                )
                continue
            except Exception:  # noqa: BLE001
                media_errors.append({"kind": attachment.kind, "category": "cache"})
                logger.exception(
                    "MAX media cache failed kind=%s",
                    attachment.kind,
                )
                event.text = _append_event_note(
                    event.text,
                    f"[Вложение MAX «{attachment.filename}» не удалось сохранить для Hermes.]",
                )
                continue

            if cached is None:
                media_errors.append({"kind": attachment.kind, "category": "unreadable"})
                event.text = _append_event_note(
                    event.text,
                    f"[Вложение MAX «{attachment.filename}» не распознано как читаемый файл.]",
                )
                continue
            event.media_urls.append(cached.path)
            event.media_types.append(cached.media_type)
            event.text = _append_event_note(event.text, cached.context_note())
            logger.info("MAX inbound media cached kind=%s", attachment.kind)
        if media_errors:
            event.metadata["max_media_errors"] = media_errors

    async def _download_media_for_chat(self, message: MaxMessage, event: Any) -> None:
        lock = self._chat_media_download_locks.setdefault(message.chat_id, asyncio.Lock())
        async with lock:
            async with self._media_download_semaphore:
                await self._populate_message_media(message, event)

    async def _dispatch_update(self, update: Mapping[str, Any]) -> str:
        update_type = str(update.get("update_type") or "")
        if update_type == "bot_started":
            await self._dispatch_bot_started(update)
            return "processed"
        if update_type in {"bot_stopped", "dialog_removed"}:
            return "processed"
        callback = MaxCallback.from_update(update)
        if callback is not None:
            await self._dispatch_callback(callback)
            return "processed"

        message = MaxMessage.from_update(update)
        if message is None:
            if update_type == "message_created":
                logger.warning("MAX incomplete message_created; cannot identify sender or media safely")
                return "invalid_update"
            return "processed"
        if self._bot_user_id and message.user_id == self._bot_user_id and message.is_bot:
            return "processed"

        is_group = message.is_group
        target_type = "chat" if is_group else "user"
        self._chat_target_types[message.chat_id] = target_type
        if self._target_store is not None:
            self._target_store.set(message.chat_id, target_type)
        mentioned = self._is_mentioned(message.text) or _is_reply_to_bot(
            message.link, self._bot_user_id
        )
        if is_group:
            if not self._access.can_group(message.user_id, message.chat_id, mentioned=mentioned):
                return "processed"
            if self._require_mention and not mentioned and not _is_command(message.text):
                return "processed"
        elif not self._access.can_dm(message.user_id):
            return "processed"
        if _is_command(message.text) and not self._access.can_run_command(
            message.user_id, message.text, is_group=is_group
        ):
            logger.warning("MAX command rejected by access policy")
            await self.send(
                message.chat_id,
                f"Команда /{_command_name(message.text)} ограничена настройками доступа этого бота. "
                "Справка: /help. Диагностика канала: /maxstatus.",
                metadata={"max_target_type": target_type, "max_user_id": message.user_id},
            )
            return "denied"
        if is_group and _command_name(message.text) in GROUP_ADMIN_COMMANDS and not self._access.is_admin(message.user_id):
            await self.send(
                message.chat_id,
                "Эта управляющая команда доступна только администратору группы.",
                metadata={"max_target_type": "chat", "max_user_id": message.user_id},
            )
            return "processed"
        self._remember_group_user(message.chat_id, message.user_id, is_group=is_group)
        if not message.text.strip() and not message.attachments:
            await self._notify_media_delivery_issue(
                message.chat_id,
                "MAX передал пустое сообщение без текста и вложений. "
                "Если вы отправляли голосовое или видеокружок, попробуйте сохранить "
                "запись и прислать её обычным аудио- или видеофайлом.",
                metadata={"max_target_type": target_type, "max_user_id": message.user_id},
            )
            return "media_failed"
        self._note_choice_request(message.chat_id, message.user_id)
        if await self._handle_local_command(message):
            return "processed"
        event = _build_message_event(
            self,
            message,
            update_event_key=max_update_event_key(update),
        )
        media_rejected = False

        async def _deliver_to_hermes() -> None:
            nonlocal media_rejected
            if message.attachments:
                await self._download_media_for_chat(message, event)
                errors = event.metadata.get("max_media_errors", [])
                if errors:
                    await self._notify_media_delivery_issue(
                        message.chat_id,
                        f"Не удалось получить вложений: {len(errors)}. "
                        "Проверьте формат и размер (до 50 МБ) и отправьте их повторно. "
                        + ("Остальные вложения переданы на обработку." if event.media_urls else "Содержимое этих файлов агенту недоступно."),
                        metadata=event.metadata,
                    )
                    if not event.media_urls and not message.text.strip():
                        media_rejected = True
                        return
            await self.handle_message(event)

        if _is_command(message.text):
            await _deliver_to_hermes()
        else:
            session_chat_id = (
                f"{message.chat_id}{GROUP_SESSION_SEPARATOR}{message.user_id}"
                if message.is_group
                else message.chat_id
            )
            lock = self._chat_dispatch_locks.setdefault(session_chat_id, asyncio.Lock())
            async with lock:
                await _deliver_to_hermes()
        if media_rejected:
            return "media_failed"
        if getattr(event, "_gateway_accepted", False):
            return "accepted"
        return "consumed" if getattr(event, "_max_gateway_consumed", False) else "rejected"

    def _remember_dialog_action_chat(self, event: Any) -> None:
        source = getattr(event, "source", None)
        target_id = str(getattr(source, "chat_id", "") or "")
        raw = getattr(event, "raw_message", None)
        recipient = raw.get("recipient") if isinstance(raw, Mapping) else None
        if not target_id or not isinstance(recipient, Mapping):
            return
        if recipient.get("chat_type") != "dialog":
            return
        value = recipient.get("chat_id")
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return
        try:
            internal_id = int(value)
        except (ValueError, TypeError):
            return
        if not internal_id or not -(2**63) <= internal_id < 2**63:
            return
        # Only admitted turns populate this map. Session/reply IDs remain user IDs.
        if not hasattr(self, "_dm_action_chat_ids"):
            self._dm_action_chat_ids = {}
        self._dm_action_chat_ids[target_id] = str(internal_id)

    async def on_processing_start(self, event: Any) -> None:
        self._remember_dialog_action_chat(event)
        metadata = getattr(event, "metadata", None) or {}
        event_key = metadata.get("max_update_event_key") if isinstance(metadata, Mapping) else None
        if event_key:
            await self._update_inbox("mark_processing_key", str(event_key))

    async def on_processing_complete(self, event: Any, outcome: Any) -> None:
        metadata = getattr(event, "metadata", None) or {}
        event_key = metadata.get("max_update_event_key") if isinstance(metadata, Mapping) else None
        if not event_key:
            return
        name = str(
            getattr(outcome, "name", None)
            or getattr(outcome, "value", None)
            or outcome
        ).lower()
        if "success" in name:
            await self._update_inbox("mark_processed_key", str(event_key))
        elif "cancel" in name:
            await self._update_inbox("mark_unknown_key", str(event_key))
        else:
            await self._update_inbox(
                "mark_failed_key", str(event_key), "Hermes обработал сообщение с ошибкой"
            )

    async def _dispatch_bot_started(self, update: Mapping[str, Any]) -> None:
        user = update.get("user") if isinstance(update.get("user"), Mapping) else {}
        user_id = str(user.get("user_id") or user.get("id") or "").strip()
        if not user_id:
            return
        chat_id = str(update.get("chat_id") or user_id).strip()
        is_group = bool(update.get("is_channel")) or chat_id != user_id
        if is_group:
            if not self._access.can_group(user_id, chat_id, mentioned=True):
                return
            target_type = "chat"
        else:
            if not self._access.can_dm(user_id):
                return
            target_type = "user"
        self._chat_target_types[chat_id] = target_type
        if self._target_store is not None:
            self._target_store.set(chat_id, target_type)
        self._remember_group_user(chat_id, user_id, is_group=is_group)
        await self._send_menu(
            chat_id,
            user_id,
            target_type=target_type,
            welcome=True,
        )

    def _remember_group_user(self, chat_id: str, user_id: str, *, is_group: bool) -> None:
        if is_group:
            if not hasattr(self, "_last_group_users"):
                self._last_group_users = {}
            self._last_group_users[str(chat_id)] = (str(user_id), time.monotonic())

    async def _handle_local_command(self, message: MaxMessage) -> bool:
        command = _command_name(message.text)
        if command == "queue" and not callable(getattr(getattr(self, "gateway_runner", None), "_handle_queue_command", None)):
            args = message.text.strip().partition(" ")[2].strip()
            if not args or args.split()[0].lower() in {"list", "status", "edit", "rm", "move", "clear"}:
                await self._reply_queue_control(_build_message_event(self, message), args)
                return True
        if command == "menu":
            await self._send_menu(
                message.chat_id,
                message.user_id,
                target_type="chat" if message.is_group else "user",
            )
            return True
        if command == "maxstatus":
            await self.send(
                message.chat_id,
                self._max_status_text(),
                metadata={
                    "max_target_type": "chat" if message.is_group else "user",
                    "max_user_id": message.user_id,
                },
            )
            return True
        if command == "start":
            await self._send_menu(
                message.chat_id,
                message.user_id,
                target_type="chat" if message.is_group else "user",
                welcome=True,
            )
            return True
        return False

    async def _dispatch_callback(self, callback: MaxCallback) -> None:
        with russian_commands():
            await self._dispatch_callback_impl(callback)

    async def _dispatch_callback_impl(self, callback: MaxCallback) -> None:
        """Resolve a MAX button through Hermes' native interactive hooks."""

        self._remember_group_user(callback.chat_id, callback.user_id, is_group=callback.is_group)
        if callback.is_group:
            authorized = self._access.can_group(
                callback.user_id, callback.chat_id, mentioned=False
            )
        else:
            authorized = self._access.can_dm(callback.user_id)
        if not authorized:
            await self._answer_callback(callback, "Нет доступа к этой кнопке.")
            return

        pending_entry = self._callbacks.peek(callback.payload)
        if pending_entry is not None and pending_entry.kind == "choice":
            state = getattr(self, "_choice_pickers", {}).get(pending_entry.value.partition(":")[0])
            if state is not None and state["message_id"] != callback.message_id:
                await self._answer_callback(callback, "Кнопка относится к другому сообщению.")
                return
        if (
            callback.is_group
            and pending_entry is not None
            and (
                pending_entry.kind in GROUP_ADMIN_CALLBACKS
                or (
                    pending_entry.kind == "command"
                    and pending_entry.value in GROUP_ADMIN_COMMANDS
                )
            )
            and not self._access.is_admin(callback.user_id)
        ):
            await self._answer_callback(callback, "Это действие доступно только администратору группы.")
            return

        entry = self._callbacks.consume(
            callback.payload,
            user_id=callback.user_id,
            chat_id=callback.chat_id,
        )
        if entry is None:
            await self._answer_callback(callback, "Кнопка устарела или уже использована.")
            return

        try:
            if entry.kind == "command":
                if (
                    callback.is_group
                    and entry.value in GROUP_ADMIN_COMMANDS
                    and not self._access.is_admin(callback.user_id)
                ):
                    await self._answer_callback(
                        callback,
                        "Эта команда доступна только администратору группы.",
                    )
                    return
                await self._answer_callback(callback, "Команда выполняется.")
                command_message = MaxMessage(
                    message_id=callback.message_id or f"callback:{callback.callback_id}",
                    user_id=callback.user_id,
                    user_name=callback.user_name,
                    chat_id=callback.chat_id,
                    chat_type=callback.chat_type,
                    chat_title=None,
                    text=f"/{entry.value}",
                    link={"message_id": callback.message_id} if callback.message_id else None,
                    raw_message=callback.raw_message,
                )
                self._note_choice_request(callback.chat_id, callback.user_id)
                if await self._handle_local_command(command_message):
                    return
                event = _build_message_event(self, command_message)
                await self._populate_message_media(command_message, event)
                await self.handle_message(event)
                return

            if entry.kind == "approval":
                from tools.approval import resolve_gateway_approval

                count = resolve_gateway_approval(entry.session_key, entry.value)
                labels = {
                    "once": "Разрешено один раз",
                    "session": "Разрешено на сессию",
                    "always": "Разрешено всегда",
                    "deny": "Запрещено",
                }
                label = labels.get(entry.value, "Запрос обработан") if count else "Запрос уже завершен"
                await self._answer_callback(callback, label)
                if count:
                    resume = getattr(self, "resume_typing_for_chat", None)
                    if callable(resume):
                        resume(callback.chat_id)
                return

            if entry.kind == "slash":
                from tools import slash_confirm

                choice, confirm_id = entry.value.split(":", 1)
                result_text = await slash_confirm.resolve(
                    entry.session_key, confirm_id, choice
                )
                labels = {
                    "once": "Подтверждено один раз",
                    "always": "Подтверждено всегда",
                    "cancel": "Отменено",
                }
                await self._answer_callback(callback, labels.get(choice, "Запрос обработан"))
                if result_text:
                    await self.send(
                        callback.chat_id,
                        str(result_text),
                        metadata={
                            "max_target_type": "chat" if callback.is_group else "user"
                        },
                    )
                return

            if entry.kind == "clarify":
                clarify_id, choice_token = entry.value.split(":", 1)
                if choice_token == "other":
                    from tools.clarify_gateway import mark_awaiting_text

                    if mark_awaiting_text(clarify_id):
                        await self._answer_callback(
                            callback, "Введите свой вариант следующим сообщением."
                        )
                    else:
                        await self._answer_callback(callback, "Запрос уже завершен.")
                    return

                idx = int(choice_token)
                resolved_text: Optional[str] = None
                try:
                    from tools.clarify_gateway import _entries as clarify_entries

                    clarify_entry = clarify_entries.get(clarify_id)
                    if clarify_entry and clarify_entry.choices and 0 <= idx < len(clarify_entry.choices):
                        resolved_text = str(clarify_entry.choices[idx])
                except Exception:
                    resolved_text = None
                resolved_text = resolved_text or f"choice {idx + 1}"

                from tools.clarify_gateway import resolve_gateway_clarify

                resolved = resolve_gateway_clarify(clarify_id, resolved_text)
                await self._answer_callback(
                    callback,
                    f"Выбрано: {resolved_text}" if resolved else "Запрос уже завершен.",
                )
                return

            if entry.kind == "model":
                await self._dispatch_model_callback(callback, entry.value)
                return

            if entry.kind == "choice":
                await self._dispatch_choice_callback(callback, entry)
                return

            await self._answer_callback(callback, "Неизвестная кнопка.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("MAX callback resolution failed: %s", exc, exc_info=True)
            await self._answer_callback(callback, "Не удалось обработать кнопку.")

    async def _dispatch_model_callback(
        self, callback: MaxCallback, value: str
    ) -> None:
        parts = str(value).split(":", 2)
        if len(parts) < 2:
            await self._answer_callback(callback, "Кнопка выбора модели некорректна.")
            return
        picker_id, action = parts[0], parts[1]
        argument = parts[2] if len(parts) == 3 else ""
        state = self._model_pickers.get(picker_id)
        if state is None:
            await self._answer_callback(callback, "Выбор модели устарел. Откройте /model заново.")
            return

        if action == "cancel":
            self._model_pickers.pop(picker_id, None)
            await self._answer_callback(callback, "Выбор модели отменен.")
            return

        if action == "back":
            rows = self._model_provider_rows(state, picker_id, callback)
            await self._answer_callback(
                callback,
                self._model_picker_provider_text(state),
                attachments=[build_inline_keyboard(rows)],
            )
            return

        if action == "provider":
            provider = next(
                (item for item in state["providers"] if str(item.get("slug")) == argument),
                None,
            )
            if provider is None:
                await self._answer_callback(callback, "Провайдер не найден.")
                return
            state["selected_provider"] = argument
            state["selected_provider_name"] = str(provider.get("name") or argument)
            state["model_list"] = [str(model) for model in provider.get("models", [])]
            rows = self._model_rows(state, picker_id, callback)
            await self._answer_callback(
                callback,
                self._model_picker_model_text(state),
                attachments=[build_inline_keyboard(rows)],
            )
            return

        if action == "model":
            try:
                index = int(argument)
            except ValueError:
                await self._answer_callback(callback, "Модель указана некорректно.")
                return
            models = state.get("model_list", [])
            if index < 0 or index >= len(models):
                await self._answer_callback(callback, "Модель не найдена.")
                return
            on_model_selected = state.get("on_model_selected")
            if not callable(on_model_selected):
                self._model_pickers.pop(picker_id, None)
                await self._answer_callback(callback, "Выбор модели устарел.")
                return
            model_id = str(models[index])
            provider_slug = str(state.get("selected_provider") or "")
            try:
                result_text = on_model_selected(callback.chat_id, model_id, provider_slug)
                if inspect.isawaitable(result_text):
                    result_text = await result_text
                result_text = str(result_text or "Модель переключена.")
            except Exception as exc:  # noqa: BLE001
                logger.warning("MAX model picker switch failed: %s", exc, exc_info=True)
                result_text = "Не удалось переключить модель."
            self._model_pickers.pop(picker_id, None)
            await self._answer_callback(callback, result_text)
            return

        await self._answer_callback(callback, "Неизвестное действие выбора модели.")

    async def _answer_callback(
        self,
        callback: MaxCallback,
        text: str,
        *,
        attachments: Optional[Iterable[Mapping[str, Any]]] = None,
    ) -> None:
        if self._client is None:
            return
        body = {
            "text": str(text)[:MAX_MESSAGE_LENGTH],
            "attachments": [dict(item) for item in attachments] if attachments is not None else [],
            "format": "markdown",
        }
        try:
            await self._rate_limiter.acquire(callback.chat_id)

            async def _answer() -> Mapping[str, Any]:
                return await self._client.answer_callback(
                    callback.callback_id,
                    message=body,
                )

            await with_backoff(
                _answer,
                is_rate_limit=_is_max_rate_limit,
                extract_retry_after=_retry_after,
            )
        except MaxApiError as exc:
            logger.warning("MAX callback answer failed: %s", exc)

    def _is_mentioned(self, text: str) -> bool:
        return bool(self._bot_username and f"@{self._bot_username.lower()}" in text.lower())

    def _target_type_for(
        self, chat_id: str, metadata: Optional[Mapping[str, Any]] = None
    ) -> str:
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        target_type = self._chat_target_types.get(transport_chat_id)
        target_store = getattr(self, "_target_store", None)
        if target_type is None and target_store is not None:
            target_type = target_store.get(transport_chat_id)
        target_type = target_type or "user"
        if metadata and metadata.get("max_target_type") in {"user", "chat"}:
            target_type = str(metadata["max_target_type"])
        return target_type

    def _max_status_text(self) -> str:
        transport = "webhook" if self._webhook_mode else "polling"
        failover = getattr(self, "_failover", None)
        poll_health = getattr(self, "_poll_health", {})
        poll_task = getattr(self, "_polling_task", None)
        if self._webhook_mode:
            poll_status = "не используется"
            last_poll_success = "не используется"
            poll_errors = 0
            poll_error_category = "нет"
        else:
            task_state = "остановлена"
            if poll_task is not None and not poll_task.done():
                task_state = "работает"
            elif poll_health.get("task_state") == "not_started":
                task_state = "не запущена"
            if poll_health.get("request_in_flight"):
                started = poll_health.get("last_poll_started")
                if isinstance(started, (int, float)) and time.monotonic() - started > 120:
                    task_state = "нет ответа более 120 с"
            poll_status = task_state
            last_success = poll_health.get("last_poll_success")
            last_poll_success = (
                f"{max(0, int(time.monotonic() - float(last_success)))} с назад"
                if isinstance(last_success, (int, float))
                else "нет с момента запуска"
            )
            poll_errors = int(poll_health.get("consecutive_errors", 0) or 0)
            poll_error_category = str(poll_health.get("last_error_category") or "нет")
        summary: Mapping[str, Any] = {}
        if self._receiver is not None:
            summary = self._receiver.status_summary()
        elif self._marker_store is not None:
            summary = self._marker_store.status_summary()
        pending = int(summary.get("pending", 0) or 0)
        dispatching = int(summary.get("dispatching", 0) or 0)
        accepted = int(summary.get("accepted", 0) or 0)
        processing = int(summary.get("processing", 0) or 0)
        failed = int(summary.get("failed", 0) or 0)
        unknown = int(summary.get("unknown", 0) or 0)
        consumed = int(summary.get("consumed", 0) or 0)
        last_error = str(summary.get("last_error") or "")
        error_category = "нет"
        if last_error:
            lowered = last_error.lower()
            if "429" in lowered or "rate" in lowered:
                error_category = "rate limit"
            elif "timeout" in lowered or "transport" in lowered:
                error_category = "сеть/таймаут"
            elif "media" in lowered or "attachment" in lowered:
                error_category = "вложение"
            elif "api" in lowered:
                error_category = "api"
            else:
                error_category = "обработка"
        return (
            "**MAX: диагностика**\n\n"
            f"Транспорт: `{transport}`\n"
            f"Автоматический резерв: `{failover.state if failover else 'выключен'}`\n"
            f"Автовозврат Webhook: `{'включён' if getattr(self, '_auto_return', False) else 'выключен'}`\n"
            f"Polling: задача `{poll_status}`, последний успех: `{last_poll_success}`\n"
            f"Ошибки polling подряд: {poll_errors}; последняя категория: `{poll_error_category}`\n"
            f"Очередь: pending={pending}, dispatching={dispatching}, accepted={accepted}, "
            f"processing={processing}, failed={failed}, unknown={unknown}\n"
            f"Потреблено Hermes без подтверждения отдельного задания: {consumed}\n"
            f"Последняя категория ошибки: `{error_category}`\n"
            "Повторная обработка неоднозначных событий автоматически не выполняется."
        )

    def _command_list_text(self, *, welcome: bool = False) -> str:
        command_lines = "\n".join(
            f"/{item['name']} — {item['description']}"
            for item in self._max_commands()
        )
        heading = "Hermes готов к работе." if welcome else "Доступные команды Hermes:"
        usage = (
            "\n\n/reasoning — выбор уровня; /reasoning high — задать уровень в этой сессии."
            "\n/reasoning off — скрыть вывод; /reasoning none — отключить рассуждения."
            "\n/goal status — состояние цели; /goal <текст> — задать цель."
            "\n/commands 2 — следующая страница полного каталога."
            "\n--global меняет настройку всего профиля; без него уровень задаётся для сессии."
        )
        return f"{heading}\n\n{command_lines}{usage}\n\nВыберите действие:"[:MAX_MESSAGE_LENGTH]

    async def _send_menu(
        self,
        chat_id: str,
        user_id: str,
        *,
        target_type: str,
        welcome: bool = False,
    ) -> None:
        if self._client is None:
            return
        label_map = {
            "commands": "Команды",
            "help": "Помощь",
            "status": "Статус",
            "new": "Новая сессия",
            "model": "Модель",
            "reasoning": "Уровень рассуждений",
            "goal": "Цель",
            "stop": "Остановить",
            "compress": "Сжать контекст",
            "maxstatus": "Диагностика MAX",
        }
        available = {item["name"] for item in self._max_commands()}
        button_names = [
            name
            for name in (
                "commands", "help", "status", "new", "model", "reasoning", "goal", "stop", "compress", "maxstatus"
            )
            if name in available
        ]
        rows: list[list[Mapping[str, str]]] = []
        buttons: list[Mapping[str, str]] = []
        for name in button_names:
            bound_user = "*" if target_type == "chat" and name in GROUP_ADMIN_COMMANDS else str(user_id)
            payload = self._callbacks.issue(
                "command",
                name,
                user_id=bound_user,
                chat_id=str(chat_id),
                session_key="",
            )
            buttons.append({"type": "callback", "text": label_map[name], "payload": payload})
        for index in range(0, len(buttons), 2):
            rows.append(buttons[index : index + 2])
        text = self._command_list_text(welcome=welcome)
        try:
            await self._rate_limiter.acquire(str(chat_id))

            async def _send() -> Mapping[str, Any]:
                return await self._client.send_message(
                    str(chat_id),
                    text,
                    target_type=target_type,
                    attachments=[build_inline_keyboard(rows)] if rows else None,
                )

            await with_backoff(
                _send,
                is_rate_limit=_is_max_rate_limit,
                extract_retry_after=_retry_after,
            )
        except MaxApiError as exc:
            logger.warning("MAX menu delivery failed: %s", exc)

    def _interactive_context(
        self, chat_id: str, metadata: Optional[Mapping[str, Any]] = None
    ) -> Optional[tuple[str, str]]:
        target_type = self._target_type_for(chat_id, metadata)
        user_id = ""
        if metadata:
            for key in ("max_user_id", "user_id", "sender_id"):
                if metadata.get(key):
                    user_id = str(metadata[key]).strip()
                    break
        if not user_id and target_type == "user":
            user_id = str(chat_id).strip()
        if not user_id and target_type == "chat":
            remembered = getattr(self, "_last_group_users", {}).get(str(chat_id))
            if remembered and time.monotonic() - remembered[1] <= 600.0:
                user_id = remembered[0]
        if not user_id:
            return None
        return target_type, user_id

    async def _send_interactive_prompt(
        self,
        chat_id: str,
        content: str,
        rows: list[list[Mapping[str, str]]],
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        if not content:
            return SendResult(success=False, error="MAX message is empty")
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        context = self._interactive_context(chat_id, metadata)
        if context is None:
            return SendResult(
                success=False,
                error="MAX native buttons require a user-bound message",
            )
        target_type, _user_id = context
        try:
            await self._rate_limiter.acquire(transport_chat_id)

            async def _send() -> Mapping[str, Any]:
                return await self._client.send_message(
                    transport_chat_id,
                    content[:MAX_MESSAGE_LENGTH],
                    target_type=target_type,
                    link=_reply_link(reply_to),
                    attachments=[build_inline_keyboard(rows)],
                )

            response = await with_backoff(
                _send,
                is_rate_limit=_is_max_rate_limit,
                extract_retry_after=_retry_after,
            )
        except MaxApiError as exc:
            return SendResult(
                success=False,
                error=str(exc),
                retryable=exc.retryable,
                retry_after=exc.retry_after,
            )
        return SendResult(
            success=True,
            message_id=_response_message_id(response),
            raw_response=response,
        )

    def _validated_media_path(self, file_path: str | Path) -> Optional[str]:
        validator = getattr(self, "validate_media_delivery_path", None)
        if callable(validator):
            try:
                safe_path = validator(str(file_path))
            except (OSError, RuntimeError, ValueError):
                return None
            return str(safe_path) if safe_path else None
        try:
            path = Path(file_path).expanduser().resolve(strict=True)
        except (OSError, RuntimeError, ValueError):
            return None
        return str(path) if path.is_file() else None

    async def _send_attachment_with_caption(
        self,
        chat_id: str,
        attachment: Mapping[str, Any],
        *,
        caption: Optional[str],
        reply_to: Optional[str],
        metadata: Optional[Mapping[str, Any]],
    ) -> Any:
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        target_type = self._target_type_for(transport_chat_id, metadata)
        chunks = split_message(str(caption or ""), MAX_MESSAGE_LENGTH) if caption else [""]
        message_ids: list[str] = []
        errors: list[str] = []
        unsupported_audio_rejected = False
        attachment_type = str(attachment.get("type") or "").strip().lower()
        for index, chunk in enumerate(chunks):
            await self._rate_limiter.acquire(transport_chat_id)
            kwargs: dict[str, Any] = {"target_type": target_type}
            if index == 0:
                kwargs["link"] = _reply_link(reply_to)
                kwargs["attachments"] = [attachment]
            try:
                response = await with_backoff(
                    lambda: self._client.send_message(transport_chat_id, chunk, **kwargs),
                    is_rate_limit=_is_media_send_retryable if index == 0 else _is_max_rate_limit,
                    extract_retry_after=_media_retry_after if index == 0 else _retry_after,
                    max_attempts=5,
                )
            except MaxApiError as exc:
                errors.append(str(exc))
                if index == 0:
                    unsupported_audio_rejected = (
                        attachment_type == "audio" and _is_unsupported_audio_error(exc)
                    )
                    # The first request carries the attachment. Do not emit
                    # later caption chunks as though the attachment succeeded.
                    break
                continue
            message_id = _response_message_id(response)
            if message_id:
                message_ids.append(message_id)

        if errors and not message_ids:
            return SendResult(
                success=False,
                error="; ".join(errors),
                error_kind=(
                    "unsupported_audio_format"
                    if unsupported_audio_rejected
                    else "media_send"
                ),
            )
        result = _send_result_from_ids(message_ids)
        if errors:
            return SendResult(
                success=False,
                message_id=getattr(result, "message_id", None),
                continuation_message_ids=getattr(result, "continuation_message_ids", ()),
                error="Подпись доставлена не полностью: " + "; ".join(errors),
                error_kind="partial_media",
            )
        return result

    async def _send_local_attachment(
        self,
        chat_id: str,
        file_path: str | Path,
        *,
        media_type: Optional[str] = None,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        force_document: bool = False,
    ) -> Any:
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        safe_path = self._validated_media_path(file_path)
        if not safe_path:
            return SendResult(success=False, error="MAX attachment path is not allowed")
        actual_type = media_type or media_type_for_file(
            safe_path, force_document=force_document
        )
        if actual_type not in {"image", "video", "audio", "file"}:
            return SendResult(success=False, error="MAX attachment type is not supported")
        try:
            upload_kwargs: dict[str, Any] = {
                "media_type": actual_type,
                "max_bytes": getattr(self, "_media_max_bytes", DEFAULT_MEDIA_MAX_BYTES),
                "mime_type": mime_type_for_file(safe_path, actual_type),
            }
            if file_name:
                upload_kwargs["filename"] = str(file_name)
            upload = await with_backoff(
                lambda: self._client.upload_media(safe_path, **upload_kwargs),
                is_rate_limit=_is_retryable_media_error,
                extract_retry_after=_media_retry_after,
                max_attempts=5,
            )
            token = str(upload.get("token") or "").strip()
            if not token:
                raise MaxApiError("MAX media upload returned no attachment token")
            attachment = {
                "type": actual_type,
                "payload": {"token": token},
            }
            result = await self._send_attachment_with_caption(
                chat_id,
                attachment,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
            if (
                actual_type == "audio"
                and not result.success
                and getattr(result, "error_kind", None) == "unsupported_audio_format"
            ):
                fallback_caption = (
                    f"{caption}\n\n{UNSUPPORTED_AUDIO_FALLBACK_MESSAGE}"
                    if caption
                    else UNSUPPORTED_AUDIO_FALLBACK_MESSAGE
                )
                return await self._send_local_attachment(
                    chat_id,
                    safe_path,
                    media_type="file",
                    caption=fallback_caption,
                    file_name=file_name,
                    reply_to=reply_to,
                    metadata=metadata,
                    force_document=True,
                )
            return result
        except (MaxApiError, OSError, ValueError) as exc:
            if actual_type == "audio" and isinstance(exc, MaxApiError) and _is_unsupported_audio_error(exc):
                fallback_caption = (
                    f"{caption}\n\n{UNSUPPORTED_AUDIO_FALLBACK_MESSAGE}"
                    if caption
                    else UNSUPPORTED_AUDIO_FALLBACK_MESSAGE
                )
                return await self._send_local_attachment(
                    chat_id,
                    safe_path,
                    media_type="file",
                    caption=fallback_caption,
                    file_name=file_name,
                    reply_to=reply_to,
                    metadata=metadata,
                    force_document=True,
                )
            return SendResult(
                success=False,
                error=str(exc),
                retryable=bool(getattr(exc, "retryable", False)),
                retry_after=getattr(exc, "retry_after", None),
            )

    async def _send_remote_image(
        self,
        chat_id: str,
        image_url: str,
        *,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        if not _safe_https_url(image_url) or not is_allowed_outbound_media_url(image_url):
            return SendResult(success=False, error="MAX image URL is not allowed")
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        try:
            data, remote_mime = await self._client.download_media(
                image_url,
                max_bytes=getattr(self, "_media_max_bytes", DEFAULT_MEDIA_MAX_BYTES),
                allowed_hosts=OUTBOUND_MEDIA_HOSTS,
            )
            filename = Path(urlsplit(image_url).path).name or "image.jpg"
            upload = await with_backoff(
                lambda: self._client.upload_media_bytes(
                    data,
                    filename=filename,
                    media_type="image",
                    max_bytes=getattr(self, "_media_max_bytes", DEFAULT_MEDIA_MAX_BYTES),
                    mime_type=remote_mime if str(remote_mime).startswith("image/") else "image/jpeg",
                ),
                is_rate_limit=_is_retryable_media_error,
                extract_retry_after=_media_retry_after,
                max_attempts=5,
            )
            token = str(upload.get("token") or "").strip()
            if not token:
                raise MaxApiError("MAX media upload returned no attachment token")
            attachment = {"type": "image", "payload": {"token": token}}
            return await self._send_attachment_with_caption(
                chat_id,
                attachment,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        except (MaxApiError, OSError, ValueError) as exc:
            return SendResult(
                success=False,
                error=str(exc),
                retryable=bool(getattr(exc, "retryable", False)),
                retry_after=getattr(exc, "retry_after", None),
            )

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        value = str(image_url or "").strip()
        if value.startswith("file://"):
            return await self.send_image_file(
                chat_id,
                unquote(value[7:]),
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        if not urlsplit(value).scheme:
            return await self.send_image_file(
                chat_id,
                value,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        return await self._send_remote_image(
            chat_id, value, caption=caption, reply_to=reply_to, metadata=metadata
        )

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        return await self.send_image(
            chat_id,
            animation_url,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        del kwargs
        return await self._send_local_attachment(
            chat_id,
            image_path,
            media_type="image",
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        del kwargs
        return await self._send_local_attachment(
            chat_id,
            file_path,
            media_type="file",
            caption=caption,
            file_name=file_name,
            reply_to=reply_to,
            metadata=metadata,
            force_document=True,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        del kwargs
        return await self._send_local_attachment(
            chat_id,
            audio_path,
            media_type="audio",
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        del kwargs
        return await self._send_local_attachment(
            chat_id,
            video_path,
            media_type="video",
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_multiple_images(
        self,
        chat_id: str,
        images: list[tuple[str, str]],
        metadata: Optional[Dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> Any:
        if len(images) > MAX_ATTACHMENTS_PER_MESSAGE:
            logger.info(
                "MAX image batch has %d items; sending in separate messages",
                len(images),
            )
        message_ids: list[str] = []
        failures: list[dict[str, Any]] = []
        delivered = 0
        for index, (image_url, alt_text) in enumerate(images, start=1):
            if human_delay > 0:
                await asyncio.sleep(human_delay)
            try:
                result = await self.send_image(
                    chat_id,
                    image_url,
                    caption=alt_text or None,
                    metadata=metadata,
                )
            except Exception as exc:  # noqa: BLE001 - continue the remaining image batch
                failures.append({"index": index, "error": f"{type(exc).__name__}: {exc}"})
                logger.warning("MAX image delivery failed at item %d: %s", index, exc)
                continue
            message_id = getattr(result, "message_id", None)
            if getattr(result, "success", False) and message_id:
                delivered += 1
                message_ids.extend(
                    str(item)
                    for item in getattr(result, "continuation_message_ids", ())
                    if item
                )
                message_ids.append(str(message_id))
            else:
                error = str(
                    getattr(result, "error", None)
                    or "MAX did not confirm the image message ID"
                )
                failures.append({"index": index, "error": error})
                logger.warning("MAX image delivery failed: %s", getattr(result, "error", result))
        if not delivered:
            if failures:
                await self._notify_media_delivery_issue(
                    chat_id,
                    "Не удалось доставить изображения "
                    + ", ".join(f"№{item['index']}" for item in failures)
                    + ".",
                    metadata=metadata,
                )
            return SendResult(
                success=False,
                error="; ".join(item["error"] for item in failures) or "MAX image list is empty",
                raw_response={"delivered": 0, "failed_images": failures},
                error_kind="media_send",
            )
        result = _send_result_from_ids(message_ids)
        if failures:
            await self._notify_media_delivery_issue(
                chat_id,
                "Не удалось доставить изображения "
                + ", ".join(f"№{item['index']}" for item in failures)
                + ". Остальные изображения отправлены.",
                metadata=metadata,
            )
            return SendResult(
                success=True,
                message_id=getattr(result, "message_id", None),
                continuation_message_ids=getattr(result, "continuation_message_ids", ()),
                error="Часть изображений не доставлена: "
                + "; ".join(f"#{item['index']}: {item['error']}" for item in failures),
                raw_response={"delivered": delivered, "failed_images": failures},
                error_kind="partial_media",
            )
        return SendResult(
            success=True,
            message_id=getattr(result, "message_id", None),
            continuation_message_ids=getattr(result, "continuation_message_ids", ()),
            raw_response={"delivered": delivered, "failed_images": []},
        )

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        if self._client is None:
            return
        if self._target_type_for(transport_chat_id, metadata) == "chat":
            action_chat_id = transport_chat_id
        else:
            action_chat_id = getattr(self, "_dm_action_chat_ids", {}).get(transport_chat_id)
        if not action_chat_id:
            return
        try:
            await self._rate_limiter.acquire(transport_chat_id)
            await with_backoff(
                lambda: self._client.send_action(action_chat_id, "typing_on"),
                is_rate_limit=_is_max_rate_limit,
                extract_retry_after=_retry_after,
            )
        except (MaxApiError, ValueError) as exc:
            logger.debug("MAX typing_on action failed (status=%s)", getattr(exc, "status_code", None))

    async def stop_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Hermes stops renewal; MAX documents no explicit typing_off action."""

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        if not content:
            return SendResult(success=False, error="MAX message is empty")
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        media_files, cleaned_content = self._extract_outbound_media(content)
        if media_files:
            return await self._send_media_files(
                chat_id,
                cleaned_content,
                media_files,
                reply_to=reply_to,
                metadata=metadata,
                force_document="[[as_document]]" in content,
            )
        if "MEDIA:" in content and callable(getattr(self, "extract_media", None)):
            return SendResult(success=False, error="MAX media directive contains no safe local file")
        if "MEDIA:" in content and not callable(getattr(self, "extract_media", None)):
            return SendResult(
                success=False,
                error="MAX media extraction is unavailable in this Hermes runtime",
                retryable=False,
            )

        target_type = self._target_type_for(transport_chat_id, metadata)
        chunks = _split_preserving_content(content, MAX_MESSAGE_LENGTH)
        message_ids: list[str] = []
        for index, chunk in enumerate(chunks):
            link = _reply_link(reply_to) if index == 0 else None
            await self._rate_limiter.acquire(transport_chat_id)

            async def _send() -> Mapping[str, Any]:
                return await self._client.send_message(
                    transport_chat_id,
                    chunk,
                    target_type=target_type,
                    link=link,
                )

            try:
                response = await with_backoff(
                    _send,
                    is_rate_limit=_is_max_rate_limit,
                    extract_retry_after=_retry_after,
                )
            except MaxApiError as exc:
                raw_response: dict[str, Any] = {
                    "acknowledged_message_ids": list(message_ids),
                    "continuation_message_ids": list(message_ids[:-1]),
                }
                if message_ids:
                    raw_response["last_message_id"] = message_ids[-1]
                    raw_response["delivered_prefix"] = "".join(chunks[:index])
                if exc.outcome_uncertain:
                    raw_response["delivery_uncertain"] = True
                elif message_ids:
                    raw_response["partial_overflow"] = True
                return SendResult(
                    success=False,
                    error=str(exc),
                    retryable=exc.retryable and not exc.outcome_uncertain,
                    retry_after=exc.retry_after,
                    message_id=message_ids[-1] if message_ids else None,
                    continuation_message_ids=tuple(message_ids[:-1]),
                    raw_response=raw_response,
                    error_kind="delivery_uncertain" if exc.outcome_uncertain else "partial_delivery",
                )
            message_id = _response_message_id(response)
            if not message_id:
                raw_response = {
                    "delivery_uncertain": True,
                    "acknowledged_message_ids": list(message_ids),
                    "continuation_message_ids": list(message_ids[:-1]),
                }
                if message_ids:
                    raw_response["last_message_id"] = message_ids[-1]
                    raw_response["delivered_prefix"] = "".join(chunks[:index])
                return SendResult(
                    success=False,
                    error="MAX response did not confirm message creation",
                    message_id=message_ids[-1] if message_ids else None,
                    continuation_message_ids=tuple(message_ids[:-1]),
                    raw_response=raw_response,
                    error_kind="delivery_uncertain",
                )
            message_ids.append(message_id)
        return _send_result_from_ids(message_ids)

    def _send_retry_is_final(self, result: Any) -> bool:
        raw = getattr(result, "raw_response", None)
        return isinstance(raw, Mapping) and bool(raw.get("delivery_uncertain"))

    def _extract_outbound_media(self, content: str) -> tuple[list, str]:
        if "MEDIA:" not in content:
            return [], content
        extractor = getattr(self, "extract_media", None)
        if not callable(extractor):
            return [], content
        try:
            media_files, cleaned = extractor(content)
            filter_paths = getattr(self, "filter_media_delivery_paths", None)
            if callable(filter_paths):
                media_files = filter_paths(media_files)
            return list(media_files or []), str(cleaned or "")
        except Exception:  # noqa: BLE001
            logger.exception("MAX outbound media extraction failed")
            return [], content

    async def _notify_media_delivery_issue(
        self,
        chat_id: str,
        notice: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if getattr(self, "_client", None) is None:
            return
        try:
            result = await self.send(chat_id, notice, metadata=metadata)
            if not getattr(result, "success", False):
                logger.warning("MAX media delivery notice was not delivered")
        except Exception:  # noqa: BLE001 - preserve the original media result
            logger.exception("MAX media delivery notice failed")

    async def _send_media_files(
        self,
        chat_id: str,
        content: str,
        media_files: list,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        force_document: bool = False,
    ) -> Any:
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        target_type = self._target_type_for(transport_chat_id, metadata)
        attachments: list[Mapping[str, Any]] = []
        if not media_files:
            return SendResult(success=False, error="MAX media attachment list is empty")
        upload_errors: list[str] = []
        for item in media_files:
            try:
                if isinstance(item, (tuple, list)):
                    media_path = str(item[0])
                    is_voice = bool(item[1]) if len(item) > 1 else False
                else:
                    media_path = str(item)
                    is_voice = False
                media_type = media_type_for_file(
                    media_path,
                    is_voice=is_voice,
                    force_document=force_document,
                )
                upload = await with_backoff(
                    lambda: self._client.upload_media(
                        media_path,
                        media_type=media_type,
                        max_bytes=getattr(self, "_media_max_bytes", DEFAULT_MEDIA_MAX_BYTES),
                        mime_type=mime_type_for_file(media_path, media_type),
                    ),
                    is_rate_limit=_is_retryable_media_error,
                    extract_retry_after=_media_retry_after,
                    max_attempts=5,
                )
                if not isinstance(upload, Mapping):
                    raise MaxApiError("MAX media upload returned an invalid response")
                token = str(upload.get("token") or "").strip()
                if not token:
                    raise MaxApiError("MAX media upload returned no attachment token")
                attachments.append({"type": media_type, "payload": {"token": token}})
            except (MaxApiError, OSError, TypeError, ValueError) as exc:
                upload_errors.append(str(exc))

        if not attachments:
            if upload_errors:
                await self._notify_media_delivery_issue(
                    chat_id,
                    f"Не удалось загрузить ни одно из {len(media_files)} вложений.",
                    metadata=metadata,
                )
            return SendResult(
                success=False,
                error="; ".join(upload_errors) or "MAX media uploads failed",
                error_kind="media_upload",
            )

        chunks = split_message(content, MAX_MESSAGE_LENGTH) if content else [""]
        attachment_batches = _attachment_batches(attachments)
        message_ids: list[str] = []
        send_errors: list[str] = []
        for batch_index, batch in enumerate(attachment_batches):
            await self._rate_limiter.acquire(transport_chat_id)
            chunk = chunks[0] if batch_index == 0 else ""
            link = _reply_link(reply_to) if batch_index == 0 else None

            async def _send() -> Mapping[str, Any]:
                return await self._client.send_message(
                    transport_chat_id,
                    chunk,
                    target_type=target_type,
                    link=link,
                    attachments=batch,
                )

            try:
                response = await with_backoff(
                    _send,
                    is_rate_limit=_is_retryable_media_error,
                    extract_retry_after=_media_retry_after,
                    max_attempts=5,
                )
            except MaxApiError as exc:
                send_errors.append(str(exc))
                continue
            message_id = _response_message_id(response)
            if message_id:
                message_ids.append(message_id)

        if message_ids:
            for chunk in chunks[1:]:
                await self._rate_limiter.acquire(transport_chat_id)

                async def _send_continuation() -> Mapping[str, Any]:
                    return await self._client.send_message(
                        transport_chat_id,
                        chunk,
                        target_type=target_type,
                    )

                try:
                    response = await with_backoff(
                        _send_continuation,
                        is_rate_limit=_is_max_rate_limit,
                        extract_retry_after=_retry_after,
                    )
                except MaxApiError as exc:
                    send_errors.append(str(exc))
                    continue
                message_id = _response_message_id(response)
                if message_id:
                    message_ids.append(message_id)

        if not message_ids:
            if upload_errors or send_errors:
                await self._notify_media_delivery_issue(
                    chat_id,
                    f"Не удалось доставить вложения ({len(upload_errors)} ошибок загрузки, "
                    f"{len(send_errors)} ошибок отправки).",
                    metadata=metadata,
                )
            return SendResult(
                success=False,
                error="; ".join(upload_errors + send_errors)
                or "MAX media messages failed",
                error_kind="media_send",
            )
        result = _send_result_from_ids(message_ids)
        if upload_errors or send_errors:
            await self._notify_media_delivery_issue(
                chat_id,
                f"Часть вложений не доставлена ({len(upload_errors)} ошибок загрузки, "
                f"{len(send_errors)} ошибок отправки). Остальные отправлены.",
                metadata=metadata,
            )
            return SendResult(
                success=False,
                message_id=getattr(result, "message_id", None),
                continuation_message_ids=getattr(result, "continuation_message_ids", ()),
                raw_response=getattr(result, "raw_response", None),
                error="Часть вложений не доставлена: "
                + "; ".join(upload_errors + send_errors),
                error_kind="partial_media",
            )
        return result

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if not choices:
            return await self.send(chat_id, f"❓ {question}", metadata=metadata)

        context = self._interactive_context(chat_id, metadata)
        if context is None:
            return SendResult(
                success=False,
                error="MAX native buttons require a user-bound message",
            )
        _target_type, user_id = context
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        rows: list[list[Mapping[str, str]]] = []
        option_lines = [f"{index + 1}. {choice}" for index, choice in enumerate(choices)]
        for index, _choice in enumerate(choices):
            payload = self._callbacks.issue(
                "clarify",
                f"{clarify_id}:{index}",
                user_id=user_id,
                chat_id=transport_chat_id,
                session_key=session_key,
            )
            rows.append([{"type": "callback", "text": str(index + 1), "payload": payload}])
        other_payload = self._callbacks.issue(
            "clarify",
            f"{clarify_id}:other",
            user_id=user_id,
            chat_id=transport_chat_id,
            session_key=session_key,
        )
        rows.append([{"type": "callback", "text": "Другое", "payload": other_payload}])
        return await self._send_interactive_prompt(
            transport_chat_id,
            "❓ " + str(question) + "\n\n" + "\n".join(option_lines),
            rows,
            metadata=metadata,
        )

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
        allow_permanent: bool = True,
        allow_session: bool = True,
        smart_denied: bool = False,
    ) -> Any:
        del smart_denied
        context = self._interactive_context(chat_id, metadata)
        if context is None:
            return SendResult(
                success=False,
                error="MAX native buttons require a user-bound message",
            )
        _target_type, user_id = context
        callback_user_id = "*" if _target_type == "chat" else user_id
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        choices: list[tuple[str, str]] = [("once", "Разрешить один раз")]
        if allow_session:
            choices.append(("session", "Разрешить на сессию"))
        if allow_permanent:
            choices.append(("always", "Разрешить всегда"))
        choices.append(("deny", "Запретить"))
        rows: list[list[Mapping[str, str]]] = []
        for index in range(0, len(choices), 2):
            row: list[Mapping[str, str]] = []
            for choice, label in choices[index : index + 2]:
                payload = self._callbacks.issue(
                    "approval",
                    choice,
                    user_id=callback_user_id,
                    chat_id=transport_chat_id,
                    session_key=session_key,
                )
                row.append({"type": "callback", "text": label, "payload": payload})
            rows.append(row)
        preview = str(command)
        if len(preview) > 3200:
            preview = preview[:3200] + "..."
        text = f"⚠️ Требуется подтверждение команды:\n\n```\n{preview}\n```\nПричина: {description}"
        return await self._send_interactive_prompt(transport_chat_id, text, rows, metadata=metadata)

    async def send_slash_confirm(
        self,
        chat_id: str,
        title: str,
        message: str,
        session_key: str,
        confirm_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        context = self._interactive_context(chat_id, metadata)
        if context is None:
            return SendResult(
                success=False,
                error="MAX native buttons require a user-bound message",
            )
        _target_type, user_id = context
        callback_user_id = "*" if _target_type == "chat" else user_id
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        choices = (
            ("once", "Подтвердить один раз"),
            ("always", "Подтверждать всегда"),
            ("cancel", "Отмена"),
        )
        rows: list[list[Mapping[str, str]]] = []
        for choice, label in choices:
            payload = self._callbacks.issue(
                "slash",
                f"{choice}:{confirm_id}",
                user_id=callback_user_id,
                chat_id=transport_chat_id,
                session_key=session_key,
            )
            rows.append([{"type": "callback", "text": label, "payload": payload}])
        return await self._send_interactive_prompt(
            transport_chat_id,
            f"**{title}**\n\n{message}",
            rows,
            metadata=metadata,
        )

    def _note_choice_request(self, chat_id: str, user_id: str) -> str:
        if not hasattr(self, "_choice_versions"):
            self._choice_versions = {}
            self._choice_pickers = {}
        route = (str(chat_id), str(user_id))
        for key, state in list(self._choice_pickers.items()):
            if state["route"] == route or state["expires_at"] <= time.monotonic():
                self._choice_pickers.pop(key, None)
        if len(self._choice_versions) >= 512 and route not in self._choice_versions:
            self._choice_versions.pop(next(iter(self._choice_versions)))
        version = token_urlsafe(12)
        self._choice_versions[route] = version
        return version

    async def send_choice_picker(
        self, chat_id: str, title: str, choices: list, session_key: str,
        on_choice_selected, metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Implement Hermes' native finite-choice UI without sending a new LLM turn."""
        if (not callable(on_choice_selected) or not isinstance(choices, list)
                or not 1 <= len(choices) <= 58
                or any(not isinstance(c, Mapping) or not isinstance(c.get("value"), str)
                       or not c["value"] for c in choices)):
            return SendResult(success=False, error="Некорректные варианты выбора MAX")
        context = COMMAND_CONTEXT.get() or {}
        supplied = context.get("metadata", {}) if context.get("adapter") == id(self) else {}
        merged = {**supplied, **(metadata or {})}
        if context.get("adapter") == id(self) and context.get("user_id"):
            merged["max_user_id"] = context["user_id"]
        bound = self._interactive_context(chat_id, merged)
        if bound is None:
            return SendResult(success=False, error="Выбор MAX требует привязки к пользователю")
        target_type, user_id = bound
        if target_type == "chat" and not self._access.is_admin(user_id):
            return SendResult(success=False, error="Выбор доступен только администратору группы")
        transport = _transport_chat_id(chat_id, merged)
        route = (transport, user_id)
        if not hasattr(self, "_choice_versions"):
            self._choice_versions, self._choice_pickers = {}, {}
        version = merged.get("max_request_version") or self._choice_versions.get(route)
        if version and self._choice_versions.get(route) != version:
            return SendResult(success=False, error="Запрос выбора уже устарел")
        version = version or self._note_choice_request(*route)
        for key, state in list(self._choice_pickers.items()):
            if state["expires_at"] <= time.monotonic() or state["route"] == route:
                self._choice_pickers.pop(key, None)
        while len(self._choice_pickers) >= 8:
            self._choice_pickers.pop(next(iter(self._choice_pickers)))
        picker_id = token_urlsafe(9)
        state = {"route": route, "version": version, "session_key": str(session_key),
                 "choices": [dict(c) for c in choices], "callback": on_choice_selected,
                 "expires_at": time.monotonic() + 600, "message_id": None}
        self._choice_pickers[picker_id] = state
        rendered = []
        for index, choice in enumerate(choices):
            label = choice_label(choice["value"], str(choice.get("label") or choice["value"]))
            if choice.get("is_current"):
                label = "✓ " + label
            payload = self._callbacks.issue("choice", f"{picker_id}:{index}", user_id=user_id,
                chat_id=transport, session_key=str(session_key))
            rendered.append({"type": "callback", "text": label[:60], "payload": payload})
        rows = [rendered[i:i + 2] for i in range(0, len(rendered), 2)]
        cancel = self._callbacks.issue("choice", f"{picker_id}:cancel", user_id=user_id,
            chat_id=transport, session_key=str(session_key))
        rows.append([{"type": "callback", "text": "Отмена", "payload": cancel}])
        result = await self._send_interactive_prompt(transport, str(title), rows, metadata=merged)
        if not getattr(result, "success", False) or not getattr(result, "message_id", None):
            self._choice_pickers.pop(picker_id, None)
            return result if not getattr(result, "success", False) else SendResult(
                success=False, error="MAX не подтвердил сообщение выбора")
        state["message_id"] = str(result.message_id)
        return result

    async def _dispatch_choice_callback(self, callback: MaxCallback, entry: Any) -> None:
        picker_id, _, selection = entry.value.partition(":")
        state = getattr(self, "_choice_pickers", {}).get(picker_id)
        if (state is None or state["expires_at"] <= time.monotonic()
                or state["route"] != (callback.chat_id, callback.user_id)
                or state["session_key"] != entry.session_key
                or state["message_id"] != callback.message_id
                or self._choice_versions.get(state["route"]) != state["version"]):
            await self._answer_callback(callback, "Выбор устарел. Вызовите команду заново.")
            return
        if selection != "cancel" and (not selection.isdigit() or int(selection) >= len(state["choices"])):
            await self._answer_callback(callback, "Некорректный вариант выбора.")
            return
        # Pop before awaiting: simultaneous taps on different buttons apply at most one choice.
        self._choice_pickers.pop(picker_id, None)
        if selection == "cancel":
            await self._answer_callback(callback, "Выбор отменён.", attachments=[])
            return
        try:
            with russian_commands():
                result = state["callback"](callback.chat_id, state["choices"][int(selection)]["value"])
                if inspect.isawaitable(result):
                    result = await result
            text = str(result or "Выбор применён.")
        except Exception as exc:
            logger.warning("MAX choice callback failed (%s)", type(exc).__name__)
            text = "Не удалось применить выбор. Вызовите команду заново."
        await self._answer_callback(callback, text, attachments=[])

    async def send_model_picker(
        self,
        chat_id: str,
        providers: list,
        current_model: str,
        current_provider: str,
        session_key: str,
        on_model_selected,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        context = self._interactive_context(chat_id, metadata)
        if context is None:
            return SendResult(
                success=False,
                error="MAX native buttons require a user-bound message",
            )
        _target_type, user_id = context
        callback_user_id = "*" if _target_type == "chat" else user_id
        transport_chat_id = _transport_chat_id(chat_id, metadata)
        normalized = [item for item in providers if isinstance(item, Mapping)]
        if not normalized:
            return SendResult(success=False, error="MAX model provider list is empty")

        picker_id = token_urlsafe(9)
        self._model_pickers[picker_id] = {
            "providers": normalized,
            "current_model": str(current_model or ""),
            "current_provider": str(current_provider or ""),
            "selected_provider": "",
            "selected_provider_name": "",
            "model_list": [],
            "on_model_selected": on_model_selected,
            "session_key": session_key,
            "user_id": user_id,
            "callback_user_id": callback_user_id,
            "chat_id": transport_chat_id,
        }
        state = self._model_pickers[picker_id]
        rows = self._model_provider_rows(state, picker_id, None)
        result = await self._send_interactive_prompt(
            transport_chat_id,
            self._model_picker_provider_text(state),
            rows,
            metadata=metadata,
        )
        if not getattr(result, "success", False):
            self._model_pickers.pop(picker_id, None)
        return result

    def _model_provider_rows(
        self,
        state: Mapping[str, Any],
        picker_id: str,
        callback: Optional[MaxCallback],
    ) -> list[list[Mapping[str, str]]]:
        chat_id = callback.chat_id if callback else ""
        user_id = callback.user_id if callback else str(state.get("callback_user_id") or state["user_id"])
        rows: list[list[Mapping[str, str]]] = []
        buttons: list[Mapping[str, str]] = []
        for provider in state["providers"][:20]:
            slug = str(provider.get("slug") or "").strip()
            if not slug:
                continue
            name = str(provider.get("name") or slug)
            count = provider.get("total_models", len(provider.get("models", [])))
            label = f"{name} ({count})"
            if slug == state.get("current_provider"):
                label = f"✓ {label}"
            payload = self._callbacks.issue(
                "model",
                f"{picker_id}:provider:{slug}",
                user_id=user_id,
                chat_id=chat_id or str(state.get("chat_id") or "user"),
                session_key=str(state.get("session_key") or ""),
            )
            buttons.append({"type": "callback", "text": label[:60], "payload": payload})
        for index in range(0, len(buttons), 2):
            rows.append(buttons[index : index + 2])
        rows.append([self._model_cancel_button(picker_id, user_id, chat_id, state)])
        return rows

    def _model_rows(
        self,
        state: Mapping[str, Any],
        picker_id: str,
        callback: MaxCallback,
    ) -> list[list[Mapping[str, str]]]:
        rows: list[list[Mapping[str, str]]] = []
        buttons: list[Mapping[str, str]] = []
        for index, model_id in enumerate(state.get("model_list", [])[:50]):
            label = str(model_id).rsplit("/", 1)[-1]
            if len(label) > 40:
                label = label[:37] + "..."
            payload = self._callbacks.issue(
                "model",
                f"{picker_id}:model:{index}",
                user_id=callback.user_id,
                chat_id=callback.chat_id,
                session_key=str(state.get("session_key") or ""),
            )
            buttons.append({"type": "callback", "text": label, "payload": payload})
        for index in range(0, len(buttons), 2):
            rows.append(buttons[index : index + 2])
        back_payload = self._callbacks.issue(
            "model",
            f"{picker_id}:back",
            user_id=callback.user_id,
            chat_id=callback.chat_id,
            session_key=str(state.get("session_key") or ""),
        )
        rows.append([{"type": "callback", "text": "Назад", "payload": back_payload}])
        rows.append([self._model_cancel_button(picker_id, callback.user_id, callback.chat_id, state)])
        return rows

    def _model_cancel_button(
        self,
        picker_id: str,
        user_id: str,
        chat_id: str,
        state: Mapping[str, Any],
    ) -> Mapping[str, str]:
        payload = self._callbacks.issue(
            "model",
            f"{picker_id}:cancel",
            user_id=user_id,
            chat_id=chat_id or str(state.get("chat_id") or "user"),
            session_key=str(state.get("session_key") or ""),
        )
        return {"type": "callback", "text": "Отмена", "payload": payload}

    @staticmethod
    def _model_picker_provider_text(state: Mapping[str, Any]) -> str:
        model = state.get("current_model") or "неизвестна"
        provider = state.get("current_provider") or "неизвестен"
        return f"⚙️ Настройка модели\n\nТекущая модель: `{model}`\nПровайдер: {provider}\n\nВыберите провайдера:"

    @staticmethod
    def _model_picker_model_text(state: Mapping[str, Any]) -> str:
        provider = state.get("selected_provider_name") or state.get("selected_provider")
        models = state.get("model_list", [])
        return f"⚙️ Настройка модели\n\nПровайдер: {provider}\nДоступно моделей: {len(models)}\n\nВыберите модель:"

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> Any:
        del finalize
        if self._client is None:
            return SendResult(success=False, error="MAX adapter is not connected")
        transport_chat_id = _transport_chat_id(chat_id)
        try:
            await self._rate_limiter.acquire(transport_chat_id)
            response = await with_backoff(
                lambda: self._client.edit_message(message_id, content[:MAX_MESSAGE_LENGTH]),
                is_rate_limit=_is_max_rate_limit,
                extract_retry_after=_retry_after,
            )
        except MaxApiError as exc:
            return SendResult(
                success=False,
                error=str(exc),
                retryable=exc.retryable,
                retry_after=exc.retry_after,
            )
        return SendResult(success=True, message_id=message_id, raw_response=response)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        transport_chat_id = _transport_chat_id(chat_id)
        target_type = self._chat_target_types.get(transport_chat_id, "user")
        return {
            "name": transport_chat_id,
            "type": "group" if target_type == "chat" else "dm",
            "chat_id": transport_chat_id,
        }


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def check_requirements() -> bool:
    try:
        import httpx  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config: Any) -> bool:
    return bool(
        str(getattr(config, "token", None) or _max_env("MAX_BOT_TOKEN", "")).strip()
    )


def is_connected(config: Any) -> bool:
    return validate_config(config)


def env_enablement() -> Optional[Dict[str, Any]]:
    token = _max_env("MAX_BOT_TOKEN", "").strip()
    if not token:
        return None
    seed: Dict[str, Any] = {"enabled": True, "token": token}
    home = _max_env("MAX_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": home}
    return seed


def apply_yaml_config(_yaml_cfg: dict, platform_cfg: dict) -> dict[str, Any]:
    """Map plugin-owned ``platforms.max`` keys without overriding env vars."""

    cfg = platform_cfg if isinstance(platform_cfg, Mapping) else {}
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else cfg
    result: dict[str, Any] = {}
    try:
        from gateway.platforms._shared import get_scoped_secret, yaml_env_setter
    except ImportError:
        read_env = os.environ.get

        def set_env(name: str, value: str) -> None:
            if not os.environ.get(name):
                os.environ[name] = value
    else:
        read_env = get_scoped_secret
        set_env = yaml_env_setter()
    foreign_profile = False
    try:
        from hermes_constants import get_hermes_home, get_routing_process_hermes_home
    except ImportError:
        pass
    else:
        foreign_profile = get_hermes_home().resolve() != get_routing_process_hermes_home().resolve()
    env_map = {
        "api_base_url": "MAX_API_BASE_URL",
        "webhook_url": "MAX_WEBHOOK_URL",
        "webhook_secret": "MAX_WEBHOOK_SECRET",
        "ca_bundle": "MAX_CA_BUNDLE",
        "callback_ttl_seconds": "MAX_CALLBACK_TTL_SECONDS",
        "media_max_bytes": "MAX_MEDIA_MAX_BYTES",
        "require_mention": "MAX_REQUIRE_MENTION",
        "allow_from": "MAX_ALLOWED_USERS",
        "group_allow_from": "MAX_GROUP_ALLOWED_USERS",
        "group_allowed_chats": "MAX_GROUP_ALLOWED_CHATS",
        "allow_admin_from": "MAX_ADMIN_USERS",
    }
    for key, value in extra.items():
        if key in {"enabled", "token"}:
            continue
        env_name = env_map.get(key)
        if env_name and value is not None and not foreign_profile:
            if isinstance(value, (list, tuple, set)):
                encoded = ",".join(str(item) for item in value)
            else:
                encoded = str(value).lower() if isinstance(value, bool) else str(value)
            set_env(env_name, encoded)
        if key in {
            "api_base_url",
            "webhook_url",
            "webhook_secret",
            "ca_bundle",
            "callback_ttl_seconds",
            "media_max_bytes",
            "require_mention",
        }:
            result[key] = read_env(env_name, value) if env_name else value
        elif key in {"allow_from", "group_allow_from", "group_allowed_chats", "allow_admin_from"}:
            result[key] = value
        elif key in {"webhook_auto_fallback", "webhook_auto_return"}:
            result[key] = _truthy(value)
        elif key in {"inbox_path", "marker_path", "target_path"} and _truthy(extra.get("webhook_auto_fallback")):
            result[key] = value
    return result


async def standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    del thread_id
    token = str(getattr(pconfig, "token", None) or _max_env("MAX_BOT_TOKEN", "")).strip()
    if not token:
        return {"error": "MAX_BOT_TOKEN is not configured"}
    extra = _config_extra(pconfig)
    try:
        verify = tls_verify_from_env(
            {"MAX_CA_BUNDLE": str(extra.get("ca_bundle", _max_env("MAX_CA_BUNDLE", "")))}
        )
        client = MaxClient(
            token,
            base_url=str(extra.get("api_base_url") or _max_env("MAX_API_BASE_URL", DEFAULT_API_BASE)),
            verify=verify,
        )
        try:
            configured_groups = extra.get("group_allowed_chats", [])
            if isinstance(configured_groups, str):
                configured_groups = [item.strip() for item in configured_groups.split(",") if item.strip()]
            target_type = "chat" if str(chat_id) in set(configured_groups or []) else "user"
            target_path = str(
                extra.get("target_path")
                or _max_env("MAX_TARGET_PATH", _max_home() / "max" / "targets.sqlite3")
            )
            target_store = MaxTargetStore(target_path)
            try:
                target_type = target_store.get(str(chat_id)) or target_type
            finally:
                target_store.close()
            try:
                media_max_bytes = int(
                    extra.get(
                        "media_max_bytes",
                        _max_env("MAX_MEDIA_MAX_BYTES", str(DEFAULT_MEDIA_MAX_BYTES)),
                    )
                )
            except (TypeError, ValueError):
                media_max_bytes = DEFAULT_MEDIA_MAX_BYTES
            media_max_bytes = min(max(1, media_max_bytes), DEFAULT_MEDIA_MAX_BYTES)
            attachments: list[Mapping[str, Any]] = []
            raw_media_files = list(media_files or [])
            upload_errors: list[str] = []
            audio_sources: dict[str, str] = {}
            audio_fallback_used = False

            async def upload_attachment(path: str, attachment_type: str) -> Mapping[str, Any]:
                return await with_backoff(
                    lambda: client.upload_media(
                        path,
                        media_type=attachment_type,
                        max_bytes=media_max_bytes,
                        mime_type=mime_type_for_file(path, attachment_type),
                    ),
                    is_rate_limit=_is_retryable_media_error,
                    extract_retry_after=_media_retry_after,
                    max_attempts=5,
                )

            for media_path in raw_media_files:
                if isinstance(media_path, (tuple, list)):
                    path = str(media_path[0])
                    is_voice = bool(media_path[1]) if len(media_path) > 1 else False
                else:
                    path = str(media_path)
                    is_voice = False
                try:
                    safe_path = BasePlatformAdapter.validate_media_delivery_path(path)
                except (OSError, RuntimeError, ValueError):
                    safe_path = None
                if not safe_path:
                    upload_errors.append("MAX standalone attachment path is not allowed")
                    continue
                media_type = media_type_for_file(path, force_document=force_document)
                if is_voice and not force_document:
                    media_type = media_type_for_file(path, is_voice=True)
                try:
                    upload = await upload_attachment(safe_path, media_type)
                    token_value = str(upload.get("token") or "").strip()
                    if not token_value:
                        raise MaxApiError("MAX media upload returned no attachment token")
                    attachments.append({"type": media_type, "payload": {"token": token_value}})
                    if media_type == "audio":
                        audio_sources[token_value] = safe_path
                except MaxApiError as exc:
                    if media_type != "audio" or not _is_unsupported_audio_error(exc):
                        upload_errors.append(str(exc))
                        continue
                    try:
                        upload = await upload_attachment(safe_path, "file")
                        token_value = str(upload.get("token") or "").strip()
                        if not token_value:
                            raise MaxApiError("MAX media upload returned no attachment token")
                        attachments.append({"type": "file", "payload": {"token": token_value}})
                        audio_fallback_used = True
                    except (MaxApiError, OSError, TypeError, ValueError) as fallback_exc:
                        upload_errors.append(str(fallback_exc))
                except (OSError, TypeError, ValueError) as exc:
                    upload_errors.append(str(exc))
            chunks = split_message(message, MAX_MESSAGE_LENGTH) if message else []
            batches = _attachment_batches(attachments) if attachments else ([[]] if chunks else [])
            if not batches:
                return {"error": "; ".join(upload_errors) or "MAX media uploads failed"}

            message_ids: list[str] = []
            send_errors: list[str] = []
            sent_count = 0
            for batch_index, batch in enumerate(batches):
                chunk = chunks[0] if batch_index == 0 and chunks else ""
                try:
                    request = lambda: client.send_message(
                        str(chat_id),
                        chunk,
                        target_type=target_type,
                        **({"attachments": batch} if batch else {}),
                    )
                    response = await with_backoff(
                        request,
                        is_rate_limit=_is_media_send_retryable,
                        extract_retry_after=_media_retry_after,
                        max_attempts=5,
                    )
                    sent_count += 1
                    message_id = _response_message_id(response)
                    if message_id:
                        message_ids.append(message_id)
                except MaxApiError as exc:
                    if (
                        len(batch) == 1
                        and str(batch[0].get("type") or "").lower() == "audio"
                        and _is_unsupported_audio_error(exc)
                    ):
                        audio_token = str(
                            (batch[0].get("payload") or {}).get("token") or ""
                        )
                        source_path = audio_sources.get(audio_token)
                        if source_path:
                            try:
                                upload = await upload_attachment(source_path, "file")
                                token_value = str(upload.get("token") or "").strip()
                                if not token_value:
                                    raise MaxApiError(
                                        "MAX media upload returned no attachment token"
                                    )
                                document = {"type": "file", "payload": {"token": token_value}}
                                response = await with_backoff(
                                    lambda: client.send_message(
                                        str(chat_id),
                                        chunk,
                                        target_type=target_type,
                                        attachments=[document],
                                    ),
                                    is_rate_limit=_is_media_send_retryable,
                                    extract_retry_after=_media_retry_after,
                                    max_attempts=5,
                                )
                                sent_count += 1
                                message_id = _response_message_id(response)
                                if message_id:
                                    message_ids.append(message_id)
                                audio_fallback_used = True
                                continue
                            except (MaxApiError, OSError, TypeError, ValueError) as fallback_exc:
                                send_errors.append(str(fallback_exc))
                                continue
                    send_errors.append(str(exc))
            if sent_count and chunks:
                for chunk in chunks[1:]:
                    try:
                        response = await with_backoff(
                            lambda: client.send_message(
                                str(chat_id), chunk, target_type=target_type
                            ),
                            is_rate_limit=_is_media_send_retryable,
                            extract_retry_after=_media_retry_after,
                            max_attempts=5,
                        )
                        sent_count += 1
                        message_id = _response_message_id(response)
                        if message_id:
                            message_ids.append(message_id)
                    except MaxApiError as exc:
                        send_errors.append(str(exc))
            if audio_fallback_used:
                try:
                    response = await with_backoff(
                        lambda: client.send_message(
                            str(chat_id),
                            UNSUPPORTED_AUDIO_FALLBACK_MESSAGE,
                            target_type=target_type,
                        ),
                        is_rate_limit=_is_max_rate_limit,
                        extract_retry_after=_retry_after,
                        max_attempts=5,
                    )
                    sent_count += 1
                    message_id = _response_message_id(response)
                    if message_id:
                        message_ids.append(message_id)
                except MaxApiError as exc:
                    send_errors.append(str(exc))
            errors = upload_errors + send_errors
            if not sent_count:
                return {"error": "; ".join(errors) or "MAX media messages failed"}
            result = {
                "success": not errors,
                "platform": PLATFORM_NAME,
                "chat_id": str(chat_id),
                "message_id": message_ids[-1] if message_ids else None,
                "continuation_message_ids": message_ids[:-1],
            }
            if errors:
                result["error"] = "Часть вложений не доставлена: " + "; ".join(errors)
            return result
        finally:
            await client.close()
    except (MaxApiError, ValueError, OSError) as exc:
        return {"error": f"MAX standalone send failed: {exc}"}


def register(ctx: Any) -> None:
    ctx.register_platform(
        name=PLATFORM_NAME,
        label=PLATFORM_LABEL,
        adapter_factory=MaxAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        env_enablement_fn=env_enablement,
        apply_yaml_config_fn=apply_yaml_config,
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        standalone_sender_fn=standalone_send,
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji=PLATFORM_EMOJI,
        platform_hint=PLATFORM_HINT,
        install_hint="httpx is required; configure MAX_BOT_TOKEN and MAX_CA_BUNDLE when host CA lacks the MAX trust chain",
    )

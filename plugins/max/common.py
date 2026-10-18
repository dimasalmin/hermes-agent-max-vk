"""Small self-contained policy and chunking helpers for the MAX plugin."""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from typing import Optional, Set


def split_message(text: str, max_len: int) -> list[str]:
    if max_len <= 0:
        raise ValueError("max_len must be positive")
    if not text:
        return []
    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_len:
        window = remaining[:max_len]
        cut = max(
            window.rfind("\n\n"),
            window.rfind("\n"),
            window.rfind(". "),
            window.rfind("! "),
            window.rfind("? "),
            window.rfind(" "),
        )
        if cut <= max_len // 2:
            cut = max_len
        elif window[cut:cut + 2] in {". ", "! ", "? "}:
            cut += 2
        else:
            cut += 1
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


ALWAYS_ALLOWED_COMMANDS = frozenset({"help", "whoami", "menu", "maxstatus"})


def _parse_ids(value: Optional[str]) -> Set[str]:
    if not value:
        return set()
    raw = value.strip()
    if raw.startswith("["):
        # Older YAML bridges wrote Python list reprs into env, including "[]".
        try:
            try:
                values = json.loads(raw)
            except ValueError:
                values = ast.literal_eval(raw)
        except (ValueError, SyntaxError, RecursionError) as exc:
            raise ValueError("MAX: некорректный список ID в настройках доступа") from exc
        if not isinstance(values, list) or any(
            isinstance(item, bool) or not isinstance(item, (str, int)) for item in values
        ):
            raise ValueError("MAX: список доступа должен содержать только ID пользователей или чатов")
        return {str(item).strip() for item in values if str(item).strip()}
    return {item.strip() for item in raw.split(",") if item.strip()}


@dataclass
class AccessPolicy:
    allowed_users: Set[str] = field(default_factory=set)
    group_allowed_users: Set[str] = field(default_factory=set)
    group_allowed_chats: Set[str] = field(default_factory=set)
    admin_users: Set[str] = field(default_factory=set)
    user_allowed_commands: Set[str] = field(default_factory=set)
    group_user_allowed_commands: Set[str] = field(default_factory=set)
    guest_mode: bool = False
    allow_all: bool = False

    @classmethod
    def from_env_and_extra(
        cls,
        *,
        allowed_users_env: Optional[str],
        group_allowed_users_env: Optional[str],
        group_allowed_chats_env: Optional[str],
        allow_all_env: Optional[str],
        guest_mode_env: Optional[str],
        admin_users_env: Optional[str] = None,
        extra: Optional[dict] = None,
    ) -> "AccessPolicy":
        extra = extra or {}

        def ids(key: str) -> Set[str]:
            value = extra.get(key)
            if isinstance(value, str):
                return _parse_ids(value)
            if isinstance(value, (list, tuple, set)):
                return {str(item).strip() for item in value if str(item).strip()}
            return set()

        return cls(
            allowed_users=_parse_ids(allowed_users_env) | ids("allow_from"),
            group_allowed_users=_parse_ids(group_allowed_users_env) | ids("group_allow_from"),
            group_allowed_chats=_parse_ids(group_allowed_chats_env) | ids("group_allowed_chats"),
            admin_users=_parse_ids(admin_users_env) | ids("allow_admin_from"),
            user_allowed_commands=ids("user_allowed_commands"),
            group_user_allowed_commands=ids("group_user_allowed_commands"),
            guest_mode=str(guest_mode_env or "").lower() in {"1", "true", "yes"},
            allow_all=str(allow_all_env or "").lower() in {"1", "true", "yes"},
        )

    def can_dm(self, user_id: str) -> bool:
        return self.allow_all or user_id in self.allowed_users

    def can_group(self, user_id: str, chat_id: str, *, mentioned: bool) -> bool:
        if self.allow_all:
            return True
        allowed_user = (
            user_id in self.allowed_users
            or user_id in self.group_allowed_users
            or user_id in self.admin_users
        )
        allowed_chat = chat_id in self.group_allowed_chats
        if allowed_user and allowed_chat:
            return True
        return bool(self.guest_mode and mentioned and allowed_chat)

    def is_admin(self, user_id: str) -> bool:
        return self.allow_all or str(user_id).strip() in self.admin_users

    def can_run_command(self, user_id: str, command: str, *, is_group: bool) -> bool:
        cmd = command.lstrip("/").split("@", 1)[0].split(" ", 1)[0].lower()
        if cmd in ALWAYS_ALLOWED_COMMANDS or user_id in self.admin_users:
            return True
        if not self.admin_users and not self.user_allowed_commands and not self.group_user_allowed_commands:
            return True
        allowed = self.group_user_allowed_commands if is_group else self.user_allowed_commands
        return cmd in allowed

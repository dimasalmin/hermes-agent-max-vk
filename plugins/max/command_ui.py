"""MAX-only command presentation; no process-global language or model changes."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any


COMMAND_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar('max_command_context', default=None)


@contextmanager
def russian_commands():
    try:
        from agent.secret_scope import (
            current_secret_scope, current_secret_scope_home, reset_secret_scope,
            serves_routed_profile, set_secret_scope,
        )
    except ImportError:
        yield
        return
    scope = current_secret_scope()
    if scope is None and serves_routed_profile():
        # Never manufacture a credential scope for an unbound routed profile.
        yield
        return
    token = set_secret_scope({**(scope or {}), 'HERMES_LANGUAGE': 'ru'},
                             profile_home=current_secret_scope_home())
    try:
        yield
    finally:
        reset_secret_scope(token)


def choice_label(value: str, label: str) -> str:
    translations = {'none': 'Без рассуждений', 'minimal': 'Минимальный', 'low': 'Низкий',
        'medium': 'Средний', 'high': 'Высокий', 'xhigh': 'Очень высокий',
        'max': 'Максимальный', 'ultra': 'Предельный'}
    if value in translations and label.lower() == value:
        return translations[value]
    if value in translations and label.lower().startswith(value + ' ('):
        suffix = label[len(value):].replace('sends max on this route', 'в API отправляется max')
        return translations[value] + suffix
    return label

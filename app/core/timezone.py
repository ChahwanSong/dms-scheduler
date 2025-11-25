from __future__ import annotations

import os
from datetime import datetime, tzinfo
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE_NAME = "Asia/Seoul"


def _coerce_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except Exception as exc:  # pragma: no cover - zoneinfo raises different errors per platform
        raise ValueError(f"Unknown timezone '{timezone_name}'") from exc


_configured_timezone_name = os.getenv("DMS_TIMEZONE", DEFAULT_TIMEZONE_NAME)
_configured_timezone = _coerce_timezone(_configured_timezone_name)


def set_default_timezone(timezone_name: str) -> None:
    """Update the default timezone used for generated timestamps."""

    global _configured_timezone_name, _configured_timezone
    _configured_timezone = _coerce_timezone(timezone_name)
    _configured_timezone_name = timezone_name


def get_default_timezone() -> ZoneInfo:
    """Return the currently configured timezone."""

    return _configured_timezone


def now(tz: tzinfo | None = None) -> datetime:
    """Return the current time in the configured timezone."""

    return datetime.now(tz or _configured_timezone)


def now_in_configured_tz() -> datetime:
    return now(_configured_timezone)


def format_log_message(message: str) -> str:
    timestamp = now_in_configured_tz().isoformat()
    return f"{timestamp},{message}"

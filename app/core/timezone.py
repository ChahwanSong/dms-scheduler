from __future__ import annotations

from datetime import datetime, timezone as dt_timezone, tzinfo
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .settings import get_settings


@lru_cache
def _get_zoneinfo() -> tzinfo:
    tz_name = get_settings().timezone
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return dt_timezone.utc


def now_in_configured_tz() -> datetime:
    return datetime.now(_get_zoneinfo())


def format_log_message(message: str) -> str:
    timestamp = now_in_configured_tz().isoformat()
    return f"{timestamp},{message}"

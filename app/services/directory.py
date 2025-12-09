"""Directory validation helpers for task execution."""

import os
from typing import Optional, Tuple, Dict

from .constants import ALLOWED_DIRECTORIES


def match_allowed_directory(path: str) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """
    입력된 path가 유효하고(absolute path), ALLOWED_DIRECTORIES 의 prefix 중
    하나와 매칭된다면 (allowed_prefix, metadata) 를 반환.
    매칭되지 않으면 (None, None) 반환.
    """

    # ----------- 0) invalid path 체크 -----------
    if not path or not isinstance(path, str):
        return None, None

    # 널 문자 포함 → 보안적 이유로 invalid
    if "\x00" in path:
        return None, None

    # 절대경로인지 체크 (반드시 "/" 로 시작)
    if not path.startswith("/"):
        return None, None

    # 최상단 경로가 아닌지 체크
    if path.count("/") == 1:
        return None, None

    # mount path 그 자체인지 체크
    if path in ALLOWED_DIRECTORIES:
        return None, None

    # 정규화
    normalized_path = os.path.normpath(path)

    best_prefix = None
    best_match = None
    longest_prefix_len = -1

    # ----------- 1) prefix 매칭 로직 -----------
    for allowed_prefix, info in ALLOWED_DIRECTORIES.items():
        normalized_prefix = os.path.normpath(allowed_prefix)

        if normalized_path == normalized_prefix or normalized_path.startswith(
            normalized_prefix + os.sep
        ):
            if len(normalized_prefix) > longest_prefix_len:
                longest_prefix_len = len(normalized_prefix)
                best_prefix = allowed_prefix
                best_match = info

    return best_prefix, best_match


def make_volume_name_from_path(path: str) -> str:
    """Convert a path into a DNS-compliant volume name."""
    if path == None:
        return ""

    name = (
        path.strip("/")
        .replace("/", "-")
        .replace("_", "-")
        .replace(".", "-")
        .lower()
    )
    name = "".join(c for c in name if c.isalnum() or c == "-")
    name = name.strip("-")
    if not name:
        name = "vol"
    if not name[0].isalnum():
        name = "v" + name
    if not name[-1].isalnum():
        name = name + "v"
    return name


__all__ = ["match_allowed_directory", "make_volume_name_from_path"]

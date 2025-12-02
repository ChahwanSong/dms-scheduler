"""Directory validation helpers for task execution."""

import os

from .constants import ALLOWED_DIRECTORIES


def match_allowed_directory(path: str):
    """Return the allowed prefix and metadata if ``path`` matches; otherwise ``(None, None)``."""

    normalized_path = os.path.normpath(path)

    best_prefix = None
    best_match = None
    longest_prefix_len = -1

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

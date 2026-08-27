"""Derive Reddit-style identifiers from URLs when the raw API record lacks a native ID field."""
from __future__ import annotations

import re

_POST_ID_RE = re.compile(r"/comments/([a-zA-Z0-9]+)/")
_COMMENT_ID_RE = re.compile(r"/comment/([a-zA-Z0-9]+)/?")

_DELETED_SENTINELS = {"[deleted]", "[ deleted ]"}
_REMOVED_SENTINELS = {"[removed]", "[ removed ]"}


def post_id_from_url(url: str) -> str | None:
    if not url:
        return None
    match = _POST_ID_RE.search(url)
    return f"t3_{match.group(1)}" if match else None


def comment_id_from_url(url: str) -> str | None:
    if not url:
        return None
    match = _COMMENT_ID_RE.search(url)
    return f"t1_{match.group(1)}" if match else None


def is_deleted_or_removed_text(text: str | None) -> tuple[bool, bool]:
    """Returns (is_deleted, is_removed) based on Reddit's literal placeholder text."""
    if text is None:
        return False, False
    normalized = text.strip().lower()
    return normalized in _DELETED_SENTINELS, normalized in _REMOVED_SENTINELS

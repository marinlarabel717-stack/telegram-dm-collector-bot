from __future__ import annotations

import re

_CHANNEL_POST_PATTERNS = [
    re.compile(r"^(https?://t\.me/[A-Za-z0-9_]{5,}/\d+)(?:\?.*)?$", re.IGNORECASE),
    re.compile(r"^(https?://t\.me/c/\d+/\d+)(?:\?.*)?$", re.IGNORECASE),
]

_PUBLIC_POST_RE = re.compile(r"^https?://t\.me/([A-Za-z0-9_]{5,})/(\d+)$", re.IGNORECASE)
_PRIVATE_POST_RE = re.compile(r"^https?://t\.me/c/(\d+)/(\d+)$", re.IGNORECASE)


def normalize_channel_post_link(text: str | None) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for pattern in _CHANNEL_POST_PATTERNS:
        match = pattern.match(raw)
        if match:
            return match.group(1)
    return None


def parse_channel_post_link(text: str | None) -> dict | None:
    normalized = normalize_channel_post_link(text)
    if not normalized:
        return None
    public_match = _PUBLIC_POST_RE.match(normalized)
    if public_match:
        return {
            "kind": "public",
            "link": normalized,
            "username": public_match.group(1),
            "message_id": int(public_match.group(2)),
        }
    private_match = _PRIVATE_POST_RE.match(normalized)
    if private_match:
        return {
            "kind": "private",
            "link": normalized,
            "channel_id": int(private_match.group(1)),
            "message_id": int(private_match.group(2)),
        }
    return None

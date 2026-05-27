from __future__ import annotations

import re

_CHANNEL_POST_PATTERNS = [
    re.compile(r"^(https?://t\.me/[A-Za-z0-9_]{5,}/\d+)(?:\?.*)?$", re.IGNORECASE),
    re.compile(r"^(https?://t\.me/c/\d+/\d+)(?:\?.*)?$", re.IGNORECASE),
]


def normalize_channel_post_link(text: str | None) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for pattern in _CHANNEL_POST_PATTERNS:
        match = pattern.match(raw)
        if match:
            return match.group(1)
    return None

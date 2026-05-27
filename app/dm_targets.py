from __future__ import annotations

import re
from dataclasses import dataclass


USERNAME_RE = re.compile(r"^@?[A-Za-z0-9_]{5,32}$")
PHONE_RE = re.compile(r"^\+?[0-9]{5,20}$")
TME_RE = re.compile(r"^(?:https?://)?t\.me/([A-Za-z0-9_]{5,32})/?$", re.IGNORECASE)


@dataclass(slots=True)
class ParsedTarget:
    raw_input: str
    normalized_input: str
    input_type: str


def parse_target(raw: str) -> ParsedTarget | None:
    value = (raw or "").strip()
    if not value:
        return None

    match = TME_RE.match(value)
    if match:
        username = match.group(1)
        return ParsedTarget(raw_input=value, normalized_input=f"@{username}", input_type="username")

    if USERNAME_RE.match(value):
        username = value[1:] if value.startswith("@") else value
        return ParsedTarget(raw_input=value, normalized_input=f"@{username}", input_type="username")

    if PHONE_RE.match(value):
        phone = value if value.startswith("+") else f"+{value}"
        return ParsedTarget(raw_input=value, normalized_input=phone, input_type="phone")

    return None


def parse_targets_text(text: str) -> tuple[list[ParsedTarget], list[str]]:
    parsed: list[ParsedTarget] = []
    invalid: list[str] = []
    seen: set[str] = set()

    for line in text.splitlines():
        item = parse_target(line)
        if not item:
            if line.strip():
                invalid.append(line.strip())
            continue
        if item.normalized_input in seen:
            continue
        seen.add(item.normalized_input)
        parsed.append(item)

    return parsed, invalid

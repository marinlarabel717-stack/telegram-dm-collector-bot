from __future__ import annotations

from dataclasses import dataclass

DEFAULT_POSTBOT_USERNAME = "postbot"


@dataclass(slots=True)
class ParsedPostbotCode:
    bot_username: str
    query: str
    raw: str


def parse_postbot_code(raw_code: str | None) -> ParsedPostbotCode:
    raw = str(raw_code or "").strip()
    if not raw:
        raise ValueError("PostBot 文案代码不能为空")

    parts = raw.split(maxsplit=1)
    if len(parts) == 2 and parts[0].lstrip("@").replace("_", "").isalnum():
        bot_username = parts[0].lstrip("@").strip() or DEFAULT_POSTBOT_USERNAME
        query = parts[1].strip()
    else:
        bot_username = DEFAULT_POSTBOT_USERNAME
        query = raw

    if not query:
        raise ValueError("PostBot 文案代码不能为空")

    return ParsedPostbotCode(bot_username=bot_username, query=query, raw=raw)


async def fetch_postbot_inline_result(client, raw_code: str):
    parsed = parse_postbot_code(raw_code)
    results = await client.inline_query(parsed.bot_username, parsed.query)
    if not results:
        raise ValueError("PostBot 没有返回可发送内容")
    return parsed, results[0]


def describe_postbot_inline_result(result) -> str:
    result_type = str(getattr(result, "type", "article") or "article")
    title = str(getattr(result, "title", "") or "").strip()
    description = str(getattr(result, "description", "") or "").strip()

    pieces = [f"结果类型：{result_type}"]
    if title:
        pieces.append(f"标题：{title}")
    if description:
        pieces.append(f"描述：{description}")
    pieces.append("实际发送时会按 PostBot 返回的内联内容发出，不会把代码原样发出去。")
    return "\n".join(pieces)

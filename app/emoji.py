from __future__ import annotations

import html
from telegram import InlineKeyboardButton

DEFAULT_EMOJI_IDS = {
    "welcome": "5296308529873828834",  # 🌠
    "inbox": "5954227490179255253",    # 🔵
    "stats": "6237934454019461140",    # 🧠
    "export": "5282843764451195532",   # 🖥
    "success": "5312028599803460968",  # 🆗
}


def tg_emoji(emoji_id: str, alt: str) -> str:
    safe_id = html.escape(str(emoji_id), quote=True)
    safe_alt = html.escape(str(alt), quote=False)
    return f'<tg-emoji emoji-id="{safe_id}">{safe_alt}</tg-emoji>'



def premium_button(text: str, emoji_id: str, **kwargs) -> InlineKeyboardButton:
    api_kwargs = dict(kwargs.pop("api_kwargs", {}) or {})
    api_kwargs["icon_custom_emoji_id"] = str(emoji_id)
    return InlineKeyboardButton(text=text, api_kwargs=api_kwargs, **kwargs)

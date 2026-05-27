from __future__ import annotations

import html
from telegram import InlineKeyboardButton

DEFAULT_EMOJI_IDS = {
    "welcome": "5296308529873828834",   # 🌠
    "inbox": "5954227490179255253",     # 🔵
    "stats": "6237934454019461140",     # 🧠
    "export": "5282843764451195532",    # 🖥
    "success": "5312028599803460968",   # 🆗
    "upload": "5474194650661147876",    # 📷
    "waiting": "5296562641613897196",   # 🕜
    "ok": "5260463209562776385",        # ✅
    "error": "5273914604752216432",     # ❌
    "timeout": "5382194935057372936",   # ⏱️
    "progress": "5352625743081775722",  # 🎚️
    "idea": "5193127592764394874",      # 💡
    "back": "5222097061276566531",      # 🍃
    "home": "5217464097234241939",      # ☀️
    "next": "5220195537520711716",      # ⚡️
    "list": "5132131004097496494",      # 🧩
    "start": "5201730588351945766",     # 🎊
    "history": "5954284484395273123",   # 🌟
    "all": "5199658498559854923",       # 🍀
}


STATUS_META = {
    "active": (DEFAULT_EMOJI_IDS["ok"], "✅", "可用"),
    "checking": (DEFAULT_EMOJI_IDS["waiting"], "🕜", "检测中"),
    "collecting": (DEFAULT_EMOJI_IDS["progress"], "🎚️", "采集中"),
    "unauthorized": (DEFAULT_EMOJI_IDS["error"], "❌", "未登录"),
    "error": (DEFAULT_EMOJI_IDS["error"], "❌", "异常"),
    "stopped": (DEFAULT_EMOJI_IDS["timeout"], "⏱️", "已停止"),
    "completed": (DEFAULT_EMOJI_IDS["success"], "🆗", "已完成"),
    "queued": (DEFAULT_EMOJI_IDS["waiting"], "🕜", "排队中"),
}


def tg_emoji(emoji_id: str, alt: str) -> str:
    safe_id = html.escape(str(emoji_id), quote=True)
    safe_alt = html.escape(str(alt), quote=False)
    return f'<tg-emoji emoji-id="{safe_id}">{safe_alt}</tg-emoji>'



def premium_button(text: str, emoji_id: str, **kwargs) -> InlineKeyboardButton:
    api_kwargs = dict(kwargs.pop("api_kwargs", {}) or {})
    api_kwargs["icon_custom_emoji_id"] = str(emoji_id)
    return InlineKeyboardButton(text=text, api_kwargs=api_kwargs, **kwargs)



def status_badge(status: str) -> str:
    emoji_id, alt, label = STATUS_META.get(status, STATUS_META["error"])
    return f"{tg_emoji(emoji_id, alt)} <b>{label}</b>"

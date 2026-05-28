from __future__ import annotations

import html
from telegram import InlineKeyboardButton

DEFAULT_EMOJI_IDS = {
    "welcome": "5296308529873828834",   # 🌠
    "inbox": "5954227490179255253",     # 🔵
    "stats": "6237934454019461140",     # 🧠
    "export": "5445355530111437729",    # 📤
    "success": "5312028599803460968",   # 🆗
    "upload": "5443127283898405358",    # 📥
    "waiting": "5296562641613897196",   # 🕜
    "ok": "5260463209562776385",        # ✅
    "error": "5273914604752216432",     # ❌
    "timeout": "5382194935057372936",   # ⏱️
    "progress": "5352625743081775722",  # 🎚️
    "refresh": "5242232228737472707",   # ⚡
    "idea": "5193127592764394874",      # 💡
    "back": "6323574371029884186",      # ⬅️
    "home": "5416041192905265756",      # 🏠
    "next": "5416117059207572332",      # ➡️
    "list": "6321041414067068140",      # 👤
    "start": "5201730588351945766",     # 🎊
    "history": "6321175945327680619",   # 📝
    "all": "5199658498559854923",       # 🍀
    "completed": "5318880799217431403", # 🟢
    "new": "5233588456730427459",       # 🆕
    "task_list": "5321178215878780131", # 📋
    "stop": "6271674836628541366",      # 🛑
    "trash": "5445267414562389170",     # 🗑
}


STATUS_META = {
    "active": (DEFAULT_EMOJI_IDS["ok"], "✅", "可用"),
    "checking": (DEFAULT_EMOJI_IDS["waiting"], "🕜", "检测中"),
    "collecting": (DEFAULT_EMOJI_IDS["progress"], "🎚️", "采集中"),
    "running": (DEFAULT_EMOJI_IDS["progress"], "🎚️", "采集中"),
    "unauthorized": (DEFAULT_EMOJI_IDS["error"], "❌", "未登录"),
    "error": (DEFAULT_EMOJI_IDS["error"], "❌", "异常"),
    "stopped": (DEFAULT_EMOJI_IDS["stop"], "🛑", "已停止"),
    "completed": (DEFAULT_EMOJI_IDS["completed"], "🟢", "已完成"),
    "queued": (DEFAULT_EMOJI_IDS["waiting"], "🕜", "排队中"),
}

RESTRICTION_META = {
    "unknown": (DEFAULT_EMOJI_IDS["waiting"], "🕜", "待检测"),
    "checking": (DEFAULT_EMOJI_IDS["stats"], "🧠", "检测中"),
    "unrestricted": (DEFAULT_EMOJI_IDS["completed"], "🟢", "无限制"),
    "temp_mutual": (DEFAULT_EMOJI_IDS["timeout"], "⏱️", "临时双向"),
    "permanent_mutual": (DEFAULT_EMOJI_IDS["error"], "❌", "永久双向"),
    "geo_limited": ("6321283126236552928", "😄", "地区限制"),
    "frozen": (DEFAULT_EMOJI_IDS["progress"], "🎚️", "冻结"),
    "spam_limited": (DEFAULT_EMOJI_IDS["error"], "❌", "官方限流"),
    "restricted": (DEFAULT_EMOJI_IDS["error"], "❌", "受限"),
    "session_invalid": (DEFAULT_EMOJI_IDS["error"], "❌", "已失效"),
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


def restriction_badge(status: str | None) -> str:
    emoji_id, alt, label = RESTRICTION_META.get(status or "unknown", RESTRICTION_META["unknown"])
    return f"{tg_emoji(emoji_id, alt)} <b>{label}</b>"

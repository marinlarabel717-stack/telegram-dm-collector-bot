from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .emoji import DEFAULT_EMOJI_IDS


load_dotenv()


@dataclass(slots=True)
class Settings:
    bot_token: str
    admin_ids: list[int]
    db_path: Path
    forward_to_admins: bool
    save_raw_update: bool
    auto_reply_enabled: bool
    auto_reply_text: str
    welcome_text: str
    emoji_welcome_id: str
    emoji_inbox_id: str
    emoji_stats_id: str
    emoji_export_id: str
    emoji_success_id: str



def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}



def _parse_admin_ids(value: str | None) -> list[int]:
    if not value:
        return []
    result: list[int] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        result.append(int(chunk))
    return result



def get_settings() -> Settings:
    token = (os.getenv("BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("缺少 BOT_TOKEN，请先配置 .env")

    db_path = Path(os.getenv("DB_PATH", "data/dm_bot.sqlite3"))
    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parent.parent
        db_path = project_root / db_path

    return Settings(
        bot_token=token,
        admin_ids=_parse_admin_ids(os.getenv("ADMIN_IDS")),
        db_path=db_path,
        forward_to_admins=_parse_bool(os.getenv("FORWARD_TO_ADMINS"), True),
        save_raw_update=_parse_bool(os.getenv("SAVE_RAW_UPDATE"), True),
        auto_reply_enabled=_parse_bool(os.getenv("AUTO_REPLY_ENABLED"), False),
        auto_reply_text=os.getenv("AUTO_REPLY_TEXT", "消息已收到，我们会尽快查看。"),
        welcome_text=os.getenv("WELCOME_TEXT", "欢迎，直接给我发消息就行，我会自动记录你的资料和私信内容。"),
        emoji_welcome_id=os.getenv("EMOJI_WELCOME_ID", DEFAULT_EMOJI_IDS["welcome"]),
        emoji_inbox_id=os.getenv("EMOJI_INBOX_ID", DEFAULT_EMOJI_IDS["inbox"]),
        emoji_stats_id=os.getenv("EMOJI_STATS_ID", DEFAULT_EMOJI_IDS["stats"]),
        emoji_export_id=os.getenv("EMOJI_EXPORT_ID", DEFAULT_EMOJI_IDS["export"]),
        emoji_success_id=os.getenv("EMOJI_SUCCESS_ID", DEFAULT_EMOJI_IDS["success"]),
    )

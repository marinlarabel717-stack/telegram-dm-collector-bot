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
    data_dir: Path
    session_dir: Path
    export_dir: Path
    api_id: int
    api_hash: str
    forward_to_admins: bool
    save_raw_update: bool
    auto_reply_enabled: bool
    auto_reply_text: str
    welcome_text: str
    max_collect_workers: int
    emoji_welcome_id: str
    emoji_inbox_id: str
    emoji_stats_id: str
    emoji_export_id: str
    emoji_success_id: str
    emoji_upload_id: str
    emoji_waiting_id: str
    emoji_ok_id: str
    emoji_error_id: str
    emoji_timeout_id: str
    emoji_progress_id: str
    emoji_refresh_id: str
    emoji_idea_id: str
    emoji_back_id: str
    emoji_home_id: str
    emoji_next_id: str
    emoji_list_id: str
    emoji_start_id: str
    emoji_history_id: str
    emoji_all_id: str



def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}



def _parse_admin_ids(value: str | None) -> list[int]:
    if not value:
        return []
    return [int(chunk.strip()) for chunk in value.split(",") if chunk.strip()]



def _parse_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    return int(value.strip())



def get_settings() -> Settings:
    token = (os.getenv("BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("缺少 BOT_TOKEN，请先配置 .env")

    api_id = _parse_int(os.getenv("API_ID"), 0)
    api_hash = (os.getenv("API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("缺少 API_ID 或 API_HASH，session 账号登录与采集功能需要 Telethon 凭证")

    project_root = Path(__file__).resolve().parent.parent
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    if not data_dir.is_absolute():
        data_dir = project_root / data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(os.getenv("DB_PATH", data_dir / "dm_bot.sqlite3"))
    if not db_path.is_absolute():
        db_path = project_root / db_path

    session_dir = Path(os.getenv("SESSION_DIR", data_dir / "sessions"))
    if not session_dir.is_absolute():
        session_dir = project_root / session_dir
    session_dir.mkdir(parents=True, exist_ok=True)

    export_dir = Path(os.getenv("EXPORT_DIR", data_dir / "exports"))
    if not export_dir.is_absolute():
        export_dir = project_root / export_dir
    export_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        bot_token=token,
        admin_ids=_parse_admin_ids(os.getenv("ADMIN_IDS")),
        db_path=db_path,
        data_dir=data_dir,
        session_dir=session_dir,
        export_dir=export_dir,
        api_id=api_id,
        api_hash=api_hash,
        forward_to_admins=_parse_bool(os.getenv("FORWARD_TO_ADMINS"), True),
        save_raw_update=_parse_bool(os.getenv("SAVE_RAW_UPDATE"), True),
        auto_reply_enabled=_parse_bool(os.getenv("AUTO_REPLY_ENABLED"), False),
        auto_reply_text=os.getenv("AUTO_REPLY_TEXT", "消息已收到，我们会尽快查看。"),
        welcome_text=os.getenv("WELCOME_TEXT", "欢迎，直接给我发消息就行，我会自动记录你的资料和私信内容。"),
        max_collect_workers=min(50, max(1, _parse_int(os.getenv("MAX_COLLECT_WORKERS"), 50))),
        emoji_welcome_id=DEFAULT_EMOJI_IDS["welcome"],
        emoji_inbox_id=DEFAULT_EMOJI_IDS["inbox"],
        emoji_stats_id=DEFAULT_EMOJI_IDS["stats"],
        emoji_export_id=DEFAULT_EMOJI_IDS["export"],
        emoji_success_id=DEFAULT_EMOJI_IDS["success"],
        emoji_upload_id=DEFAULT_EMOJI_IDS["upload"],
        emoji_waiting_id=DEFAULT_EMOJI_IDS["waiting"],
        emoji_ok_id=DEFAULT_EMOJI_IDS["ok"],
        emoji_error_id=DEFAULT_EMOJI_IDS["error"],
        emoji_timeout_id=DEFAULT_EMOJI_IDS["timeout"],
        emoji_progress_id=DEFAULT_EMOJI_IDS["progress"],
        emoji_refresh_id=DEFAULT_EMOJI_IDS["refresh"],
        emoji_idea_id=DEFAULT_EMOJI_IDS["idea"],
        emoji_back_id=DEFAULT_EMOJI_IDS["back"],
        emoji_home_id=DEFAULT_EMOJI_IDS["home"],
        emoji_next_id=DEFAULT_EMOJI_IDS["next"],
        emoji_list_id=DEFAULT_EMOJI_IDS["list"],
        emoji_start_id=DEFAULT_EMOJI_IDS["start"],
        emoji_history_id=DEFAULT_EMOJI_IDS["history"],
        emoji_all_id=DEFAULT_EMOJI_IDS["all"],
    )

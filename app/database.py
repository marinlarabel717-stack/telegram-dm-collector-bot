from __future__ import annotations

import csv
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ExportPaths:
    users_csv: Path
    messages_csv: Path


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS users (
                    tg_user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT,
                    is_bot INTEGER NOT NULL DEFAULT 0,
                    is_premium INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    start_count INTEGER NOT NULL DEFAULT 0,
                    message_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_message_id INTEGER NOT NULL,
                    tg_user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    text TEXT,
                    caption TEXT,
                    file_id TEXT,
                    file_unique_id TEXT,
                    media_group_id TEXT,
                    raw_json TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tg_user_id) REFERENCES users (tg_user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
                """
            )
            self.conn.commit()

    def upsert_user(self, user: Any, chat_id: int, increment_start: bool = False, increment_message: bool = False) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO users (
                    tg_user_id, chat_id, username, first_name, last_name,
                    language_code, is_bot, is_premium, start_count, message_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_user_id) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    language_code=excluded.language_code,
                    is_bot=excluded.is_bot,
                    is_premium=excluded.is_premium,
                    last_seen_at=CURRENT_TIMESTAMP,
                    start_count=start_count + ?,
                    message_count=message_count + ?
                """,
                (
                    user.id,
                    chat_id,
                    user.username,
                    user.first_name,
                    user.last_name,
                    getattr(user, "language_code", None),
                    int(bool(getattr(user, "is_bot", False))),
                    int(bool(getattr(user, "is_premium", False))),
                    1 if increment_start else 0,
                    1 if increment_message else 0,
                    1 if increment_start else 0,
                    1 if increment_message else 0,
                ),
            )
            self.conn.commit()

    def save_message(self, *, message: Any, tg_user_id: int, chat_id: int, message_type: str, raw_json: dict[str, Any] | None) -> None:
        file_id = None
        file_unique_id = None

        if message.photo:
            largest = message.photo[-1]
            file_id = largest.file_id
            file_unique_id = largest.file_unique_id
        elif message.document:
            file_id = message.document.file_id
            file_unique_id = message.document.file_unique_id
        elif message.video:
            file_id = message.video.file_id
            file_unique_id = message.video.file_unique_id
        elif message.voice:
            file_id = message.voice.file_id
            file_unique_id = message.voice.file_unique_id
        elif message.audio:
            file_id = message.audio.file_id
            file_unique_id = message.audio.file_unique_id
        elif message.sticker:
            file_id = message.sticker.file_id
            file_unique_id = message.sticker.file_unique_id

        with self.lock:
            self.conn.execute(
                """
                INSERT INTO messages (
                    tg_message_id, tg_user_id, chat_id, message_type, text, caption,
                    file_id, file_unique_id, media_group_id, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.message_id,
                    tg_user_id,
                    chat_id,
                    message_type,
                    message.text,
                    message.caption,
                    file_id,
                    file_unique_id,
                    str(message.media_group_id) if message.media_group_id else None,
                    json.dumps(raw_json, ensure_ascii=False) if raw_json else None,
                ),
            )
            self.conn.commit()

    def get_stats(self) -> dict[str, int]:
        with self.lock:
            user_count = self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            message_count = self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            today_message_count = self.conn.execute(
                "SELECT COUNT(*) FROM messages WHERE date(created_at, 'localtime') = date('now', 'localtime')"
            ).fetchone()[0]
        return {
            "users": user_count,
            "messages": message_count,
            "today_messages": today_message_count,
        }

    def export_csv(self, export_dir: Path) -> ExportPaths:
        export_dir.mkdir(parents=True, exist_ok=True)
        users_csv = export_dir / "users.csv"
        messages_csv = export_dir / "messages.csv"

        with self.lock:
            users = self.conn.execute("SELECT * FROM users ORDER BY last_seen_at DESC").fetchall()
            messages = self.conn.execute("SELECT * FROM messages ORDER BY created_at DESC").fetchall()

        with users_csv.open("w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.writer(fp)
            writer.writerow([
                "tg_user_id", "chat_id", "username", "first_name", "last_name",
                "language_code", "is_bot", "is_premium", "first_seen_at", "last_seen_at",
                "start_count", "message_count",
            ])
            for row in users:
                writer.writerow([row[key] for key in row.keys()])

        with messages_csv.open("w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.writer(fp)
            writer.writerow([
                "id", "tg_message_id", "tg_user_id", "chat_id", "message_type", "text",
                "caption", "file_id", "file_unique_id", "media_group_id", "raw_json", "created_at",
            ])
            for row in messages:
                writer.writerow([row[key] for key in row.keys()])

        return ExportPaths(users_csv=users_csv, messages_csv=messages_csv)

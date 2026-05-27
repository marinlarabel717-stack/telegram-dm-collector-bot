from __future__ import annotations

import csv
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


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

                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_name TEXT NOT NULL UNIQUE,
                    session_file TEXT NOT NULL UNIQUE,
                    tg_user_id INTEGER,
                    phone TEXT,
                    username TEXT,
                    display_name TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    last_error TEXT,
                    last_checked_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS collect_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requester_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    days_limit INTEGER NOT NULL,
                    worker_count INTEGER NOT NULL DEFAULT 1,
                    account_count INTEGER NOT NULL DEFAULT 0,
                    channels_json TEXT NOT NULL,
                    account_ids_json TEXT NOT NULL,
                    total_channels INTEGER NOT NULL DEFAULT 0,
                    finished_channels INTEGER NOT NULL DEFAULT 0,
                    total_messages_scanned INTEGER NOT NULL DEFAULT 0,
                    total_hits INTEGER NOT NULL DEFAULT 0,
                    unique_hits INTEGER NOT NULL DEFAULT 0,
                    progress_chat_id INTEGER,
                    progress_message_id INTEGER,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    result_file_path TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS collect_task_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    account_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'queued',
                    scanned_messages INTEGER NOT NULL DEFAULT 0,
                    hits INTEGER NOT NULL DEFAULT 0,
                    unique_hits INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES collect_tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                );

                CREATE TABLE IF NOT EXISTS collect_task_usernames (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    source_channel TEXT,
                    source_message_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES collect_tasks(id) ON DELETE CASCADE,
                    UNIQUE(task_id, username)
                );

                CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
                CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
                CREATE INDEX IF NOT EXISTS idx_collect_tasks_status ON collect_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_task_channels_task_id ON collect_task_channels(task_id);
                CREATE INDEX IF NOT EXISTS idx_task_usernames_task_id ON collect_task_usernames(task_id);
                """
            )
            self.conn.commit()

    # ---------- basic dm storage ----------
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
            account_count = self.conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            active_account_count = self.conn.execute("SELECT COUNT(*) FROM accounts WHERE status='active'").fetchone()[0]
            running_task_count = self.conn.execute("SELECT COUNT(*) FROM collect_tasks WHERE status IN ('queued','running')").fetchone()[0]
        return {
            "users": user_count,
            "messages": message_count,
            "today_messages": today_message_count,
            "accounts": account_count,
            "active_accounts": active_account_count,
            "running_tasks": running_task_count,
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

    # ---------- account storage ----------
    def upsert_account(
        self,
        *,
        session_name: str,
        session_file: str,
        tg_user_id: int | None,
        phone: str | None,
        username: str | None,
        display_name: str | None,
        status: str,
        last_error: str | None,
    ) -> sqlite3.Row:
        with self.lock:
            existing = None
            if tg_user_id is not None:
                existing = self.conn.execute("SELECT * FROM accounts WHERE tg_user_id=?", (tg_user_id,)).fetchone()
            if existing:
                self.conn.execute(
                    """
                    UPDATE accounts SET
                        session_name=?,
                        session_file=?,
                        phone=?,
                        username=?,
                        display_name=?,
                        status=?,
                        last_error=?,
                        last_checked_at=CURRENT_TIMESTAMP,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (session_name, session_file, phone, username, display_name, status, last_error, existing["id"]),
                )
                account_id = existing["id"]
            else:
                self.conn.execute(
                    """
                    INSERT INTO accounts (
                        session_name, session_file, tg_user_id, phone, username, display_name,
                        status, last_error, last_checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (session_name, session_file, tg_user_id, phone, username, display_name, status, last_error),
                )
                account_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            self.conn.commit()
            return self.conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()

    def list_accounts(self, *, limit: int = 20, offset: int = 0) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM accounts ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

    def count_accounts(self) -> int:
        with self.lock:
            return self.conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]

    def get_account(self, account_id: int) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()

    def get_active_accounts(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM accounts WHERE status='active' ORDER BY updated_at DESC, id DESC"
            ).fetchall()

    def update_account_status(
        self,
        account_id: int,
        *,
        status: str,
        last_error: str | None = None,
        tg_user_id: int | None = None,
        phone: str | None = None,
        username: str | None = None,
        display_name: str | None = None,
    ) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE accounts SET
                    status=?,
                    last_error=?,
                    tg_user_id=COALESCE(?, tg_user_id),
                    phone=COALESCE(?, phone),
                    username=COALESCE(?, username),
                    display_name=COALESCE(?, display_name),
                    last_checked_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (status, last_error, tg_user_id, phone, username, display_name, account_id),
            )
            self.conn.commit()

    def delete_account(self, account_id: int) -> sqlite3.Row | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
            if row:
                self.conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))
                self.conn.commit()
            return row

    # ---------- collection task storage ----------
    def create_collect_task(
        self,
        *,
        requester_id: int,
        channels: list[str],
        days_limit: int,
        account_ids: list[int],
        worker_count: int,
    ) -> sqlite3.Row:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO collect_tasks (
                    requester_id, days_limit, worker_count, account_count,
                    channels_json, account_ids_json, total_channels
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_id,
                    days_limit,
                    worker_count,
                    len(account_ids),
                    json.dumps(channels, ensure_ascii=False),
                    json.dumps(account_ids, ensure_ascii=False),
                    len(channels),
                ),
            )
            task_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            self.conn.executemany(
                "INSERT INTO collect_task_channels (task_id, channel) VALUES (?, ?)",
                [(task_id, channel) for channel in channels],
            )
            self.conn.commit()
            return self.conn.execute("SELECT * FROM collect_tasks WHERE id=?", (task_id,)).fetchone()

    def get_collect_task(self, task_id: int) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute("SELECT * FROM collect_tasks WHERE id=?", (task_id,)).fetchone()

    def list_collect_tasks(self, *, limit: int = 10, history: bool = False) -> list[sqlite3.Row]:
        with self.lock:
            if history:
                query = "SELECT * FROM collect_tasks ORDER BY id DESC LIMIT ?"
                return self.conn.execute(query, (limit,)).fetchall()
            query = "SELECT * FROM collect_tasks WHERE status IN ('queued','running','stopped','error','completed') ORDER BY id DESC LIMIT ?"
            return self.conn.execute(query, (limit,)).fetchall()

    def list_collect_task_channels(self, task_id: int) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM collect_task_channels WHERE task_id=? ORDER BY id ASC",
                (task_id,),
            ).fetchall()

    def set_collect_task_progress_message(self, task_id: int, chat_id: int, message_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE collect_tasks SET progress_chat_id=?, progress_message_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (chat_id, message_id, task_id),
            )
            self.conn.commit()

    def mark_collect_task_status(self, task_id: int, status: str, *, last_error: str | None = None) -> None:
        with self.lock:
            started_expr = "COALESCE(started_at, CURRENT_TIMESTAMP)" if status == "running" else "started_at"
            finished_expr = "CURRENT_TIMESTAMP" if status in {"completed", "error", "stopped"} else "finished_at"
            self.conn.execute(
                f"""
                UPDATE collect_tasks SET
                    status=?,
                    last_error=?,
                    started_at={started_expr},
                    finished_at={finished_expr},
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (status, last_error, task_id),
            )
            self.conn.commit()

    def request_task_stop(self, task_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE collect_tasks SET stop_requested=1, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (task_id,),
            )
            self.conn.commit()

    def should_stop_task(self, task_id: int) -> bool:
        with self.lock:
            row = self.conn.execute("SELECT stop_requested FROM collect_tasks WHERE id=?", (task_id,)).fetchone()
        return bool(row and row[0])

    def start_task_channel(self, task_channel_id: int, account_id: int) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE collect_task_channels SET
                    account_id=?,
                    status='running',
                    started_at=COALESCE(started_at, CURRENT_TIMESTAMP),
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (account_id, task_channel_id),
            )
            self.conn.commit()

    def finish_task_channel(
        self,
        task_channel_id: int,
        *,
        status: str,
        scanned_messages: int,
        hits: int,
        unique_hits: int,
        last_error: str | None,
    ) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE collect_task_channels SET
                    status=?,
                    scanned_messages=?,
                    hits=?,
                    unique_hits=?,
                    last_error=?,
                    finished_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (status, scanned_messages, hits, unique_hits, last_error, task_channel_id),
            )
            self.conn.commit()

    def increment_task_metrics(
        self,
        task_id: int,
        *,
        scanned_delta: int = 0,
        hits_delta: int = 0,
        finished_delta: int = 0,
        unique_total: int | None = None,
    ) -> None:
        with self.lock:
            if unique_total is None:
                self.conn.execute(
                    """
                    UPDATE collect_tasks SET
                        total_messages_scanned=total_messages_scanned + ?,
                        total_hits=total_hits + ?,
                        finished_channels=finished_channels + ?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (scanned_delta, hits_delta, finished_delta, task_id),
                )
            else:
                self.conn.execute(
                    """
                    UPDATE collect_tasks SET
                        total_messages_scanned=total_messages_scanned + ?,
                        total_hits=total_hits + ?,
                        finished_channels=finished_channels + ?,
                        unique_hits=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (scanned_delta, hits_delta, finished_delta, unique_total, task_id),
                )
            self.conn.commit()

    def add_collected_usernames(
        self,
        task_id: int,
        *,
        usernames: Iterable[str],
        source_channel: str,
        source_message_id: int | None,
    ) -> int:
        cleaned = [(task_id, username, source_channel, source_message_id) for username in usernames]
        if not cleaned:
            return 0
        with self.lock:
            before = self.conn.total_changes
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO collect_task_usernames (
                    task_id, username, source_channel, source_message_id
                ) VALUES (?, ?, ?, ?)
                """,
                cleaned,
            )
            self.conn.commit()
            return self.conn.total_changes - before

    def count_unique_usernames(self, task_id: int) -> int:
        with self.lock:
            return self.conn.execute(
                "SELECT COUNT(*) FROM collect_task_usernames WHERE task_id=?",
                (task_id,),
            ).fetchone()[0]

    def set_task_result_file(self, task_id: int, result_file_path: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE collect_tasks SET result_file_path=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (result_file_path, task_id),
            )
            self.conn.commit()

    def export_task_usernames_txt(self, task_id: int, export_dir: Path) -> Path:
        export_dir.mkdir(parents=True, exist_ok=True)
        output = export_dir / f"task_{task_id}_usernames.txt"
        with self.lock:
            rows = self.conn.execute(
                "SELECT username FROM collect_task_usernames WHERE task_id=? ORDER BY username ASC",
                (task_id,),
            ).fetchall()
        output.write_text("\n".join(row["username"] for row in rows), encoding="utf-8")
        self.set_task_result_file(task_id, str(output))
        return output

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
                    task_type TEXT NOT NULL DEFAULT 'channel',
                    status TEXT NOT NULL DEFAULT 'queued',
                    days_limit INTEGER NOT NULL,
                    worker_count INTEGER NOT NULL DEFAULT 1,
                    account_count INTEGER NOT NULL DEFAULT 0,
                    filters_json TEXT NOT NULL DEFAULT '{}',
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

                CREATE TABLE IF NOT EXISTS collect_task_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    display_name TEXT,
                    source_channel TEXT,
                    source_message_id INTEGER,
                    message_count INTEGER NOT NULL DEFAULT 1,
                    is_bot INTEGER NOT NULL DEFAULT 0,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    has_photo INTEGER NOT NULL DEFAULT 0,
                    last_spoke_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES collect_tasks(id) ON DELETE CASCADE,
                    UNIQUE(task_id, user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
                CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
                CREATE INDEX IF NOT EXISTS idx_collect_tasks_status ON collect_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_task_channels_task_id ON collect_task_channels(task_id);
                CREATE INDEX IF NOT EXISTS idx_task_usernames_task_id ON collect_task_usernames(task_id);
                CREATE INDEX IF NOT EXISTS idx_task_members_task_id ON collect_task_members(task_id);
                """
            )
            for statement in (
                "ALTER TABLE collect_tasks ADD COLUMN task_type TEXT NOT NULL DEFAULT 'channel'",
                "ALTER TABLE collect_tasks ADD COLUMN filters_json TEXT NOT NULL DEFAULT '{}'",
            ):
                try:
                    self.conn.execute(statement)
                except sqlite3.OperationalError:
                    pass
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
                "SELECT * FROM accounts WHERE status IN ('active','checking','collecting') ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

    def list_all_accounts(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM accounts ORDER BY id DESC"
            ).fetchall()

    def list_invalid_accounts(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM accounts WHERE status NOT IN ('active','checking','collecting') ORDER BY id DESC"
            ).fetchall()

    def count_accounts(self) -> int:
        with self.lock:
            return self.conn.execute("SELECT COUNT(*) FROM accounts WHERE status IN ('active','checking','collecting')").fetchone()[0]

    def get_account_status_counts(self) -> dict[str, int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS total FROM accounts WHERE status IN ('active','checking','collecting') GROUP BY status"
            ).fetchall()
            invalid_total = self.conn.execute(
                "SELECT COUNT(*) FROM accounts WHERE status NOT IN ('active','checking','collecting')"
            ).fetchone()[0]
        result = {"active": 0, "checking": 0, "collecting": 0, "invalid": int(invalid_total)}
        for row in rows:
            result[str(row["status"])] = int(row["total"])
        return result

    def get_account(self, account_id: int) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()

    def get_active_accounts(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM accounts WHERE status='active' ORDER BY id DESC"
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
        task_type: str = "channel",
        filters_json: str | None = None,
    ) -> sqlite3.Row:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO collect_tasks (
                    requester_id, task_type, days_limit, worker_count, account_count,
                    filters_json, channels_json, account_ids_json, total_channels
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_id,
                    task_type,
                    days_limit,
                    worker_count,
                    len(account_ids),
                    filters_json or "{}",
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

    def list_collect_tasks(self, *, limit: int = 10, offset: int = 0, history: bool = False) -> list[sqlite3.Row]:
        with self.lock:
            if history:
                query = "SELECT * FROM collect_tasks ORDER BY id DESC LIMIT ? OFFSET ?"
                return self.conn.execute(query, (limit, offset)).fetchall()
            query = "SELECT * FROM collect_tasks WHERE status IN ('queued','running','stopped','error','completed') ORDER BY id DESC LIMIT ? OFFSET ?"
            return self.conn.execute(query, (limit, offset)).fetchall()

    def count_collect_tasks(self, *, history: bool = False) -> int:
        with self.lock:
            if history:
                return self.conn.execute("SELECT COUNT(*) FROM collect_tasks WHERE status IN ('completed','stopped','error')").fetchone()[0]
            return self.conn.execute(
                "SELECT COUNT(*) FROM collect_tasks WHERE status IN ('queued','running','stopped','error','completed')"
            ).fetchone()[0]

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

    def stop_collect_task_now(self, task_id: int, *, reason: str) -> sqlite3.Row | None:
        with self.lock:
            task = self.conn.execute("SELECT * FROM collect_tasks WHERE id=?", (task_id,)).fetchone()
            if not task:
                return None

            self.conn.execute(
                """
                UPDATE collect_tasks SET
                    status='stopped',
                    stop_requested=1,
                    last_error=?,
                    finished_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (reason, task_id),
            )
            self.conn.execute(
                """
                UPDATE collect_task_channels SET
                    status='stopped',
                    last_error=COALESCE(last_error, ?),
                    finished_at=COALESCE(finished_at, CURRENT_TIMESTAMP),
                    updated_at=CURRENT_TIMESTAMP
                WHERE task_id=? AND status IN ('queued', 'running')
                """,
                (reason, task_id),
            )

            finished_channels = self.conn.execute(
                "SELECT COUNT(*) FROM collect_task_channels WHERE task_id=? AND status IN ('completed', 'error', 'stopped')",
                (task_id,),
            ).fetchone()[0]
            unique_hits = self.conn.execute(
                "SELECT COUNT(*) FROM collect_task_usernames WHERE task_id=?",
                (task_id,),
            ).fetchone()[0]
            self.conn.execute(
                "UPDATE collect_tasks SET finished_channels=?, unique_hits=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (finished_channels, unique_hits, task_id),
            )

            try:
                account_ids = [int(value) for value in json.loads(task["account_ids_json"] or "[]")]
            except Exception:  # noqa: BLE001
                account_ids = []
            if account_ids:
                placeholders = ",".join("?" for _ in account_ids)
                self.conn.execute(
                    f"UPDATE accounts SET status='active', last_error=NULL, updated_at=CURRENT_TIMESTAMP WHERE status='collecting' AND id IN ({placeholders})",
                    account_ids,
                )
            self.conn.commit()
            return self.conn.execute("SELECT * FROM collect_tasks WHERE id=?", (task_id,)).fetchone()

    def recover_interrupted_tasks(self, *, reason: str) -> int:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id FROM collect_tasks WHERE status IN ('queued', 'running') ORDER BY id ASC"
            ).fetchall()
        for row in rows:
            self.stop_collect_task_now(row["id"], reason=reason)
        with self.lock:
            self.conn.execute(
                "UPDATE accounts SET status='active', last_error=NULL, updated_at=CURRENT_TIMESTAMP WHERE status='collecting'"
            )
            self.conn.commit()
        return len(rows)

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

    def update_task_channel_progress(
        self,
        task_channel_id: int,
        *,
        scanned_messages: int,
        hits: int,
        unique_hits: int,
        last_error: str | None = None,
    ) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE collect_task_channels SET
                    scanned_messages=?,
                    hits=?,
                    unique_hits=?,
                    last_error=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (scanned_messages, hits, unique_hits, last_error, task_channel_id),
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

    def sync_task_metrics(self, task_id: int, *, unique_total: int | None = None) -> None:
        with self.lock:
            stats = self.conn.execute(
                """
                SELECT
                    COALESCE(SUM(scanned_messages), 0) AS total_scanned,
                    COALESCE(SUM(hits), 0) AS total_hits,
                    COALESCE(SUM(CASE WHEN status IN ('completed', 'error', 'stopped') THEN 1 ELSE 0 END), 0) AS finished_channels
                FROM collect_task_channels
                WHERE task_id=?
                """,
                (task_id,),
            ).fetchone()
            if unique_total is None:
                self.conn.execute(
                    """
                    UPDATE collect_tasks SET
                        total_messages_scanned=?,
                        total_hits=?,
                        finished_channels=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (stats["total_scanned"], stats["total_hits"], stats["finished_channels"], task_id),
                )
            else:
                self.conn.execute(
                    """
                    UPDATE collect_tasks SET
                        total_messages_scanned=?,
                        total_hits=?,
                        finished_channels=?,
                        unique_hits=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (stats["total_scanned"], stats["total_hits"], stats["finished_channels"], unique_total, task_id),
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
            task = self.conn.execute("SELECT task_type FROM collect_tasks WHERE id=?", (task_id,)).fetchone()
            if task and task["task_type"] == "group":
                return self.conn.execute(
                    "SELECT COUNT(*) FROM collect_task_members WHERE task_id=?",
                    (task_id,),
                ).fetchone()[0]
            return self.conn.execute(
                "SELECT COUNT(*) FROM collect_task_usernames WHERE task_id=?",
                (task_id,),
            ).fetchone()[0]

    def add_collected_member(
        self,
        task_id: int,
        *,
        user_id: int,
        username: str | None,
        display_name: str | None,
        source_channel: str,
        source_message_id: int | None,
        is_bot: bool,
        is_admin: bool,
        has_photo: bool,
        spoke_at: str | None,
    ) -> int:
        with self.lock:
            existing = self.conn.execute(
                "SELECT id FROM collect_task_members WHERE task_id=? AND user_id=?",
                (task_id, user_id),
            ).fetchone()
            if existing:
                self.conn.execute(
                    """
                    UPDATE collect_task_members SET
                        username=COALESCE(?, username),
                        display_name=COALESCE(?, display_name),
                        source_channel=COALESCE(?, source_channel),
                        source_message_id=COALESCE(?, source_message_id),
                        message_count=message_count + 1,
                        is_bot=?,
                        is_admin=?,
                        has_photo=?,
                        last_spoke_at=COALESCE(?, last_spoke_at),
                        updated_at=CURRENT_TIMESTAMP
                    WHERE task_id=? AND user_id=?
                    """,
                    (
                        username,
                        display_name,
                        source_channel,
                        source_message_id,
                        int(bool(is_bot)),
                        int(bool(is_admin)),
                        int(bool(has_photo)),
                        spoke_at,
                        task_id,
                        user_id,
                    ),
                )
                self.conn.commit()
                return 0
            self.conn.execute(
                """
                INSERT INTO collect_task_members (
                    task_id, user_id, username, display_name, source_channel, source_message_id,
                    message_count, is_bot, is_admin, has_photo, last_spoke_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    user_id,
                    username,
                    display_name,
                    source_channel,
                    source_message_id,
                    int(bool(is_bot)),
                    int(bool(is_admin)),
                    int(bool(has_photo)),
                    spoke_at,
                ),
            )
            self.conn.commit()
            return 1

    def set_task_result_file(self, task_id: int, result_file_path: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE collect_tasks SET result_file_path=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (result_file_path, task_id),
            )
            self.conn.commit()

    def delete_collect_task(self, task_id: int) -> sqlite3.Row | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM collect_tasks WHERE id=?", (task_id,)).fetchone()
            if not row:
                return None
            self.conn.execute("DELETE FROM collect_task_usernames WHERE task_id=?", (task_id,))
            self.conn.execute("DELETE FROM collect_task_members WHERE task_id=?", (task_id,))
            self.conn.execute("DELETE FROM collect_task_channels WHERE task_id=?", (task_id,))
            self.conn.execute("DELETE FROM collect_tasks WHERE id=?", (task_id,))
            self.conn.commit()
            return row

    def list_history_tasks(self, *, limit: int = 10, offset: int = 0) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM collect_tasks WHERE status IN ('completed','stopped','error') ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

    def delete_history_tasks(self) -> list[sqlite3.Row]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM collect_tasks WHERE status IN ('completed','stopped','error') ORDER BY id DESC"
            ).fetchall()
            if not rows:
                return []
            task_ids = [int(row['id']) for row in rows]
            placeholders = ','.join('?' for _ in task_ids)
            self.conn.execute(f"DELETE FROM collect_task_usernames WHERE task_id IN ({placeholders})", task_ids)
            self.conn.execute(f"DELETE FROM collect_task_members WHERE task_id IN ({placeholders})", task_ids)
            self.conn.execute(f"DELETE FROM collect_task_channels WHERE task_id IN ({placeholders})", task_ids)
            self.conn.execute(f"DELETE FROM collect_tasks WHERE id IN ({placeholders})", task_ids)
            self.conn.commit()
            return rows

    def export_task_usernames_txt(self, task_id: int, export_dir: Path) -> Path:
        export_dir.mkdir(parents=True, exist_ok=True)
        output = export_dir / f"task_{task_id}_usernames.txt"
        with self.lock:
            task = self.conn.execute("SELECT task_type FROM collect_tasks WHERE id=?", (task_id,)).fetchone()
            task_type = task["task_type"] if task else "channel"
            if task_type == "group":
                usernames = self.conn.execute(
                    "SELECT username FROM collect_task_members WHERE task_id=? AND username IS NOT NULL AND username != '' ORDER BY username ASC",
                    (task_id,),
                ).fetchall()
            else:
                usernames = self.conn.execute(
                    "SELECT username FROM collect_task_usernames WHERE task_id=? ORDER BY username ASC",
                    (task_id,),
                ).fetchall()
            failed_channels = self.conn.execute(
                """
                SELECT channel, status, last_error
                FROM collect_task_channels
                WHERE task_id=? AND status IN ('error', 'stopped')
                ORDER BY id ASC
                """,
                (task_id,),
            ).fetchall()

        lines = ["# 去重用户名结果", ""]
        lines.extend(row["username"] for row in usernames)

        if failed_channels:
            lines.extend(["", "# 失败/跳过频道", ""])
            for row in failed_channels:
                reason = row["last_error"] or ("任务已停止" if row["status"] == "stopped" else "未知原因")
                lines.append(f"{row['channel']} | {reason}")

        output.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        self.set_task_result_file(task_id, str(output))
        return output

    def export_group_task_files(self, task_id: int, export_dir: Path) -> dict[str, Path]:
        export_dir.mkdir(parents=True, exist_ok=True)
        usernames_path = export_dir / f"task_{task_id}_usernames.txt"
        ids_path = export_dir / f"task_{task_id}_ids.txt"
        failed_path = export_dir / f"task_{task_id}_failed_groups.txt"
        with self.lock:
            members = self.conn.execute(
                """
                SELECT user_id, username, display_name, source_channel, message_count, last_spoke_at
                FROM collect_task_members
                WHERE task_id=?
                ORDER BY COALESCE(username, ''), user_id ASC
                """,
                (task_id,),
            ).fetchall()
            failed_channels = self.conn.execute(
                """
                SELECT channel, status, last_error
                FROM collect_task_channels
                WHERE task_id=? AND status IN ('error', 'stopped')
                ORDER BY id ASC
                """,
                (task_id,),
            ).fetchall()

        usernames_lines = ["# 群组发言用户名结果", ""]
        usernames_lines.extend(row["username"] for row in members if row["username"])
        if len(usernames_lines) == 2:
            usernames_lines.append("# 空")

        ids_lines = ["# 群组发言用户 ID 结果（无用户名）", ""]
        ids_lines.extend(str(row["user_id"]) for row in members if not row["username"])
        if len(ids_lines) == 2:
            ids_lines.append("# 空")

        failed_lines = ["# 无法采集的群组", ""]
        if failed_channels:
            for row in failed_channels:
                reason = row["last_error"] or ("任务已停止" if row["status"] == "stopped" else "未知原因")
                failed_lines.append(f"{row['channel']} | {reason}")
        else:
            failed_lines.append("# 无失败群组")

        usernames_path.write_text("\n".join(usernames_lines).strip() + "\n", encoding="utf-8")
        ids_path.write_text("\n".join(ids_lines).strip() + "\n", encoding="utf-8")
        failed_path.write_text("\n".join(failed_lines).strip() + "\n", encoding="utf-8")
        self.set_task_result_file(task_id, str(usernames_path))
        return {"usernames": usernames_path, "ids": ids_path, "failed": failed_path}

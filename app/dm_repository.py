from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence

if TYPE_CHECKING:
    from .database import Database
    from .dm_targets import ParsedTarget


def ensure_dm_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dm_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requester_id INTEGER NOT NULL,
            name TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            message_mode TEXT NOT NULL DEFAULT 'single',
            content_type TEXT NOT NULL DEFAULT 'text',
            total_targets INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            active_accounts INTEGER NOT NULL DEFAULT 0,
            worker_count INTEGER NOT NULL DEFAULT 1,
            policy_json TEXT NOT NULL DEFAULT '{}',
            payload_json TEXT NOT NULL DEFAULT '{}',
            progress_chat_id INTEGER,
            progress_message_id INTEGER,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dm_task_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            sent_success_count INTEGER NOT NULL DEFAULT 0,
            sent_fail_count INTEGER NOT NULL DEFAULT 0,
            frequent_error_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            cooldown_until TEXT,
            last_sent_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES dm_tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            UNIQUE(task_id, account_id)
        );

        CREATE TABLE IF NOT EXISTS dm_recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_input TEXT NOT NULL,
            normalized_input TEXT NOT NULL UNIQUE,
            input_type TEXT NOT NULL,
            resolved_peer_id INTEGER,
            username TEXT,
            phone TEXT,
            resolve_status TEXT NOT NULL DEFAULT 'pending',
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dm_task_recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            recipient_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            assigned_account_id INTEGER,
            retry_count INTEGER NOT NULL DEFAULT 0,
            error_code TEXT,
            error_message TEXT,
            sent_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES dm_tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (recipient_id) REFERENCES dm_recipients(id) ON DELETE CASCADE,
            FOREIGN KEY (assigned_account_id) REFERENCES accounts(id),
            UNIQUE(task_id, recipient_id)
        );

        CREATE TABLE IF NOT EXISTS dm_send_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            account_id INTEGER,
            recipient_id INTEGER,
            action TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            raw_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES dm_tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            FOREIGN KEY (recipient_id) REFERENCES dm_recipients(id)
        );

        CREATE INDEX IF NOT EXISTS idx_dm_tasks_status ON dm_tasks(status);
        CREATE INDEX IF NOT EXISTS idx_dm_task_accounts_task_id ON dm_task_accounts(task_id);
        CREATE INDEX IF NOT EXISTS idx_dm_task_recipients_task_id ON dm_task_recipients(task_id);
        CREATE INDEX IF NOT EXISTS idx_dm_task_recipients_status ON dm_task_recipients(status);
        CREATE INDEX IF NOT EXISTS idx_dm_send_logs_task_id ON dm_send_logs(task_id);
        """
    )
    for statement in (
        "ALTER TABLE accounts ADD COLUMN restriction_status TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE accounts ADD COLUMN restriction_reason TEXT",
        "ALTER TABLE accounts ADD COLUMN restriction_raw_reply TEXT",
        "ALTER TABLE accounts ADD COLUMN restriction_checked_at TEXT",
    ):
        try:
            conn.execute(statement)
        except sqlite3.OperationalError:
            pass


@dataclass(slots=True)
class DMExportPaths:
    success_txt: Path
    failed_txt: Path
    failed_csv: Path


class DmRepository:
    def __init__(self, database: Database):
        self.db = database

    def update_account_restriction(
        self,
        account_id: int,
        *,
        restriction_status: str,
        restriction_reason: str | None = None,
        raw_reply: str | None = None,
    ) -> None:
        with self.db.lock:
            self.db.conn.execute(
                """
                UPDATE accounts SET
                    restriction_status=?,
                    restriction_reason=?,
                    restriction_raw_reply=?,
                    restriction_checked_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (restriction_status, restriction_reason, raw_reply, account_id),
            )
            self.db.conn.commit()

    def get_account_restriction_summary_counts(self) -> dict[str, int]:
        with self.db.lock:
            rows = self.db.conn.execute(
                "SELECT restriction_status, COUNT(*) AS total FROM accounts WHERE status IN ('active','checking','collecting') GROUP BY restriction_status"
            ).fetchall()
        summary = {"unrestricted": 0, "limited": 0, "frozen": 0, "unknown": 0, "invalid": 0}
        limited_statuses = {"temp_mutual", "permanent_mutual", "geo_limited", "spam_limited", "restricted"}
        for row in rows:
            status = str(row["restriction_status"] or "unknown")
            total = int(row["total"])
            if status == "unrestricted":
                summary["unrestricted"] += total
            elif status == "frozen":
                summary["frozen"] += total
            elif status == "session_invalid":
                summary["invalid"] += total
            elif status in limited_statuses:
                summary["limited"] += total
            else:
                summary["unknown"] += total
        return summary

    def create_dm_task(
        self,
        *,
        requester_id: int,
        account_ids: Sequence[int],
        worker_count: int,
        message_mode: str,
        content_type: str,
        payload: dict,
        policy: dict,
        name: str | None = None,
    ) -> sqlite3.Row:
        with self.db.lock:
            self.db.conn.execute(
                """
                INSERT INTO dm_tasks (requester_id, name, worker_count, message_mode, content_type, payload_json, policy_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_id,
                    name,
                    worker_count,
                    message_mode,
                    content_type,
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps(policy, ensure_ascii=False),
                ),
            )
            task_id = int(self.db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            self.db.conn.executemany(
                "INSERT INTO dm_task_accounts (task_id, account_id) VALUES (?, ?)",
                [(task_id, account_id) for account_id in account_ids],
            )
            self.db.conn.commit()
            return self.db.conn.execute("SELECT * FROM dm_tasks WHERE id=?", (task_id,)).fetchone()

    def get_dm_task(self, task_id: int) -> sqlite3.Row | None:
        with self.db.lock:
            return self.db.conn.execute("SELECT * FROM dm_tasks WHERE id=?", (task_id,)).fetchone()

    def create_or_get_recipients(self, targets: Iterable[ParsedTarget]) -> list[int]:
        recipient_ids: list[int] = []
        with self.db.lock:
            for item in targets:
                username = item.normalized_input if item.input_type == "username" else None
                phone = item.normalized_input if item.input_type == "phone" else None
                self.db.conn.execute(
                    """
                    INSERT INTO dm_recipients (raw_input, normalized_input, input_type, username, phone)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(normalized_input) DO UPDATE SET
                        raw_input=excluded.raw_input,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (item.raw_input, item.normalized_input, item.input_type, username, phone),
                )
                row = self.db.conn.execute(
                    "SELECT id FROM dm_recipients WHERE normalized_input=?",
                    (item.normalized_input,),
                ).fetchone()
                recipient_ids.append(int(row["id"]))
            self.db.conn.commit()
        return recipient_ids

    def attach_task_recipients(self, task_id: int, recipient_ids: Sequence[int]) -> None:
        with self.db.lock:
            self.db.conn.executemany(
                "INSERT OR IGNORE INTO dm_task_recipients (task_id, recipient_id) VALUES (?, ?)",
                [(task_id, recipient_id) for recipient_id in recipient_ids],
            )
            self.db.conn.execute(
                "UPDATE dm_tasks SET total_targets=(SELECT COUNT(*) FROM dm_task_recipients WHERE task_id=?), updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (task_id, task_id),
            )
            self.db.conn.commit()

    def add_send_log(
        self,
        *,
        task_id: int,
        action: str,
        status: str,
        account_id: int | None = None,
        recipient_id: int | None = None,
        message: str | None = None,
        raw_error: str | None = None,
    ) -> None:
        with self.db.lock:
            self.db.conn.execute(
                """
                INSERT INTO dm_send_logs (task_id, account_id, recipient_id, action, status, message, raw_error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, account_id, recipient_id, action, status, message, raw_error),
            )
            self.db.conn.commit()

    def export_task_results(self, task_id: int, export_dir: Path) -> DMExportPaths:
        export_dir.mkdir(parents=True, exist_ok=True)
        success_txt = export_dir / f"dm_task_{task_id}_success.txt"
        failed_txt = export_dir / f"dm_task_{task_id}_failed.txt"
        failed_csv = export_dir / f"dm_task_{task_id}_failed.csv"

        with self.db.lock:
            success_rows = self.db.conn.execute(
                """
                SELECT r.normalized_input
                FROM dm_task_recipients tr
                JOIN dm_recipients r ON r.id = tr.recipient_id
                WHERE tr.task_id=? AND tr.status='success'
                ORDER BY tr.id ASC
                """,
                (task_id,),
            ).fetchall()
            failed_rows = self.db.conn.execute(
                """
                SELECT r.normalized_input, tr.error_code, tr.error_message, tr.retry_count
                FROM dm_task_recipients tr
                JOIN dm_recipients r ON r.id = tr.recipient_id
                WHERE tr.task_id=? AND tr.status='failed'
                ORDER BY tr.id ASC
                """,
                (task_id,),
            ).fetchall()

        success_txt.write_text("\n".join(str(row["normalized_input"]) for row in success_rows), encoding="utf-8-sig")
        failed_txt.write_text("\n".join(str(row["normalized_input"]) for row in failed_rows), encoding="utf-8-sig")
        with failed_csv.open("w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.writer(fp)
            writer.writerow(["target", "error_code", "error_message", "retry_count"])
            for row in failed_rows:
                writer.writerow([row["normalized_input"], row["error_code"], row["error_message"], row["retry_count"]])
        return DMExportPaths(success_txt=success_txt, failed_txt=failed_txt, failed_csv=failed_csv)

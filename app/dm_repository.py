from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence

if TYPE_CHECKING:
    from .database import Database
    from .dm_targets import ParsedTarget


BEIJING_TZ = timezone(timedelta(hours=8))


DM_TASK_STATUS_LABELS = {
    "queued": "排队中",
    "running": "私信中",
    "paused": "已暂停",
    "stopped": "已停止",
    "error": "异常",
    "completed": "已完成",
}

DM_LOG_ACTION_LABELS = {
    "account_check": "账号检查",
    "send": "发送",
    "pin": "置顶",
}

DM_LOG_STATUS_LABELS = {
    "success": "成功",
    "failed": "失败",
    "retry": "重试中",
    "pending": "待发送",
    "sending": "发送中",
    "queued": "排队中",
    "running": "私信中",
    "stopped": "已停止",
    "error": "异常",
    "completed": "已完成",
}

DM_REPORT_SECTION_LABELS = {
    "overview": "概览",
    "failure_reason": "失败原因",
    "account_stats": "账号统计",
    "failed_target": "失败目标",
    "log": "日志",
}

DM_ERROR_LABELS = {
    "peer_flood": "官方判定发送过于频繁",
    "privacy_restricted": "对方隐私限制，无法私信",
    "user_not_found": "用户不存在或无法解析",
    "bot_target": "目标不是可私信的普通用户",
    "blocked": "对方已拉黑或关系异常",
    "too_many_requests": "请求过于频繁",
    "mutual_limit": "账号存在双向或发送限制",
    "frozen": "账号疑似冻结",
    "flood_wait": "官方限速等待中",
    "chat_write_forbidden": "这个会话当前不允许发送消息",
    "media_forbidden": "当前聊天不允许发送媒体",
    "text_forbidden": "当前聊天不允许发送文本",
    "forward_forbidden": "这个目标不允许转发该帖子内容",
    "admin_required": "当前账号没有执行该操作的权限",
    "postbot_failed": "PostBot 内联结果获取失败",
    "send_failed": "发送失败",
}


def dm_task_status_label(status: str | None) -> str:
    return DM_TASK_STATUS_LABELS.get(str(status or ""), str(status or "-"))



def dm_log_action_label(action: str | None) -> str:
    return DM_LOG_ACTION_LABELS.get(str(action or ""), str(action or "-"))



def dm_log_status_label(status: str | None) -> str:
    return DM_LOG_STATUS_LABELS.get(str(status or ""), str(status or "-"))



def dm_report_section_label(section: str | None) -> str:
    return DM_REPORT_SECTION_LABELS.get(str(section or ""), str(section or "-"))



def dm_error_label(code: str | None, message: str | None = None) -> str:
    raw_message = str(message or "").strip()
    raw_code = str(code or "").strip()
    if raw_message:
        return raw_message
    if raw_code:
        return DM_ERROR_LABELS.get(raw_code, raw_code)
    return "未知失败"



def format_beijing_timestamp(value) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return str(value)
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")



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
            stop_requested INTEGER NOT NULL DEFAULT 0,
            result_file_path TEXT,
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
        "ALTER TABLE dm_tasks ADD COLUMN stop_requested INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE dm_tasks ADD COLUMN result_file_path TEXT",
    ):
        try:
            conn.execute(statement)
        except sqlite3.OperationalError:
            pass


@dataclass(slots=True)
class DMExportPaths:
    success_txt: Path
    failed_txt: Path
    report_csv: Path
    pending_txt: Path


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

    def list_dm_tasks(self, *, limit: int = 10, offset: int = 0, history: bool = False) -> list[sqlite3.Row]:
        with self.db.lock:
            if history:
                query = "SELECT * FROM dm_tasks ORDER BY id DESC LIMIT ? OFFSET ?"
                return self.db.conn.execute(query, (limit, offset)).fetchall()
            query = "SELECT * FROM dm_tasks WHERE status IN ('queued','running','paused','stopped','error','completed') ORDER BY id DESC LIMIT ? OFFSET ?"
            return self.db.conn.execute(query, (limit, offset)).fetchall()

    def count_dm_tasks(self, *, history: bool = False) -> int:
        with self.db.lock:
            if history:
                return int(self.db.conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE status IN ('completed','stopped','error')").fetchone()[0])
            return int(self.db.conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE status IN ('queued','running','paused','stopped','error','completed')").fetchone()[0])

    def list_dm_task_accounts(self, task_id: int) -> list[sqlite3.Row]:
        with self.db.lock:
            return self.db.conn.execute(
                """
                SELECT ta.*, a.session_file, a.session_name, a.username, a.phone, a.display_name, a.status AS account_runtime_status,
                       a.restriction_status, a.restriction_reason, a.last_error AS account_last_error
                FROM dm_task_accounts ta
                LEFT JOIN accounts a ON a.id = ta.account_id
                WHERE ta.task_id=?
                ORDER BY ta.id ASC
                """,
                (task_id,),
            ).fetchall()

    def list_dm_task_recipients(self, task_id: int, *, statuses: Sequence[str] | None = None, limit: int | None = None) -> list[sqlite3.Row]:
        with self.db.lock:
            where = ["tr.task_id=?"]
            params: list[object] = [task_id]
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                where.append(f"tr.status IN ({placeholders})")
                params.extend(statuses)
            limit_sql = f" LIMIT {int(limit)}" if limit else ""
            query = (
                "SELECT tr.*, r.normalized_input, r.input_type, r.username, r.phone "
                "FROM dm_task_recipients tr JOIN dm_recipients r ON r.id = tr.recipient_id "
                f"WHERE {' AND '.join(where)} ORDER BY tr.id ASC{limit_sql}"
            )
            return self.db.conn.execute(query, params).fetchall()

    def get_dm_task_current_recipient(self, task_id: int) -> sqlite3.Row | None:
        with self.db.lock:
            return self.db.conn.execute(
                """
                SELECT tr.*, r.normalized_input, r.input_type, a.username AS account_username, a.phone AS account_phone, a.display_name AS account_display_name
                FROM dm_task_recipients tr
                JOIN dm_recipients r ON r.id = tr.recipient_id
                LEFT JOIN accounts a ON a.id = tr.assigned_account_id
                WHERE tr.task_id=? AND tr.status='sending'
                ORDER BY tr.updated_at DESC, tr.id DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()

    def list_dm_recent_logs(self, task_id: int, *, limit: int = 5) -> list[sqlite3.Row]:
        with self.db.lock:
            return self.db.conn.execute(
                """
                SELECT l.*, r.normalized_input, a.username AS account_username, a.phone AS account_phone, a.display_name AS account_display_name,
                       ta.sent_success_count AS account_sent_success_count, ta.sent_fail_count AS account_sent_fail_count,
                       ta.last_error AS account_last_error
                FROM dm_send_logs l
                LEFT JOIN dm_recipients r ON r.id = l.recipient_id
                LEFT JOIN accounts a ON a.id = l.account_id
                LEFT JOIN dm_task_accounts ta ON ta.task_id = l.task_id AND ta.account_id = l.account_id
                WHERE l.task_id=?
                ORDER BY l.id DESC
                LIMIT ?
                """,
                (task_id, limit),
            ).fetchall()

    def get_dm_task_failure_summary(self, task_id: int, *, limit: int = 8) -> list[sqlite3.Row]:
        with self.db.lock:
            return self.db.conn.execute(
                """
                SELECT COALESCE(NULLIF(error_message, ''), NULLIF(error_code, ''), '未知失败') AS reason, COUNT(*) AS total
                FROM dm_task_recipients
                WHERE task_id=? AND status='failed'
                GROUP BY COALESCE(NULLIF(error_message, ''), NULLIF(error_code, ''), '未知失败')
                ORDER BY total DESC, reason ASC
                LIMIT ?
                """,
                (task_id, limit),
            ).fetchall()

    def clear_dm_finished_tasks(self) -> int:
        with self.db.lock:
            rows = self.db.conn.execute(
                "SELECT id FROM dm_tasks WHERE status IN ('completed','stopped','error')"
            ).fetchall()
            task_ids = [int(row["id"]) for row in rows]
            if not task_ids:
                return 0
            placeholders = ",".join("?" for _ in task_ids)
            self.db.conn.execute(f"DELETE FROM dm_tasks WHERE id IN ({placeholders})", task_ids)
            self.db.conn.commit()
            return len(task_ids)

    def get_dm_task_processed_count(self, task_id: int) -> int:
        with self.db.lock:
            return int(
                self.db.conn.execute(
                    "SELECT COUNT(*) FROM dm_task_recipients WHERE task_id=? AND status IN ('sending','success','failed','skipped')",
                    (task_id,),
                ).fetchone()[0]
            )

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

    def mark_dm_task_status(self, task_id: int, status: str, *, last_error: str | None = None) -> None:
        with self.db.lock:
            started_expr = "COALESCE(started_at, CURRENT_TIMESTAMP)" if status == "running" else "started_at"
            finished_expr = "CURRENT_TIMESTAMP" if status in {"completed", "error", "stopped"} else "finished_at"
            self.db.conn.execute(
                f"""
                UPDATE dm_tasks SET
                    status=?,
                    last_error=?,
                    started_at={started_expr},
                    finished_at={finished_expr},
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (status, last_error, task_id),
            )
            self.db.conn.commit()

    def set_dm_task_progress_message(self, task_id: int, chat_id: int, message_id: int) -> None:
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE dm_tasks SET progress_chat_id=?, progress_message_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (chat_id, message_id, task_id),
            )
            self.db.conn.commit()

    def request_dm_task_stop(self, task_id: int) -> None:
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE dm_tasks SET stop_requested=1, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (task_id,),
            )
            self.db.conn.commit()

    def should_stop_dm_task(self, task_id: int) -> bool:
        with self.db.lock:
            row = self.db.conn.execute("SELECT stop_requested FROM dm_tasks WHERE id=?", (task_id,)).fetchone()
            return bool(row and int(row[0]))

    def recover_interrupted_dm_tasks(self, *, reason: str) -> int:
        with self.db.lock:
            rows = self.db.conn.execute("SELECT id FROM dm_tasks WHERE status IN ('queued', 'running') ORDER BY id ASC").fetchall()
            task_ids = [int(row["id"]) for row in rows]
            if not task_ids:
                return 0
            placeholders = ",".join("?" for _ in task_ids)
            self.db.conn.execute(
                f"UPDATE dm_tasks SET status='stopped', stop_requested=1, last_error=?, finished_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id IN ({placeholders})",
                [reason, *task_ids],
            )
            self.db.conn.execute(
                f"UPDATE dm_task_accounts SET status='stopped', last_error=?, updated_at=CURRENT_TIMESTAMP WHERE task_id IN ({placeholders}) AND status IN ('queued','running')",
                [reason, *task_ids],
            )
            self.db.conn.execute(
                f"UPDATE dm_task_recipients SET status='pending', updated_at=CURRENT_TIMESTAMP WHERE task_id IN ({placeholders}) AND status='sending'",
                task_ids,
            )
            self.db.conn.commit()
            return len(task_ids)

    def update_dm_task_account(
        self,
        task_id: int,
        account_id: int,
        *,
        status: str | None = None,
        success_delta: int = 0,
        fail_delta: int = 0,
        frequent_delta: int = 0,
        last_error: str | None = None,
    ) -> None:
        with self.db.lock:
            fields = [
                "sent_success_count=sent_success_count + ?",
                "sent_fail_count=sent_fail_count + ?",
                "frequent_error_count=frequent_error_count + ?",
                "last_sent_at=CURRENT_TIMESTAMP",
                "updated_at=CURRENT_TIMESTAMP",
            ]
            params: list[object] = [success_delta, fail_delta, frequent_delta]
            if status is not None:
                fields.insert(0, "status=?")
                params.insert(0, status)
            if last_error is not None:
                fields.append("last_error=?")
                params.append(last_error)
            params.extend([task_id, account_id])
            self.db.conn.execute(
                f"UPDATE dm_task_accounts SET {', '.join(fields)} WHERE task_id=? AND account_id=?",
                params,
            )
            self.db.conn.commit()

    def mark_dm_recipient_sending(self, task_id: int, recipient_id: int, account_id: int) -> None:
        with self.db.lock:
            self.db.conn.execute(
                """
                UPDATE dm_task_recipients SET
                    status='sending',
                    assigned_account_id=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE task_id=? AND recipient_id=?
                """,
                (account_id, task_id, recipient_id),
            )
            self.db.conn.commit()

    def mark_dm_recipient_result(
        self,
        task_id: int,
        recipient_id: int,
        *,
        account_id: int | None,
        status: str,
        error_code: str | None = None,
        error_message: str | None = None,
        increment_retry: bool = False,
    ) -> None:
        with self.db.lock:
            retry_expr = "retry_count + 1" if increment_retry else "retry_count"
            sent_expr = "CURRENT_TIMESTAMP" if status == "success" else "sent_at"
            self.db.conn.execute(
                f"""
                UPDATE dm_task_recipients SET
                    status=?,
                    assigned_account_id=?,
                    error_code=?,
                    error_message=?,
                    retry_count={retry_expr},
                    sent_at={sent_expr},
                    updated_at=CURRENT_TIMESTAMP
                WHERE task_id=? AND recipient_id=?
                """,
                (status, account_id, error_code, error_message, task_id, recipient_id),
            )
            self.db.conn.commit()

    def sync_dm_task_metrics(self, task_id: int) -> None:
        with self.db.lock:
            row = self.db.conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status='success' THEN 1 ELSE 0 END), 0) AS success_count,
                    COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0) AS failed_count,
                    COALESCE(SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END), 0) AS skipped_count
                FROM dm_task_recipients
                WHERE task_id=?
                """,
                (task_id,),
            ).fetchone()
            active_accounts = self.db.conn.execute(
                "SELECT COUNT(*) FROM dm_task_accounts WHERE task_id=? AND status='running'",
                (task_id,),
            ).fetchone()[0]
            self.db.conn.execute(
                """
                UPDATE dm_tasks SET
                    success_count=?,
                    failed_count=?,
                    skipped_count=?,
                    active_accounts=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    int(row["success_count"]),
                    int(row["failed_count"]),
                    int(row["skipped_count"]),
                    int(active_accounts),
                    task_id,
                ),
            )
            self.db.conn.commit()

    def count_dm_pending_recipients(self, task_id: int) -> int:
        with self.db.lock:
            return int(self.db.conn.execute("SELECT COUNT(*) FROM dm_task_recipients WHERE task_id=? AND status IN ('pending','sending')", (task_id,)).fetchone()[0])

    def set_dm_task_result_file(self, task_id: int, result_file_path: str) -> None:
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE dm_tasks SET result_file_path=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (result_file_path, task_id),
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
        report_csv = export_dir / f"dm_task_{task_id}_report.csv"
        pending_txt = export_dir / f"dm_task_{task_id}_pending.txt"

        with self.db.lock:
            task_row = self.db.conn.execute(
                "SELECT * FROM dm_tasks WHERE id=?",
                (task_id,),
            ).fetchone()
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
            pending_rows = self.db.conn.execute(
                """
                SELECT r.normalized_input
                FROM dm_task_recipients tr
                JOIN dm_recipients r ON r.id = tr.recipient_id
                WHERE tr.task_id=? AND tr.status IN ('pending','sending')
                ORDER BY tr.id ASC
                """,
                (task_id,),
            ).fetchall()
            failure_summary_rows = self.db.conn.execute(
                """
                SELECT COALESCE(NULLIF(tr.error_message, ''), NULLIF(tr.error_code, ''), '未知失败') AS reason, COUNT(*) AS total
                FROM dm_task_recipients tr
                WHERE tr.task_id=? AND tr.status='failed'
                GROUP BY COALESCE(NULLIF(tr.error_message, ''), NULLIF(tr.error_code, ''), '未知失败')
                ORDER BY total DESC, reason ASC
                """,
                (task_id,),
            ).fetchall()
            account_rows = self.db.conn.execute(
                """
                SELECT ta.account_id,
                       COALESCE(NULLIF(a.username, ''), NULLIF(a.phone, ''), NULLIF(a.display_name, ''), NULLIF(a.session_name, ''), '#' || ta.account_id) AS account_label,
                       ta.sent_success_count,
                       ta.sent_fail_count,
                       ta.status,
                       ta.last_error
                FROM dm_task_accounts ta
                JOIN accounts a ON a.id = ta.account_id
                WHERE ta.task_id=?
                ORDER BY ta.sent_success_count DESC, ta.account_id ASC
                """,
                (task_id,),
            ).fetchall()
            log_rows = self.db.conn.execute(
                """
                SELECT l.created_at, l.action, l.status,
                       COALESCE(NULLIF(a.username, ''), NULLIF(a.phone, ''), NULLIF(a.display_name, ''), NULLIF(a.session_name, ''), CASE WHEN l.account_id IS NOT NULL THEN '#' || l.account_id ELSE '-' END) AS account_label,
                       COALESCE(r.normalized_input, '-') AS normalized_input,
                       l.message,
                       l.raw_error
                FROM dm_send_logs l
                LEFT JOIN accounts a ON a.id = l.account_id
                LEFT JOIN dm_recipients r ON r.id = l.recipient_id
                WHERE l.task_id=?
                ORDER BY l.id ASC
                """,
                (task_id,),
            ).fetchall()

        success_txt.write_text("\n".join(str(row["normalized_input"]) for row in success_rows), encoding="utf-8-sig")
        failed_txt.write_text("\n".join(str(row["normalized_input"]) for row in failed_rows), encoding="utf-8-sig")
        pending_txt.write_text("\n".join(str(row["normalized_input"]) for row in pending_rows), encoding="utf-8-sig")
        with report_csv.open("w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.writer(fp)
            writer.writerow(["分区", "字段1", "字段2", "字段3", "字段4", "字段5"])
            if task_row is not None:
                pending_count = max(
                    0,
                    int(task_row["total_targets"] or 0)
                    - int(task_row["success_count"] or 0)
                    - int(task_row["failed_count"] or 0)
                    - int(task_row["skipped_count"] or 0),
                )
                writer.writerow([dm_report_section_label("overview"), "任务ID", task_id, "任务状态", dm_task_status_label(task_row["status"]), "-"])
                writer.writerow([dm_report_section_label("overview"), "成功数", int(task_row["success_count"] or 0), "失败数", int(task_row["failed_count"] or 0), "-"])
                writer.writerow([dm_report_section_label("overview"), "待发送数", pending_count, "跳过数", int(task_row["skipped_count"] or 0), "-"])
                writer.writerow([dm_report_section_label("overview"), "并发线程", int(task_row["worker_count"] or 0), "运行账号", int(task_row["active_accounts"] or 0), "-"])
            for row in failure_summary_rows:
                writer.writerow([dm_report_section_label("failure_reason"), dm_error_label(None, str(row["reason"] or "未知失败")), int(row["total"]), "-", "-", "-"])
            for row in account_rows:
                writer.writerow([
                    dm_report_section_label("account_stats"),
                    row["account_label"],
                    int(row["sent_success_count"] or 0),
                    int(row["sent_fail_count"] or 0),
                    dm_task_status_label(row["status"]),
                    str(row["last_error"] or ""),
                ])
            for row in failed_rows:
                writer.writerow([
                    dm_report_section_label("failed_target"),
                    row["normalized_input"],
                    dm_error_label(row["error_code"], row["error_message"]),
                    str(row["error_code"] or "-"),
                    row["retry_count"],
                    "-",
                ])
            for row in log_rows:
                writer.writerow([
                    dm_report_section_label("log"),
                    format_beijing_timestamp(row["created_at"]),
                    row["account_label"],
                    row["normalized_input"],
                    f"{dm_log_action_label(row['action'])}:{dm_log_status_label(row['status'])}",
                    str(row["message"] or row["raw_error"] or "-"),
                ])
        return DMExportPaths(success_txt=success_txt, failed_txt=failed_txt, report_csv=report_csv, pending_txt=pending_txt)

from __future__ import annotations

import asyncio
import json
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    RPCError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import ChannelParticipantsAdmins, User

from .config import Settings
from .database import Database

USERNAME_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_]{5,32})")
INVITE_LINK_RE = re.compile(r"(?:https?://)?t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)")
GROUP_JOIN_BATCH_LIMIT = 5
GROUP_JOIN_COOLDOWN_SECONDS = 300
PROGRESS_FLUSH_EVERY_MESSAGES = 50


@dataclass(slots=True)
class SessionCheckResult:
    status: str
    tg_user_id: int | None
    phone: str | None
    username: str | None
    display_name: str | None
    last_error: str | None


class CollectionManager:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        *,
        on_progress: Callable[[int], Awaitable[None]] | None = None,
        on_complete: Callable[[int], Awaitable[None]] | None = None,
    ):
        self.settings = settings
        self.db = db
        self.on_progress = on_progress
        self.on_complete = on_complete

    async def verify_session_file(self, session_file: Path) -> SessionCheckResult:
        client: TelegramClient | None = None
        try:
            client = self._build_client(session_file)
            await client.connect()
            authorized = await client.is_user_authorized()
            if not authorized:
                return SessionCheckResult(
                    status="unauthorized",
                    tg_user_id=None,
                    phone=None,
                    username=None,
                    display_name=None,
                    last_error="session 未授权或已失效",
                )
            me = await client.get_me()
            return SessionCheckResult(
                status="active",
                tg_user_id=getattr(me, "id", None),
                phone=getattr(me, "phone", None),
                username=getattr(me, "username", None),
                display_name=getattr(me, "first_name", "") and me.first_name or getattr(me, "username", None),
                last_error=None,
            )
        except FloodWaitError as exc:
            return SessionCheckResult(
                status="error",
                tg_user_id=None,
                phone=None,
                username=None,
                display_name=None,
                last_error=f"FloodWait {exc.seconds}s",
            )
        except Exception as exc:  # noqa: BLE001
            return SessionCheckResult(
                status="error",
                tg_user_id=None,
                phone=None,
                username=None,
                display_name=None,
                last_error=self._short_error(exc),
            )
        finally:
            if client is not None:
                await client.disconnect()

    async def verify_account(self, account_row) -> SessionCheckResult:
        result = await self.verify_session_file(Path(account_row["session_file"]))
        self.db.update_account_status(
            account_row["id"],
            status=result.status,
            last_error=result.last_error,
            tg_user_id=result.tg_user_id,
            phone=result.phone,
            username=result.username,
            display_name=result.display_name,
        )
        return result

    async def run_collect_task(self, task_id: int) -> None:
        task = self.db.get_collect_task(task_id)
        if not task:
            return

        try:
            self.db.mark_collect_task_status(task_id, "running")
            await self._emit_progress(task_id)

            task_channels = self.db.list_collect_task_channels(task_id)
            selected_account_ids = self._parse_json_ids(task["account_ids_json"])
            accounts = [row for row in (self.db.get_account(account_id) for account_id in selected_account_ids) if row]
            active_workers = []
            for account in accounts:
                checked = await self.verify_account(account)
                if checked.status == "active":
                    active_workers.append(self.db.get_account(account["id"]))

            if not active_workers:
                self.db.mark_collect_task_status(task_id, "error", last_error="没有可用账号，无法开始采集")
                await self._emit_complete(task_id)
                return

            queue: asyncio.Queue = asyncio.Queue()
            for item in task_channels:
                queue.put_nowait(item)

            worker_count = min(task["worker_count"], len(active_workers), queue.qsize(), self.settings.max_collect_workers)
            worker_fn = self._group_worker if (task["task_type"] or "channel") == "group" else self._worker
            workers = [
                asyncio.create_task(worker_fn(task_id, queue, active_workers[index]))
                for index in range(worker_count)
            ]
            await asyncio.gather(*workers, return_exceptions=True)

            unique_total = self.db.count_unique_usernames(task_id)
            self.db.increment_task_metrics(task_id, unique_total=unique_total)

            final_status = "stopped" if self.db.should_stop_task(task_id) else "completed"
            if final_status == "completed":
                self.db.mark_collect_task_status(task_id, "completed")
            else:
                self.db.mark_collect_task_status(task_id, "stopped")

            if (task["task_type"] or "channel") == "group":
                outputs = self.db.export_group_task_files(task_id, self.settings.export_dir)
                self.db.set_task_result_file(task_id, str(outputs["usernames"]))
            else:
                output_path = self.db.export_task_usernames_txt(task_id, self.settings.export_dir)
                self.db.set_task_result_file(task_id, str(output_path))
            await self._emit_complete(task_id)
        except asyncio.CancelledError:
            self.db.stop_collect_task_now(task_id, reason="任务已停止，账号已释放")
            if (task["task_type"] or "channel") == "group":
                outputs = self.db.export_group_task_files(task_id, self.settings.export_dir)
                self.db.set_task_result_file(task_id, str(outputs["usernames"]))
            else:
                output_path = self.db.export_task_usernames_txt(task_id, self.settings.export_dir)
                self.db.set_task_result_file(task_id, str(output_path))
            await self._emit_complete(task_id)
            return
        except Exception as exc:  # noqa: BLE001
            self.db.mark_collect_task_status(task_id, "error", last_error=self._short_error(exc))
            try:
                if (task["task_type"] or "channel") == "group":
                    outputs = self.db.export_group_task_files(task_id, self.settings.export_dir)
                    self.db.set_task_result_file(task_id, str(outputs["usernames"]))
                else:
                    output_path = self.db.export_task_usernames_txt(task_id, self.settings.export_dir)
                    self.db.set_task_result_file(task_id, str(output_path))
            except Exception:  # noqa: BLE001
                pass
            await self._emit_complete(task_id)
            return

    async def _worker(self, task_id: int, queue: asyncio.Queue, account_row) -> None:
        account_id = account_row["id"]
        session_file = Path(account_row["session_file"])
        client: TelegramClient | None = None
        try:
            client = self._build_client(session_file)
            await client.connect()
            if not await client.is_user_authorized():
                self.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                return
            self.db.update_account_status(account_id, status="collecting", last_error=None)

            while not queue.empty():
                if self.db.should_stop_task(task_id):
                    break
                try:
                    task_channel = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    await self._process_channel(task_id, client, account_id, task_channel)
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.db.update_account_status(account_id, status="error", last_error=self._short_error(exc))
        finally:
            refreshed = self.db.get_account(account_id)
            next_status = "active"
            last_error = None
            if refreshed:
                last_error = refreshed["last_error"]
                if refreshed["status"] in {"unauthorized", "error"}:
                    next_status = refreshed["status"]
            self.db.update_account_status(account_id, status=next_status, last_error=last_error)
            if client is not None:
                await client.disconnect()

    async def _group_worker(self, task_id: int, queue: asyncio.Queue, account_row) -> None:
        account_id = account_row["id"]
        session_file = Path(account_row["session_file"])
        client: TelegramClient | None = None
        joined_since_cooldown = 0
        try:
            client = self._build_client(session_file)
            await client.connect()
            if not await client.is_user_authorized():
                self.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                return
            self.db.update_account_status(account_id, status="collecting", last_error=None)

            while not queue.empty():
                if self.db.should_stop_task(task_id):
                    break
                if joined_since_cooldown >= GROUP_JOIN_BATCH_LIMIT:
                    await self._sleep_with_stop(task_id, GROUP_JOIN_COOLDOWN_SECONDS)
                    joined_since_cooldown = 0
                try:
                    task_channel = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    joined = await self._process_group(task_id, client, account_id, task_channel)
                    if joined:
                        joined_since_cooldown += 1
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.db.update_account_status(account_id, status="error", last_error=self._short_error(exc))
        finally:
            refreshed = self.db.get_account(account_id)
            next_status = "active"
            last_error = None
            if refreshed:
                last_error = refreshed["last_error"]
                if refreshed["status"] in {"unauthorized", "error"}:
                    next_status = refreshed["status"]
            self.db.update_account_status(account_id, status=next_status, last_error=last_error)
            if client is not None:
                await client.disconnect()

    async def _process_channel(self, task_id: int, client: TelegramClient, account_id: int, task_channel) -> None:
        task_channel_id = task_channel["id"]
        channel = task_channel["channel"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, self.db.get_collect_task(task_id)["days_limit"]))

        scanned_messages = 0
        total_hits = 0
        inserted_hits = 0
        last_error = None
        status = "completed"

        self.db.start_task_channel(task_channel_id, account_id)
        try:
            async for message in client.iter_messages(channel):
                if self.db.should_stop_task(task_id):
                    status = "stopped"
                    break

                message_date = getattr(message, "date", None)
                if message_date is not None:
                    if message_date.tzinfo is None:
                        message_date = message_date.replace(tzinfo=timezone.utc)
                    if message_date < cutoff:
                        break

                scanned_messages += 1
                raw_text = getattr(message, "raw_text", None) or getattr(message, "message", None) or ""
                usernames = self._extract_usernames(raw_text)
                if not usernames:
                    continue
                total_hits += len(usernames)
                inserted_hits += self.db.add_collected_usernames(
                    task_id,
                    usernames=usernames,
                    source_channel=channel,
                    source_message_id=getattr(message, "id", None),
                )
                if scanned_messages % PROGRESS_FLUSH_EVERY_MESSAGES == 0:
                    await self._flush_task_progress(
                        task_id,
                        task_channel_id,
                        scanned_messages=scanned_messages,
                        hits=total_hits,
                        unique_hits=inserted_hits,
                    )
        except FloodWaitError as exc:
            status = "error"
            last_error = f"FloodWait {exc.seconds}s"
            self.db.update_account_status(account_id, status="error", last_error=last_error)
        except RPCError as exc:
            status = "error"
            last_error = self._short_error(exc)
        except Exception as exc:  # noqa: BLE001
            status = "error"
            last_error = self._short_error(exc)

        unique_total = self.db.count_unique_usernames(task_id)
        self.db.finish_task_channel(
            task_channel_id,
            status=status,
            scanned_messages=scanned_messages,
            hits=total_hits,
            unique_hits=inserted_hits,
            last_error=last_error,
        )
        self.db.sync_task_metrics(task_id, unique_total=unique_total)
        await self._emit_progress(task_id)

    async def _process_group(self, task_id: int, client: TelegramClient, account_id: int, task_channel) -> bool:
        task_channel_id = task_channel["id"]
        group_target = task_channel["channel"]
        task = self.db.get_collect_task(task_id)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, task["days_limit"]))
        filters = self._parse_filters(task["filters_json"])

        scanned_messages = 0
        total_hits = 0
        inserted_hits = 0
        last_error = None
        status = "completed"
        joined_now = False

        self.db.start_task_channel(task_channel_id, account_id)
        try:
            entity, joined_now = await self._ensure_group_entity(client, group_target)
            admin_ids = await self._load_admin_ids(client, entity)
            async for message in client.iter_messages(entity):
                if self.db.should_stop_task(task_id):
                    status = "stopped"
                    break

                message_date = getattr(message, "date", None)
                if message_date is not None:
                    if message_date.tzinfo is None:
                        message_date = message_date.replace(tzinfo=timezone.utc)
                    if message_date < cutoff:
                        break

                scanned_messages += 1
                sender = await message.get_sender()
                if not isinstance(sender, User):
                    continue

                username = f"@{sender.username}" if getattr(sender, "username", None) else None
                is_bot = bool(getattr(sender, "bot", False))
                is_admin = int(getattr(sender, "id", 0)) in admin_ids
                has_photo = bool(getattr(sender, "photo", None))
                is_premium = bool(getattr(sender, "premium", False) or getattr(sender, "is_premium", False))
                if filters["exclude_bots"] and is_bot:
                    continue
                if filters["exclude_admins"] and is_admin:
                    continue
                if filters["exclude_no_photo"] and not has_photo:
                    continue
                if filters["exclude_no_username"] and not username:
                    continue
                if filters["premium_mode"] == "premium_only" and not is_premium:
                    continue
                if filters["premium_mode"] == "non_premium_only" and is_premium:
                    continue

                total_hits += 1
                inserted_hits += self.db.add_collected_member(
                    task_id,
                    user_id=int(sender.id),
                    username=username,
                    display_name=self._display_name(sender),
                    source_channel=group_target,
                    source_message_id=getattr(message, "id", None),
                    is_bot=is_bot,
                    is_admin=is_admin,
                    has_photo=has_photo,
                    spoke_at=message_date.isoformat() if message_date else None,
                )
                if scanned_messages % PROGRESS_FLUSH_EVERY_MESSAGES == 0:
                    await self._flush_task_progress(
                        task_id,
                        task_channel_id,
                        scanned_messages=scanned_messages,
                        hits=total_hits,
                        unique_hits=inserted_hits,
                        unique_total=self.db.count_unique_usernames(task_id),
                    )
        except FloodWaitError as exc:
            status = "error"
            last_error = f"FloodWait {exc.seconds}s"
            self.db.update_account_status(account_id, status="error", last_error=last_error)
        except (InviteHashInvalidError, InviteHashExpiredError) as exc:
            status = "error"
            last_error = self._short_error(exc)
        except RPCError as exc:
            status = "error"
            last_error = self._short_error(exc)
        except Exception as exc:  # noqa: BLE001
            status = "error"
            last_error = self._short_error(exc)

        unique_total = self.db.count_unique_usernames(task_id)
        self.db.finish_task_channel(
            task_channel_id,
            status=status,
            scanned_messages=scanned_messages,
            hits=total_hits,
            unique_hits=inserted_hits,
            last_error=last_error,
        )
        self.db.sync_task_metrics(task_id, unique_total=unique_total)
        await self._emit_progress(task_id)
        return joined_now

    async def _flush_task_progress(
        self,
        task_id: int,
        task_channel_id: int,
        *,
        scanned_messages: int,
        hits: int,
        unique_hits: int,
        unique_total: int | None = None,
    ) -> None:
        self.db.update_task_channel_progress(
            task_channel_id,
            scanned_messages=scanned_messages,
            hits=hits,
            unique_hits=unique_hits,
        )
        self.db.sync_task_metrics(task_id, unique_total=unique_total)
        await self._emit_progress(task_id)

    def _build_client(self, session_file: Path) -> TelegramClient:
        session_base = str(session_file)
        if session_base.endswith(".session"):
            session_base = session_base[:-8]
        try:
            return TelegramClient(session_base, self.settings.api_id, self.settings.api_hash)
        except ValueError as exc:
            if "too many values to unpack" not in str(exc):
                raise
            compat_session = self._build_compat_session_file(session_file)
            compat_base = str(compat_session)
            if compat_base.endswith(".session"):
                compat_base = compat_base[:-8]
            return TelegramClient(compat_base, self.settings.api_id, self.settings.api_hash)

    def _build_compat_session_file(self, session_file: Path) -> Path:
        compat_file = session_file.with_name(f"{session_file.stem}.compat.session")
        shutil.copy2(session_file, compat_file)

        try:
            with sqlite3.connect(compat_file) as conn:
                conn.row_factory = sqlite3.Row
                columns = [row[1] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()]
                if not columns:
                    raise ValueError("session 文件里缺少 sessions 表")
                row = conn.execute("SELECT * FROM sessions LIMIT 1").fetchone()
                if row is None:
                    raise ValueError("session 文件里没有可用的登录记录")
                row_dict = {key: row[key] for key in row.keys()}
                required = {
                    "dc_id": row_dict.get("dc_id"),
                    "server_address": row_dict.get("server_address"),
                    "port": row_dict.get("port"),
                    "auth_key": row_dict.get("auth_key"),
                    "takeout_id": row_dict.get("takeout_id"),
                }
                if required["dc_id"] is None or required["server_address"] is None or required["port"] is None or required["auth_key"] is None:
                    raise ValueError("session 文件缺少 Telethon 必要字段")

                conn.execute("ALTER TABLE sessions RENAME TO sessions_backup")
                conn.execute(
                    """
                    CREATE TABLE sessions (
                        dc_id INTEGER PRIMARY KEY,
                        server_address TEXT,
                        port INTEGER,
                        auth_key BLOB,
                        takeout_id INTEGER
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO sessions (dc_id, server_address, port, auth_key, takeout_id) VALUES (?, ?, ?, ?, ?)",
                    (
                        required["dc_id"],
                        required["server_address"],
                        required["port"],
                        required["auth_key"],
                        required["takeout_id"],
                    ),
                )
                conn.execute("DROP TABLE sessions_backup")

                version_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='version'"
                ).fetchone()
                if version_exists:
                    conn.execute("DELETE FROM version")
                    conn.execute("INSERT INTO version (version) VALUES (7)")
                conn.commit()
        except sqlite3.DatabaseError as exc:
            raise ValueError(f"session 文件已损坏或不是有效 SQLite：{self._short_error(exc)}") from exc
        return compat_file

    async def _ensure_group_entity(self, client: TelegramClient, target: str):
        invite_hash = self._extract_invite_hash(target)
        if invite_hash:
            invite = await client(CheckChatInviteRequest(invite_hash))
            if hasattr(invite, "chat"):
                return invite.chat, False
            updates = await client(ImportChatInviteRequest(invite_hash))
            chats = list(getattr(updates, "chats", []) or [])
            if not chats:
                raise ValueError("加群成功，但没有拿到群实体")
            return chats[0], True

        normalized = target if target.startswith("@") else f"@{target.lstrip('@')}"
        entity = await client.get_entity(normalized)
        joined_now = False
        try:
            await client(JoinChannelRequest(entity))
            joined_now = True
        except UserAlreadyParticipantError:
            joined_now = False
        except RPCError as exc:
            text = str(exc).lower()
            if "user_already_participant" not in text:
                raise
        entity = await client.get_entity(normalized)
        return entity, joined_now

    async def _load_admin_ids(self, client: TelegramClient, entity) -> set[int]:
        admin_ids: set[int] = set()
        try:
            async for admin in client.iter_participants(entity, filter=ChannelParticipantsAdmins):
                admin_ids.add(int(admin.id))
        except Exception:  # noqa: BLE001
            return admin_ids
        return admin_ids

    async def _sleep_with_stop(self, task_id: int, seconds: int) -> None:
        remaining = max(0, int(seconds))
        while remaining > 0:
            if self.db.should_stop_task(task_id):
                return
            await asyncio.sleep(min(5, remaining))
            remaining -= 5

    @staticmethod
    def _extract_invite_hash(target: str) -> str | None:
        match = INVITE_LINK_RE.search((target or "").strip())
        return match.group(1) if match else None

    @staticmethod
    def _display_name(user: User) -> str:
        parts = [value for value in [getattr(user, "first_name", None), getattr(user, "last_name", None)] if value]
        if parts:
            return " ".join(parts)
        return getattr(user, "username", None) or str(getattr(user, "id", "-"))

    @staticmethod
    def _parse_filters(raw: str | None) -> dict[str, object]:
        defaults = {
            "exclude_bots": False,
            "exclude_admins": False,
            "exclude_no_photo": False,
            "exclude_no_username": False,
            "premium_mode": "all",
        }
        if not raw:
            return defaults
        try:
            loaded = json.loads(raw)
        except Exception:  # noqa: BLE001
            return defaults
        for key in ("exclude_bots", "exclude_admins", "exclude_no_photo", "exclude_no_username"):
            if key in loaded:
                defaults[key] = bool(loaded[key])
        premium_mode = str(loaded.get("premium_mode") or "all")
        if premium_mode in {"all", "premium_only", "non_premium_only"}:
            defaults["premium_mode"] = premium_mode
        return defaults

    @staticmethod
    def _extract_usernames(text: str) -> list[str]:
        if not text:
            return []
        usernames = {
            f"@{match.group(1)}"
            for match in USERNAME_RE.finditer(text)
            if not match.group(1).lower().endswith("bot")
        }
        return sorted(usernames)

    @staticmethod
    def _parse_json_ids(raw: str) -> list[int]:
        try:
            values = list(json.loads(raw))
        except Exception:  # noqa: BLE001
            return []
        result: list[int] = []
        for value in values:
            try:
                result.append(int(value))
            except Exception:  # noqa: BLE001
                continue
        return result

    @staticmethod
    def _short_error(exc: Exception) -> str:
        text = str(exc).strip() or exc.__class__.__name__
        return text[:300]

    async def _emit_progress(self, task_id: int) -> None:
        if self.on_progress:
            await self.on_progress(task_id)

    async def _emit_complete(self, task_id: int) -> None:
        if self.on_complete:
            await self.on_complete(task_id)

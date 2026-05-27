from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RpcError

from .config import Settings
from .database import Database

USERNAME_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_]{5,32})")


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
        client = self._build_client(session_file)
        try:
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
        workers = [
            asyncio.create_task(self._worker(task_id, queue, active_workers[index]))
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

        output_path = self.db.export_task_usernames_txt(task_id, self.settings.export_dir)
        self.db.set_task_result_file(task_id, str(output_path))
        await self._emit_complete(task_id)

    async def _worker(self, task_id: int, queue: asyncio.Queue, account_row) -> None:
        account_id = account_row["id"]
        session_file = Path(account_row["session_file"])
        client = self._build_client(session_file)
        try:
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
        finally:
            refreshed = self.db.get_account(account_id)
            next_status = "active"
            last_error = None
            if refreshed:
                last_error = refreshed["last_error"]
                if refreshed["status"] in {"unauthorized", "error"}:
                    next_status = refreshed["status"]
            self.db.update_account_status(account_id, status=next_status, last_error=last_error)
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
        except FloodWaitError as exc:
            status = "error"
            last_error = f"FloodWait {exc.seconds}s"
            self.db.update_account_status(account_id, status="error", last_error=last_error)
        except RpcError as exc:
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
        self.db.increment_task_metrics(
            task_id,
            scanned_delta=scanned_messages,
            hits_delta=total_hits,
            finished_delta=1,
            unique_total=unique_total,
        )
        await self._emit_progress(task_id)

    def _build_client(self, session_file: Path) -> TelegramClient:
        session_base = str(session_file)
        if session_base.endswith(".session"):
            session_base = session_base[:-8]
        return TelegramClient(session_base, self.settings.api_id, self.settings.api_hash)

    @staticmethod
    def _extract_usernames(text: str) -> list[str]:
        if not text:
            return []
        return sorted({f"@{match.group(1)}" for match in USERNAME_RE.finditer(text)})

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

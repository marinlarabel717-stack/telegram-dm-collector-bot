from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from telethon.errors import FloodWaitError, RPCError

from .collector import CollectionManager
from .database import Database
from .dm_logging import compose_log
from .dm_policy import DMTaskPolicy
from .dm_repository import DmRepository

logger = logging.getLogger(__name__)


class DmSenderManager:
    def __init__(self, repository: DmRepository, db: Database, collection_manager: CollectionManager, *, on_progress=None, on_complete=None):
        self.repository = repository
        self.db = db
        self.collection_manager = collection_manager
        self.on_progress = on_progress
        self.on_complete = on_complete

    async def run_task(self, task_id: int, policy: DMTaskPolicy | None = None) -> None:
        task = self.repository.get_dm_task(task_id)
        if not task:
            logger.warning(compose_log("任务不存在，无法启动", task_id=task_id))
            return
        active_policy = policy or self._policy_from_task(task)
        recipients = self.repository.list_dm_task_recipients(task_id, statuses=("pending",))
        accounts = [row for row in self.repository.list_dm_task_accounts(task_id) if row["account_runtime_status"] == "active"]
        if not recipients:
            self.repository.mark_dm_task_status(task_id, "completed")
            await self._emit_complete(task_id)
            return
        if not accounts:
            self.repository.mark_dm_task_status(task_id, "error", last_error="没有可用账号")
            await self._emit_complete(task_id)
            return

        queue: asyncio.Queue = asyncio.Queue()
        for row in recipients:
            queue.put_nowait(row)

        worker_count = min(int(task["worker_count"] or 1), len(accounts), queue.qsize())
        self.repository.mark_dm_task_status(task_id, "running")
        await self._emit_progress(task_id)
        logger.info(compose_log(f"启动｜目标={queue.qsize()}｜账号={len(accounts)}｜并发={worker_count}", task_id=task_id))

        workers = [
            asyncio.create_task(self._account_worker(task_id, queue, accounts[index], active_policy))
            for index in range(worker_count)
        ]
        try:
            await asyncio.gather(*workers, return_exceptions=False)
        except asyncio.CancelledError:
            self.repository.request_dm_task_stop(task_id)
            self.repository.mark_dm_task_status(task_id, "stopped", last_error="管理员手动停止任务")
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(compose_log(f"发送器异常｜{exc}", task_id=task_id))
            self.repository.mark_dm_task_status(task_id, "error", last_error=self.collection_manager._short_error(exc))
        else:
            pending = self.repository.count_dm_pending_recipients(task_id)
            if self.repository.should_stop_dm_task(task_id):
                self.repository.mark_dm_task_status(task_id, "stopped", last_error="管理员手动停止任务")
            elif pending > 0:
                self.repository.mark_dm_task_status(task_id, "stopped", last_error="账号已用尽，任务已停止")
            else:
                self.repository.mark_dm_task_status(task_id, "completed")
        self.repository.sync_dm_task_metrics(task_id)
        export = self.repository.export_task_results(task_id, self.collection_manager.settings.export_dir)
        self.repository.set_dm_task_result_file(task_id, str(export.success_txt))
        await self._emit_complete(task_id)

    async def _account_worker(self, task_id: int, queue: asyncio.Queue, account_row, policy: DMTaskPolicy) -> None:
        account_id = int(account_row["account_id"])
        session_file = Path(account_row["session_file"])
        client = None
        success_count = int(account_row["sent_success_count"] or 0)
        frequent_errors = int(account_row["frequent_error_count"] or 0)
        self.repository.update_dm_task_account(task_id, account_id, status="running", last_error=None)
        try:
            client = self.collection_manager._build_client(session_file)
            await client.connect()
            if not await client.is_user_authorized():
                self.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                self.repository.update_dm_task_account(task_id, account_id, status="error", last_error="session 未登录")
                return
            self.db.update_account_status(account_id, status="collecting", last_error=None)
            logger.info(compose_log("开始发送", task_id=task_id, account_id=account_id))

            while not queue.empty():
                if self.repository.should_stop_dm_task(task_id):
                    break
                if policy.should_rotate_account(success_count):
                    logger.info(compose_log(f"达到单号上限｜success={success_count}", task_id=task_id, account_id=account_id))
                    break
                try:
                    recipient = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                recipient_id = int(recipient["recipient_id"])
                target = str(recipient["normalized_input"])
                self.repository.mark_dm_recipient_sending(task_id, recipient_id, account_id)
                try:
                    entity = await client.get_input_entity(target)
                    if policy.typing_simulation:
                        async with client.action(entity, "typing"):
                            await asyncio.sleep(min(2.5, max(0.5, policy.delay_window.next_delay() / 3)))
                    payload = json.loads(str(self.repository.get_dm_task(task_id)["payload_json"] or "{}"))
                    text = str(payload.get("text") or "").strip()
                    await client.send_message(entity, text)
                    success_count += 1
                    self.repository.mark_dm_recipient_result(task_id, recipient_id, account_id=account_id, status="success")
                    self.repository.update_dm_task_account(task_id, account_id, success_delta=1, last_error=None)
                    self.repository.add_send_log(task_id=task_id, account_id=account_id, recipient_id=recipient_id, action="send", status="success", message=target)
                    logger.info(compose_log("发送成功", task_id=task_id, account_id=account_id, recipient=target))
                except FloodWaitError as exc:
                    wait_seconds = int(getattr(exc, "seconds", 0) or 0)
                    self.repository.mark_dm_recipient_result(task_id, recipient_id, account_id=account_id, status="pending", error_code="flood_wait", error_message=f"FloodWait {wait_seconds}s", increment_retry=True)
                    self.repository.add_send_log(task_id=task_id, account_id=account_id, recipient_id=recipient_id, action="send", status="retry", message=target, raw_error=f"FloodWait {wait_seconds}s")
                    logger.warning(compose_log(f"命中限速｜等待={wait_seconds}s", task_id=task_id, account_id=account_id, recipient=target))
                    await asyncio.sleep(max(wait_seconds, 1))
                    queue.put_nowait(recipient)
                except Exception as exc:  # noqa: BLE001
                    short = self.collection_manager._short_error(exc)
                    lowered = short.lower()
                    frequent_hit = "too many requests" in lowered or "peerflood" in lowered or "user is restricted" in lowered
                    frequent_errors += 1 if frequent_hit else 0
                    final_status = "failed"
                    self.repository.mark_dm_recipient_result(task_id, recipient_id, account_id=account_id, status=final_status, error_code="send_failed", error_message=short, increment_retry=True)
                    self.repository.update_dm_task_account(task_id, account_id, fail_delta=1, frequent_delta=1 if frequent_hit else 0, last_error=short)
                    self.repository.add_send_log(task_id=task_id, account_id=account_id, recipient_id=recipient_id, action="send", status="failed", message=target, raw_error=short)
                    logger.warning(compose_log(f"发送失败｜{short}", task_id=task_id, account_id=account_id, recipient=target))
                    if policy.should_stop_account_for_frequent(frequent_errors):
                        logger.warning(compose_log(f"达到频繁阈值，停止该账号｜count={frequent_errors}", task_id=task_id, account_id=account_id))
                        break
                finally:
                    queue.task_done()
                    self.repository.sync_dm_task_metrics(task_id)
                    await self._emit_progress(task_id)
                    await asyncio.sleep(policy.delay_window.next_delay())
        except RPCError as exc:
            short = self.collection_manager._short_error(exc)
            self.repository.update_dm_task_account(task_id, account_id, status="error", last_error=short)
            logger.exception(compose_log(f"账号异常｜{short}", task_id=task_id, account_id=account_id))
        finally:
            refreshed = self.db.get_account(account_id)
            next_status = "active"
            last_error = None
            if refreshed:
                last_error = refreshed["last_error"]
                if refreshed["status"] in {"unauthorized", "error"}:
                    next_status = refreshed["status"]
            self.db.update_account_status(account_id, status=next_status, last_error=last_error)
            current = self.repository.get_dm_task(task_id)
            final_task_account_status = "stopped" if current and self.repository.should_stop_dm_task(task_id) else "completed"
            self.repository.update_dm_task_account(task_id, account_id, status=final_task_account_status, last_error=last_error)
            self.repository.sync_dm_task_metrics(task_id)
            await self._emit_progress(task_id)
            if client is not None:
                await client.disconnect()

    def _policy_from_task(self, task) -> DMTaskPolicy:
        raw = str(task["policy_json"] or "{}")
        data = json.loads(raw)
        return DMTaskPolicy(
            per_account_success_limit=int(data.get("per_account_success_limit") or 40),
            auto_switch_account=bool(data.get("auto_switch_account", True)),
            auto_stop_when_accounts_exhausted=bool(data.get("auto_stop_when_accounts_exhausted", True)),
            typing_simulation=bool(data.get("typing_simulation", True)),
        )

    async def _emit_progress(self, task_id: int) -> None:
        if self.on_progress:
            await self.on_progress(task_id)

    async def _emit_complete(self, task_id: int) -> None:
        if self.on_complete:
            await self.on_complete(task_id)

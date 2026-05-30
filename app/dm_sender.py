from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path

from telethon.errors import FloodWaitError, RPCError
from telethon.tl import functions, types

from .collector import CollectionManager
from .database import Database
from .dm_links import parse_channel_post_link
from .dm_logging import compose_log
from .dm_postbot import fetch_postbot_inline_result
from .dm_policy import DMTaskPolicy, DelayWindow, RetryPolicy
from .dm_repository import DmRepository
from .dm_transport_tgmatrix import TgMatrixDmTransport

logger = logging.getLogger(__name__)
DM_SEND_RECONNECT_RETRIES = 2
DM_MAX_PARALLEL_ACCOUNTS = 100


class DmSenderManager:
    def __init__(self, repository: DmRepository, db: Database, collection_manager: CollectionManager, *, on_progress=None, on_complete=None):
        self.repository = repository
        self.db = db
        self.collection_manager = collection_manager
        self.on_progress = on_progress
        self.on_complete = on_complete
        self.transport = TgMatrixDmTransport(self)

    async def run_task(self, task_id: int, policy: DMTaskPolicy | None = None) -> None:
        task = self.repository.get_dm_task(task_id)
        if not task:
            logger.warning(compose_log("任务不存在，无法启动", task_id=task_id))
            return
        active_policy = policy or self._policy_from_task(task)
        recipients = self.repository.list_dm_task_recipients(task_id, statuses=("pending",))
        task_accounts = self.repository.list_dm_task_accounts(task_id)
        accounts = []
        unavailable_reasons: list[str] = []
        for row in task_accounts:
            can_send, reason = self._account_can_send(row)
            if can_send:
                accounts.append(row)
                continue
            self.repository.update_dm_task_account(task_id, int(row["account_id"]), status="error", last_error=reason)
            self.repository.add_send_log(
                task_id=task_id,
                account_id=int(row["account_id"]),
                action="account_check",
                status="failed",
                message=f"账号不可用｜{reason}",
                raw_error=str(row["account_last_error"] or row["restriction_reason"] or reason),
            )
            unavailable_reasons.append(f"#{row['account_id']} {reason}")
        if not recipients:
            self.repository.mark_dm_task_status(task_id, "completed")
            await self._emit_complete(task_id)
            return
        selected_account_ids = [int(row["account_id"]) for row in accounts]
        fallback_account_ids: list[int] = []
        if active_policy.auto_switch_account:
            selected_account_id_set = set(selected_account_ids)
            requester_id = int(task["requester_id"] or 0)
            for row in self.db.get_active_accounts(owner_id=requester_id):
                account_id = int(row["id"])
                if account_id in selected_account_id_set:
                    continue
                if not self._account_can_send(row)[0]:
                    continue
                self.repository.ensure_dm_task_account(task_id, account_id)
                fallback_account_ids.append(account_id)
        task_accounts = self.repository.list_dm_task_accounts(task_id)
        task_account_map = {int(row["account_id"]): row for row in task_accounts}
        accounts = [task_account_map[account_id] for account_id in selected_account_ids if account_id in task_account_map]
        fallback_accounts = [task_account_map[account_id] for account_id in fallback_account_ids if account_id in task_account_map]
        account_seed_rows = accounts + fallback_accounts
        if not account_seed_rows:
            reason_text = "；".join(unavailable_reasons[:3]) if unavailable_reasons else "没有可用账号"
            summary = self._build_task_stop_summary(task_id, fallback_total_accounts=len(task_accounts), fallback_pending=len(recipients))
            self.repository.mark_dm_task_status(task_id, "stopped", last_error=f"没有可用账号，任务已停止｜{summary}｜{reason_text}")
            self.repository.sync_dm_task_metrics(task_id)
            await self._emit_complete(task_id)
            return

        worker_count = min(int(task["worker_count"] or 1), DM_MAX_PARALLEL_ACCOUNTS, len(account_seed_rows), len(recipients))
        active_account_ids = {int(row["account_id"]) for row in account_seed_rows}

        queue: asyncio.Queue = asyncio.Queue()
        for row in recipients:
            queue.put_nowait(row)

        account_queue: asyncio.Queue = asyncio.Queue()
        for row in account_seed_rows:
            account_queue.put_nowait(row)

        self.repository.mark_dm_task_status(task_id, "running")
        await self._emit_progress(task_id)
        logger.info(compose_log(f"启动｜目标={queue.qsize()}｜账号={len(accounts)}｜并发={worker_count}｜补位账号={max(0, len(account_seed_rows) - worker_count)}", task_id=task_id))

        workers: list[asyncio.Task] = []
        try:
            current_parallel = min(worker_count, len(account_seed_rows), queue.qsize())
            if current_parallel > 0:
                logger.info(
                    compose_log(
                        f"全局账号池启动｜总账号={len(account_seed_rows)}｜即时并发={current_parallel}｜剩余目标={queue.qsize()}",
                        task_id=task_id,
                    )
                )
                workers = [
                    asyncio.create_task(
                        self._account_slot_worker(
                            task_id,
                            queue,
                            account_queue,
                            active_policy,
                            active_account_ids=active_account_ids,
                            slot_index=index + 1,
                        )
                    )
                    for index in range(current_parallel)
                ]
                await asyncio.gather(*workers, return_exceptions=False)
                workers = []
        except asyncio.CancelledError:
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            current_task = self.repository.get_dm_task(task_id)
            stop_reason = str((current_task["last_error"] if current_task else "") or "").strip()
            if stop_reason == "管理员手动停止任务":
                final_reason = stop_reason
            else:
                final_reason = stop_reason or self._infer_task_stop_reason(
                    task_id,
                    active_account_ids=active_account_ids,
                    fallback_total_accounts=len(active_account_ids),
                    fallback_pending=self.repository.count_dm_pending_recipients(task_id),
                )
            self.repository.request_dm_task_stop(task_id)
            self.repository.mark_dm_task_status(task_id, "stopped", last_error=final_reason)
            raise
        except Exception as exc:  # noqa: BLE001
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            logger.exception(compose_log(f"发送器异常｜{exc}", task_id=task_id))
            self.repository.mark_dm_task_status(task_id, "error", last_error=self.collection_manager._short_error(exc))
        else:
            pending = self.repository.count_dm_pending_recipients(task_id)
            current_task = self.repository.get_dm_task(task_id)
            if self.repository.should_stop_dm_task(task_id):
                stop_reason = str((current_task["last_error"] if current_task else "") or "").strip()
                stop_reason = stop_reason or self._infer_task_stop_reason(
                    task_id,
                    active_account_ids=active_account_ids,
                    fallback_total_accounts=len(active_account_ids),
                    fallback_pending=pending,
                )
                self.repository.mark_dm_task_status(task_id, "stopped", last_error=stop_reason)
            elif pending > 0:
                summary = self._build_task_stop_summary(
                    task_id,
                    active_account_ids=active_account_ids,
                    fallback_total_accounts=len(active_account_ids),
                    fallback_pending=pending,
                )
                reason_prefix = "当前所选账号已用尽，且未开启自动补号，任务已停止" if not active_policy.auto_switch_account else "所选账号与账号列表补位均已用尽，任务已停止"
                self.repository.mark_dm_task_status(task_id, "stopped", last_error=f"{reason_prefix}｜{summary}")
            else:
                self.repository.mark_dm_task_status(task_id, "completed")
        self.repository.sync_dm_task_metrics(task_id)
        export = self.repository.export_task_results(task_id, self.collection_manager.settings.export_dir)
        self.repository.set_dm_task_result_file(task_id, str(export.success_txt))
        await self._emit_complete(task_id)

    def _account_can_send(self, account_row) -> tuple[bool, str]:
        keys = set(account_row.keys()) if hasattr(account_row, "keys") else set()
        runtime_status = str(account_row["account_runtime_status"] if "account_runtime_status" in keys else account_row["status"] or "")
        restriction_status = str(account_row["restriction_status"] or "unknown")
        restriction_reason = str(account_row["restriction_reason"] or "").strip()
        account_last_error = str(account_row["account_last_error"] if "account_last_error" in keys else account_row["last_error"] or "").strip()
        if runtime_status != "active":
            return False, account_last_error or f"账号状态={runtime_status}"
        if restriction_status in {"session_invalid", "frozen"}:
            return False, restriction_reason or f"限制状态={restriction_status}"
        return True, ""

    async def _account_slot_worker(
        self,
        task_id: int,
        queue: asyncio.Queue,
        account_queue: asyncio.Queue,
        policy: DMTaskPolicy,
        *,
        active_account_ids: set[int],
        slot_index: int,
    ) -> None:
        while not queue.empty() and not self.repository.should_stop_dm_task(task_id):
            try:
                account_row = account_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            account_id = int(account_row["account_id"])
            logger.info(compose_log(f"槽位接管账号｜slot={slot_index}", task_id=task_id, account_id=account_id))
            try:
                await self._account_worker(task_id, queue, account_queue, account_row, policy, active_account_ids=active_account_ids)
            finally:
                account_queue.task_done()

    async def _account_worker(
        self,
        task_id: int,
        queue: asyncio.Queue,
        account_queue: asyncio.Queue,
        account_row,
        policy: DMTaskPolicy,
        *,
        active_account_ids: set[int],
    ) -> None:
        account_id = int(account_row["account_id"])
        session_file = Path(account_row["session_file"])
        client = None
        success_count = int(account_row["sent_success_count"] or 0)
        frequent_errors = int(account_row["frequent_error_count"] or 0)
        too_many_requests_hits = int(account_row["too_many_requests_count"] or 0)
        task = self.repository.get_dm_task(task_id)
        payload = json.loads(str(task["payload_json"] or "{}")) if task else {}
        content_type = str((task["content_type"] if task else None) or payload.get("content_type") or "text")
        self.repository.update_dm_task_account(task_id, account_id, status="running", last_error=None)
        try:
            self._append_runtime_log(task_id, account_id=account_id, message="正在连接 Telegram")
            client = await self.collection_manager.connect_client(session_file, account_row=account_row)
            if not await client.is_user_authorized():
                self.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                self.repository.update_dm_task_account(task_id, account_id, status="error", last_error="session 未登录")
                return
            self.db.update_account_status(account_id, status="collecting", last_error=None)
            self._append_runtime_log(task_id, account_id=account_id, message="账号连接成功，开始处理目标")
            logger.info(compose_log("开始发送", task_id=task_id, account_id=account_id))

            while not queue.empty():
                if self.repository.should_stop_dm_task(task_id):
                    break
                if policy.should_rotate_account(success_count):
                    logger.info(compose_log(f"达到单号上限｜success={success_count}", task_id=task_id, account_id=account_id))
                    self.repository.update_dm_task_account(
                        task_id,
                        account_id,
                        status="stopped",
                        last_error=f"达到单号上限（{success_count}/{policy.per_account_success_limit}）",
                    )
                    break
                try:
                    recipient = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                recipient_id = int(recipient["recipient_id"])
                target = str(recipient["normalized_input"])
                retry_count = int(recipient["retry_count"] or 0)
                post_attempt_delay = 0.0
                self.repository.mark_dm_recipient_sending(task_id, recipient_id, account_id)
                send_attempt = 0
                try:
                    while True:
                        try:
                            self._append_runtime_log(task_id, account_id=account_id, recipient_id=recipient_id, message="正在解析目标")
                            entity, cleanup = await self.transport.resolve_target_entity(client, target)
                            try:
                                self._append_runtime_log(
                                    task_id,
                                    account_id=account_id,
                                    recipient_id=recipient_id,
                                    message=self.transport.dispatch_progress_message(content_type, policy),
                                )
                                sent_message, sent_messages = await self.transport.dispatch_payload(client, entity, payload, content_type, policy)
                                await self.transport.apply_post_send_actions(
                                    client,
                                    entity,
                                    sent_message,
                                    sent_messages,
                                    policy,
                                    task_id=task_id,
                                    account_id=account_id,
                                    recipient_id=recipient_id,
                                )
                            finally:
                                await self.transport.run_entity_cleanup(cleanup)
                            success_count += 1
                            post_attempt_delay = policy.delay_window.next_delay()
                            self.repository.mark_dm_recipient_result(task_id, recipient_id, account_id=account_id, status="success")
                            self.repository.update_dm_task_account(task_id, account_id, success_delta=1, last_error=None)
                            self.repository.add_send_log(task_id=task_id, account_id=account_id, recipient_id=recipient_id, action="send", status="success", message=self._action_success_message(content_type))
                            logger.info(compose_log("发送成功", task_id=task_id, account_id=account_id, recipient=target))
                            break
                        except Exception as exc:  # noqa: BLE001
                            if send_attempt >= DM_SEND_RECONNECT_RETRIES or not self.collection_manager._is_disconnect_error(exc) or self.repository.should_stop_dm_task(task_id):
                                raise
                            send_attempt += 1
                            short_error = self.collection_manager._short_error(exc)
                            logger.warning(compose_log(f"连接中断，重连后重试｜第{send_attempt}次｜{short_error}", task_id=task_id, account_id=account_id, recipient=target))
                            self.repository.add_send_log(
                                task_id=task_id,
                                account_id=account_id,
                                recipient_id=recipient_id,
                                action="send",
                                status="retry",
                                message=f"连接中断，正在重连后重试（第{send_attempt}次）",
                                raw_error=short_error,
                            )
                            client = await self.collection_manager._reconnect_client(client, session_file, account_row=account_row)
                            self.db.update_account_status(account_id, status="collecting", last_error=None)
                except FloodWaitError as exc:
                    wait_seconds = int(getattr(exc, "seconds", 0) or 0)
                    should_switch_account = self._should_retry_with_other_account(
                        task_id,
                        account_id,
                        error_code="flood_wait",
                        retry_count=retry_count,
                        policy=policy,
                        active_account_ids=active_account_ids,
                    )
                    if should_switch_account:
                        self.repository.mark_dm_recipient_result(
                            task_id,
                            recipient_id,
                            account_id=account_id,
                            status="pending",
                            error_code="flood_wait",
                            error_message=f"FloodWait {wait_seconds}s",
                            increment_retry=True,
                        )
                        self.repository.update_dm_task_account(task_id, account_id, fail_delta=1, last_error=f"FloodWait {wait_seconds}s", status="stopped")
                        self.repository.add_send_log(
                            task_id=task_id,
                            account_id=account_id,
                            recipient_id=recipient_id,
                            action="send",
                            status="retry",
                            message="当前账号触发限速，目标已回退队列并换号重试",
                            raw_error=f"FloodWait {wait_seconds}s",
                        )
                        logger.warning(compose_log(f"命中限速，目标回退队列换号重试｜等待={wait_seconds}s", task_id=task_id, account_id=account_id, recipient=target))
                        queue.put_nowait(recipient)
                        break
                    if retry_count + 1 > policy.retry_policy.max_retries:
                        post_attempt_delay = self._failure_backoff_delay("flood_wait_exhausted", policy)
                        self.repository.mark_dm_recipient_result(
                            task_id,
                            recipient_id,
                            account_id=account_id,
                            status="failed",
                            error_code="flood_wait",
                            error_message=f"FloodWait {wait_seconds}s，重试已耗尽",
                            increment_retry=True,
                        )
                        self.repository.update_dm_task_account(task_id, account_id, fail_delta=1, last_error=f"FloodWait {wait_seconds}s")
                        self.repository.add_send_log(
                            task_id=task_id,
                            account_id=account_id,
                            recipient_id=recipient_id,
                            action="send",
                            status="failed",
                            message=self._append_limit_progress("官方限速，重试已耗尽", success_count, policy),
                            raw_error=f"FloodWait {wait_seconds}s",
                        )
                        logger.warning(compose_log(f"限速重试耗尽｜等待={wait_seconds}s", task_id=task_id, account_id=account_id, recipient=target))
                        continue
                    self.repository.mark_dm_recipient_result(
                        task_id,
                        recipient_id,
                        account_id=account_id,
                        status="pending",
                        error_code="flood_wait",
                        error_message=f"FloodWait {wait_seconds}s",
                        increment_retry=True,
                    )
                    self.repository.add_send_log(
                        task_id=task_id,
                        account_id=account_id,
                        recipient_id=recipient_id,
                        action="send",
                        status="retry",
                        message=self._append_limit_progress("官方限速，自动等待后重试", success_count, policy),
                        raw_error=f"FloodWait {wait_seconds}s",
                    )
                    logger.warning(compose_log(f"命中限速｜等待={wait_seconds}s", task_id=task_id, account_id=account_id, recipient=target))
                    await asyncio.sleep(max(wait_seconds, 1))
                    queue.put_nowait(recipient)
                except Exception as exc:  # noqa: BLE001
                    raw_error = self.collection_manager._short_error(exc)
                    error_code, error_message, frequent_hit = self.transport.classify_send_error(exc)
                    post_attempt_delay = self._failure_backoff_delay(error_code, policy)
                    if frequent_hit and error_code != "too_many_requests":
                        frequent_errors += 1
                    too_many_requests_delta = 0
                    too_many_requests_limit = int(policy.retry_policy.stop_account_after_too_many_requests or 0)
                    too_many_requests_reason = error_message
                    if error_code == "too_many_requests":
                        too_many_requests_hits += 1
                        too_many_requests_delta = 1
                        too_many_requests_reason = self._format_too_many_requests_reason(
                            too_many_requests_hits,
                            too_many_requests_limit,
                        )
                    threshold_reached = (
                        error_code == "too_many_requests"
                        and policy.should_stop_account_for_too_many_requests(too_many_requests_hits)
                    )
                    should_switch_account = self._should_retry_with_other_account(
                        task_id,
                        account_id,
                        error_code=error_code,
                        retry_count=retry_count,
                        policy=policy,
                        active_account_ids=active_account_ids,
                    )
                    if threshold_reached:
                        should_switch_account = False
                    if should_switch_account:
                        account_status = "stopped"
                        account_error = error_message
                        retry_message = f"{error_message}，目标已回退队列并换号重试"
                        requeue_account = False
                        if error_code == "too_many_requests":
                            account_status = "queued"
                            account_error = too_many_requests_reason
                            retry_message = f"{too_many_requests_reason}，当前账号先回账号池，目标已回退队列并换号重试"
                            requeue_account = True
                        self.repository.mark_dm_recipient_result(
                            task_id,
                            recipient_id,
                            account_id=account_id,
                            status="pending",
                            error_code=error_code,
                            error_message=error_message,
                            increment_retry=True,
                        )
                        self.repository.update_dm_task_account(
                            task_id,
                            account_id,
                            status=account_status,
                            fail_delta=1,
                            frequent_delta=1 if frequent_hit and error_code != "too_many_requests" else 0,
                            too_many_requests_delta=too_many_requests_delta,
                            last_error=account_error,
                        )
                        self.repository.add_send_log(
                            task_id=task_id,
                            account_id=account_id,
                            recipient_id=recipient_id,
                            action="send",
                            status="retry",
                            message=retry_message,
                            raw_error=raw_error,
                        )
                        logger.warning(compose_log(f"发送失败，目标回退队列换号重试｜{error_code}｜{error_message}", task_id=task_id, account_id=account_id, recipient=target))
                        queue.put_nowait(recipient)
                        if requeue_account:
                            refreshed_account_row = self.repository.get_dm_task_account(task_id, account_id)
                            if refreshed_account_row is not None:
                                account_queue.put_nowait(refreshed_account_row)
                        break
                    self.repository.mark_dm_recipient_result(
                        task_id,
                        recipient_id,
                        account_id=account_id,
                        status="failed",
                        error_code=error_code,
                        error_message=error_message,
                        increment_retry=True,
                    )
                    self.repository.update_dm_task_account(
                        task_id,
                        account_id,
                        fail_delta=1,
                        frequent_delta=1 if frequent_hit and error_code != "too_many_requests" else 0,
                        too_many_requests_delta=too_many_requests_delta,
                        last_error=too_many_requests_reason if error_code == "too_many_requests" else error_message,
                    )
                    log_message = error_message
                    if error_code == "too_many_requests":
                        log_message = too_many_requests_reason
                    elif frequent_hit:
                        log_message = self._append_limit_progress(log_message, success_count, policy)
                    self.repository.add_send_log(
                        task_id=task_id,
                        account_id=account_id,
                        recipient_id=recipient_id,
                        action="send",
                        status="failed",
                        message=log_message,
                        raw_error=raw_error,
                    )
                    logger.warning(compose_log(f"发送失败｜{error_code}｜{error_message}", task_id=task_id, account_id=account_id, recipient=target))
                    if self._is_account_terminal_error(error_code):
                        stop_reason = self._handle_terminal_account_error(
                            task_id,
                            account_id,
                            error_code=error_code,
                            error_message=error_message,
                            raw_error=raw_error,
                            auto_switch_account=policy.auto_switch_account,
                            active_account_ids=active_account_ids,
                        )
                        if stop_reason:
                            logger.warning(compose_log(stop_reason, task_id=task_id, account_id=account_id))
                        break
                    if policy.should_stop_account_for_too_many_requests(too_many_requests_hits):
                        logger.warning(
                            compose_log(
                                f"请求过于频繁次数达到阈值，停止该账号｜count={too_many_requests_hits}",
                                task_id=task_id,
                                account_id=account_id,
                            )
                        )
                        reason_text = f"请求过于频繁次数达到阈值（{too_many_requests_hits}/{policy.retry_policy.stop_account_after_too_many_requests}）"
                        self.repository.update_dm_task_account(
                            task_id,
                            account_id,
                            status="stopped",
                            last_error=reason_text,
                        )
                        break
                    if policy.should_stop_account_for_frequent(frequent_errors):
                        logger.warning(compose_log(f"达到频繁阈值，停止该账号｜count={frequent_errors}", task_id=task_id, account_id=account_id))
                        reason_text = f"账号频繁失败次数达到阈值（{frequent_errors}/{policy.retry_policy.stop_account_after_user_frequent}）"
                        self.repository.update_dm_task_account(task_id, account_id, status="stopped", last_error=reason_text)
                        break
                finally:
                    queue.task_done()
                    self.repository.sync_dm_task_metrics(task_id)
                    await self._emit_progress(task_id)
                    if post_attempt_delay > 0 and not self.repository.should_stop_dm_task(task_id) and not queue.empty():
                        await asyncio.sleep(post_attempt_delay)
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
            row_after_run = next((row for row in self.repository.list_dm_task_accounts(task_id) if int(row["account_id"]) == account_id), None)
            current_account_status = str(row_after_run["status"] or "") if row_after_run else ""
            if current_account_status in {"error", "stopped"}:
                final_task_account_status = current_account_status
                if row_after_run and row_after_run["last_error"]:
                    last_error = row_after_run["last_error"]
            elif current_account_status == "queued":
                final_task_account_status = "queued"
                if row_after_run and row_after_run["last_error"]:
                    last_error = row_after_run["last_error"]
            elif next_status in {"unauthorized", "error"}:
                final_task_account_status = "error"
            elif current and self.repository.should_stop_dm_task(task_id):
                final_task_account_status = "stopped"
            else:
                final_task_account_status = "completed"
            self.repository.update_dm_task_account(task_id, account_id, status=final_task_account_status, last_error=last_error)
            self.repository.sync_dm_task_metrics(task_id)
            await self._emit_progress(task_id)
            if client is not None:
                await client.disconnect()

    async def _send_text_message(self, client, entity, text: str, policy: DMTaskPolicy, *, parse_mode: str | None = None):
        content = str(text or "").strip()
        if not content:
            return None
        if policy.typing_simulation:
            try:
                async with client.action(entity, "typing"):
                    await asyncio.sleep(min(2.5, max(0.5, policy.delay_window.next_delay() / 3)))
            except Exception:
                logger.debug("typing 模拟失败，改为直接发送", exc_info=True)
        if parse_mode:
            return await self._send_with_recovery(
                client,
                entity,
                lambda: client.send_message(entity, content, parse_mode=parse_mode),
                expected_text=content,
            )
        return await self._send_with_recovery(
            client,
            entity,
            lambda: client.send_message(entity, content),
            expected_text=content,
        )

    async def _dispatch_single_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        if content_type == "media":
            media_path = Path(str(payload.get("media_path") or "")).expanduser()
            if not media_path.exists():
                raise FileNotFoundError(f"媒体文件不存在: {media_path}")
            caption = str(payload.get("caption") or "").strip() or None
            media_kind = self._detect_dm_media_kind(media_path, str(payload.get("media_kind") or "document"))
            return await self._send_with_recovery(
                client,
                entity,
                lambda: client.send_file(
                    entity,
                    file=str(media_path),
                    caption=caption,
                    force_document=(media_kind == "document"),
                    supports_streaming=(media_kind == "video"),
                ),
                expected_text=caption or "",
                expect_media=True,
            )
        if content_type == "post":
            post_code = str(payload.get("body") or payload.get("post_code") or payload.get("text") or "").strip()
            _, inline_result = await fetch_postbot_inline_result(client, post_code)
            return await self._send_with_recovery(
                client,
                entity,
                lambda: inline_result.click(entity),
            )
        if content_type == "forward":
            forward_link = str(payload.get("forward_link") or "").strip()
            if forward_link:
                source_peer, source_message_id = await self._resolve_channel_post_link(client, forward_link)
                return await self._send_with_recovery(
                    client,
                    entity,
                    lambda: client.forward_messages(entity, messages=source_message_id, from_peer=source_peer, drop_author=False),
                    expect_media=True,
                )
            source_chat_id = payload.get("source_chat_id")
            source_message_id = payload.get("source_message_id")
            if not source_chat_id or not source_message_id:
                raise ValueError("频道帖子链接不能为空")
            from_peer = await client.get_input_entity(int(source_chat_id))
            return await self._send_with_recovery(
                client,
                entity,
                lambda: client.forward_messages(entity, messages=int(source_message_id), from_peer=from_peer, drop_author=False),
                expect_media=True,
            )
        main_text = str(payload.get("body") or payload.get("text") or "").strip()
        return await self._send_text_message(client, entity, main_text, policy)

    async def _dispatch_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        if content_type == "reply":
            return await self._dispatch_reply_payload(client, entity, payload, policy)
        mode = str(payload.get("mode") or "single")
        if mode != "three_stage":
            main_result = await self._dispatch_single_payload(client, entity, payload, content_type, policy)
            return self._normalize_sent_message(main_result), self._collect_sent_messages(main_result)

        greeting = str(payload.get("greeting") or "").strip()
        closing = str(payload.get("closing") or "").strip()
        all_messages = []
        if greeting:
            greeting_result = await self._send_text_message(client, entity, greeting, policy)
            all_messages.extend(self._collect_sent_messages(greeting_result))
            await asyncio.sleep(max(0.0, float(policy.stage1_delay_seconds or 0)))

        main_result = await self._dispatch_single_payload(client, entity, payload, content_type, policy)
        main_sent = self._normalize_sent_message(main_result)
        all_messages.extend(self._collect_sent_messages(main_result))

        if closing:
            await asyncio.sleep(max(0.0, float(policy.stage2_delay_seconds or 0)))
            closing_result = await self._send_text_message(client, entity, closing, policy)
            all_messages.extend(self._collect_sent_messages(closing_result))
        return main_sent, all_messages

    async def _dispatch_reply_payload(self, client, entity, payload: dict, policy: DMTaskPolicy):
        greeting = str(payload.get("greeting") or payload.get("text") or "").strip()
        default_reply = str(payload.get("body") or payload.get("reply_text") or "").strip()
        closing = str(payload.get("closing") or "").strip()
        keyword_rules = payload.get("reply_keyword_rules") or []

        all_messages = []
        baseline_incoming_id = await self._latest_incoming_message_id(client, entity)
        if greeting:
            greeting_result = await self._send_text_message(client, entity, greeting, policy)
            all_messages.extend(self._collect_sent_messages(greeting_result))

        incoming_message = await self._wait_for_reply_message(
            client,
            entity,
            after_message_id=baseline_incoming_id,
            timeout_seconds=float(policy.reply_wait_timeout_seconds or 300),
        )
        reply_text, matched_keywords = self._pick_reply_text(incoming_message, default_reply, keyword_rules)
        if not reply_text:
            raise RuntimeError("对方回复了，但没有配置可发送的回复文案")

        reply_delay = max(0.0, float(policy.reply_delay_seconds or 0))
        if reply_delay > 0:
            await asyncio.sleep(reply_delay)
        reply_result = await self._send_text_message(client, entity, reply_text, policy)
        all_messages.extend(self._collect_sent_messages(reply_result))
        main_sent = self._normalize_sent_message(reply_result)

        if closing:
            await asyncio.sleep(max(0.0, float(policy.stage2_delay_seconds or 0)))
            closing_result = await self._send_text_message(client, entity, closing, policy)
            all_messages.extend(self._collect_sent_messages(closing_result))

        return main_sent, all_messages

    @staticmethod
    def _build_message_parts(payload: dict) -> list[str]:
        mode = str(payload.get("mode") or "single")
        if mode == "three_stage":
            return [
                part for part in (
                    str(payload.get("greeting") or "").strip(),
                    str(payload.get("body") or payload.get("post_code") or "").strip(),
                    str(payload.get("closing") or "").strip(),
                ) if part
            ]
        return [str(payload.get("text") or payload.get("post_code") or "").strip()]

    async def _resolve_channel_post_link(self, client, link: str):
        parsed = parse_channel_post_link(link)
        if not parsed:
            raise ValueError("频道帖子链接格式不正确")
        if parsed["kind"] == "public":
            source_peer = await client.get_input_entity(f"@{parsed['username']}")
        else:
            source_peer = await client.get_input_entity(int(f"-100{parsed['channel_id']}"))
        return source_peer, int(parsed["message_id"])

    def _policy_from_task(self, task) -> DMTaskPolicy:
        raw = str(task["policy_json"] or "{}")
        data = json.loads(raw)
        return DMTaskPolicy(
            per_account_success_limit=int(data.get("per_account_success_limit") or 40),
            auto_switch_account=bool(data.get("auto_switch_account", True)),
            auto_stop_when_accounts_exhausted=bool(data.get("auto_stop_when_accounts_exhausted", True)),
            typing_simulation=bool(data.get("typing_simulation", True)),
            delay_window=DelayWindow(
                min_seconds=float(data.get("delay_min") or 8),
                max_seconds=float(data.get("delay_max") or 15),
            ),
            stage1_delay_seconds=float(data.get("stage1_delay_seconds") or 5),
            stage2_delay_seconds=float(data.get("stage2_delay_seconds") or 3),
            pin_after_send=bool(data.get("pin_after_send", False)),
            pin_delay_seconds=float(data.get("pin_delay_seconds") or 3),
            delete_dialog_after_send=bool(data.get("delete_dialog_after_send", False)),
            delete_dialog_delay_seconds=float(data.get("delete_dialog_delay_seconds") or 0),
            reply_delay_seconds=float(data.get("reply_delay_seconds") or 5),
            reply_wait_timeout_seconds=float(data.get("reply_wait_timeout_seconds") or 300),
            retry_policy=RetryPolicy(
                max_retries=int(data.get("max_retries") or 3),
                stop_account_after_user_frequent=int(data.get("stop_account_after_user_frequent") or 30),
                stop_account_after_too_many_requests=int(data.get("stop_account_after_too_many_requests") or 40),
            ),
        )

    def _classify_send_error(self, exc: Exception) -> tuple[str, str, bool]:
        short = self.collection_manager._short_error(exc)
        lowered = short.lower()
        if "cannot send requests while disconnected" in lowered or ("disconnected" in lowered and "request" in lowered):
            return "account_disconnected", "账号掉线了，连接已经断开，没法继续发请求", False
        if "too many requests" in lowered or "retry after" in lowered:
            return "too_many_requests", "请求过于频繁", True
        if "the key is not registered in the system" in lowered:
            return "session_invalid", "账号鉴权 key 无效或未注册，当前账号不能继续发送", False
        if any(keyword in lowered for keyword in ("auth key", "authkey", "unauthorized", "session revoked", "user deactivated", "input_user_deactivated", "phone number banned", "user_deactivated_ban", "deactivated", "banned", "revoked")):
            return "session_invalid", "账号失效或被封禁了，当前 session 不能继续使用", False
        if "peerflood" in lowered:
            return "peer_flood", "官方判定发送过于频繁", True
        if "you can't write in this chat" in lowered or "chat_write_forbidden" in lowered or "settypingrequest" in lowered:
            return "chat_write_forbidden", "这个会话当前不允许发送消息", False
        if "privacy" in lowered or "privacyrestricted" in lowered or "user is not mutual contact" in lowered:
            return "privacy_restricted", "对方隐私限制，无法私信", False
        if "chat_send_media_forbidden" in lowered or "send media" in lowered and "forbidden" in lowered:
            return "media_forbidden", "当前聊天不允许发送媒体", False
        if "chat_send_plain_forbidden" in lowered:
            return "text_forbidden", "当前聊天不允许发送文本", False
        if "chat_admin_required" in lowered or "pin" in lowered and "admin" in lowered:
            return "admin_required", "当前账号在这个会话里没有管理员权限", False
        if "message author required" in lowered or "forwards are restricted" in lowered or "forward" in lowered and "forbidden" in lowered:
            return "forward_forbidden", "这个目标不允许转发该帖子内容", False
        if "inline bot" in lowered or "bot response timeout" in lowered or "next_offset_invalid" in lowered:
            return "postbot_failed", "PostBot 内联结果获取失败", False
        if "等待对方回复超时" in short:
            return "reply_timeout", "等了很久，对方一直没回复", False
        if "username not occupied" in lowered or "cannot find" in lowered or "no user has" in lowered or "entity not found" in lowered or "nobody is using this username" in lowered or "username is unacceptable" in lowered:
            return "user_not_found", "目标用户名无效或当前账号无法解析", False
        if "bot method invalid" in lowered or "bot invalid" in lowered:
            return "bot_target", "目标不是可私信的普通用户", False
        if "user is blocked" in lowered or "you blocked" in lowered:
            return "blocked", "对方已拉黑或账号关系异常", False
        if "user is restricted" in lowered or "mutual" in lowered:
            return "mutual_limit", "账号存在双向或发送限制", True
        if "frozen" in lowered:
            return "frozen", "账号疑似冻结", True
        return "send_failed", short or exc.__class__.__name__, False

    async def _resolve_target_entity(self, client, target: str):
        last_error: Exception | None = None
        candidates = [str(target or "").strip()]
        normalized = candidates[0]
        if normalized.startswith("@"):
            stripped = normalized[1:].strip()
            if stripped:
                candidates.append(stripped)
        for candidate in candidates:
            if not candidate:
                continue
            try:
                return await client.get_input_entity(candidate)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            try:
                entity = await client.get_entity(candidate)
                return await client.get_input_entity(entity)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("目标为空，无法解析")

    def _append_runtime_log(self, task_id: int, *, account_id: int | None = None, recipient_id: int | None = None, message: str) -> None:
        self.repository.add_send_log(
            task_id=task_id,
            account_id=account_id,
            recipient_id=recipient_id,
            action="send",
            status="running",
            message=message,
        )

    @staticmethod
    def _dispatch_progress_message(content_type: str, payload: dict, policy: DMTaskPolicy) -> str:
        normalized = str(content_type or "text")
        if normalized == "forward":
            return "正在抓取频道帖子并转发"
        if normalized == "post":
            return "正在获取 PostBot 内联内容并发送"
        if normalized == "reply":
            timeout = int(float(policy.reply_wait_timeout_seconds or 300))
            return f"准备发送招呼并等待对方回复（最长{timeout}秒）"
        if normalized == "media":
            return "正在发送媒体内容"
        return "正在发送文本内容"

    def _is_account_terminal_error(self, error_code: str) -> bool:
        return error_code in {"account_disconnected", "session_invalid", "frozen"}

    def _should_retry_with_other_account(
        self,
        task_id: int,
        account_id: int,
        *,
        error_code: str,
        retry_count: int,
        policy: DMTaskPolicy,
        active_account_ids: set[int] | None = None,
    ) -> bool:
        if not policy.auto_switch_account:
            return False
        if retry_count + 1 > int(policy.retry_policy.max_retries or 0):
            return False
        if error_code not in {"flood_wait", "account_disconnected", "session_invalid", "frozen", "peer_flood", "too_many_requests", "mutual_limit"}:
            return False
        return not self._task_has_no_other_usable_accounts(
            task_id,
            exclude_account_id=account_id,
            active_account_ids=active_account_ids,
        )

    def _request_task_auto_stop(self, task_id: int, reason: str) -> str:
        current_task = self.repository.get_dm_task(task_id)
        current_status = str((current_task["status"] if current_task else "") or "")
        self.repository.mark_dm_task_status(
            task_id,
            current_status if current_status in {"queued", "running", "paused"} else "running",
            last_error=reason,
        )
        self.repository.request_dm_task_stop(task_id)
        return reason

    def _build_account_auto_stop_reason(
        self,
        task_id: int,
        account_id: int,
        reason_text: str,
        *,
        active_account_ids: set[int] | None = None,
        fallback_total_accounts: int | None = None,
        fallback_pending: int | None = None,
    ) -> str:
        summary = self._build_task_stop_summary(
            task_id,
            active_account_ids=active_account_ids,
            fallback_total_accounts=fallback_total_accounts,
            fallback_pending=fallback_pending,
        )
        return f"自动停止：账号 #{account_id} {reason_text}｜{summary}"

    def _infer_task_stop_reason(
        self,
        task_id: int,
        *,
        active_account_ids: set[int] | None = None,
        fallback_total_accounts: int | None = None,
        fallback_pending: int | None = None,
    ) -> str:
        task = self.repository.get_dm_task(task_id)
        task_reason = str((task["last_error"] if task else "") or "").strip()
        if task_reason:
            return task_reason
        account_rows = self.repository.list_dm_task_accounts(task_id)
        for row in account_rows:
            if active_account_ids is not None and int(row["account_id"]) not in active_account_ids:
                continue
            account_reason = str(row["last_error"] or "").strip()
            if str(row["status"] or "") in {"stopped", "error"} and account_reason:
                return self._build_account_auto_stop_reason(
                    task_id,
                    int(row["account_id"]),
                    f"停止原因：{account_reason}",
                    active_account_ids=active_account_ids,
                    fallback_total_accounts=fallback_total_accounts,
                    fallback_pending=fallback_pending,
                )
        summary = self._build_task_stop_summary(
            task_id,
            active_account_ids=active_account_ids,
            fallback_total_accounts=fallback_total_accounts,
            fallback_pending=fallback_pending,
        )
        return f"任务已自动停止，但当前未拿到更具体的停止原因｜{summary}"

    def _handle_terminal_account_error(
        self,
        task_id: int,
        account_id: int,
        *,
        error_code: str,
        error_message: str,
        raw_error: str,
        auto_switch_account: bool,
        active_account_ids: set[int] | None = None,
    ) -> str:
        runtime_status = "error"
        task_account_status = "error"
        if error_code == "session_invalid":
            runtime_status = "unauthorized"
            self.repository.update_account_restriction(
                account_id,
                restriction_status="session_invalid",
                restriction_reason=error_message,
                raw_reply=raw_error,
            )
        elif error_code == "frozen":
            runtime_status = "active"
            task_account_status = "stopped"
            self.repository.update_account_restriction(
                account_id,
                restriction_status="frozen",
                restriction_reason=error_message,
                raw_reply=raw_error,
            )
        self.db.update_account_status(account_id, status=runtime_status, last_error=error_message)
        self.repository.update_dm_task_account(task_id, account_id, status=task_account_status, last_error=error_message)
        if self._task_has_no_other_usable_accounts(task_id, exclude_account_id=account_id, active_account_ids=active_account_ids):
            reason = self._build_account_auto_stop_reason(
                task_id,
                account_id,
                f"{error_message}，且没有其他可用账号，任务已自动停止",
                active_account_ids=active_account_ids,
            )
            self._request_task_auto_stop(task_id, reason)
            return reason
        return f"{error_message}，已停止当前账号，任务继续使用其余可用账号发送"

    def _build_task_stop_summary(
        self,
        task_id: int,
        *,
        active_account_ids: set[int] | None = None,
        fallback_total_accounts: int | None = None,
        fallback_pending: int | None = None,
    ) -> str:
        account_rows = self.repository.list_dm_task_accounts(task_id)
        scoped_rows = [row for row in account_rows if active_account_ids is None or int(row["account_id"]) in active_account_ids]
        total_accounts = fallback_total_accounts if fallback_total_accounts is not None else len(scoped_rows)
        stopped_accounts = sum(1 for row in scoped_rows if str(row["status"] or "") in {"stopped", "error", "completed"})
        pending = fallback_pending if fallback_pending is not None else self.repository.count_dm_pending_recipients(task_id)
        return f"已停账号 {stopped_accounts}/{total_accounts} 个，剩余目标 {pending} 个"

    @staticmethod
    def _format_too_many_requests_reason(current_hits: int, limit: int) -> str:
        if limit > 0:
            return f"请求过于频繁（{current_hits}/{limit}）"
        return f"请求过于频繁（累计 {current_hits} 次）"

    def _task_has_no_other_usable_accounts(self, task_id: int, *, exclude_account_id: int, active_account_ids: set[int] | None = None) -> bool:
        for row in self.repository.list_dm_task_accounts(task_id):
            if int(row["account_id"]) == exclude_account_id:
                continue
            if active_account_ids is not None and int(row["account_id"]) not in active_account_ids:
                continue
            task_status = str(row["status"] or "")
            runtime_status = str(row["account_runtime_status"] or "")
            restriction_status = str(row["restriction_status"] or "unknown")
            if task_status not in {"queued", "running"}:
                continue
            if runtime_status in {"unauthorized", "error"}:
                continue
            if restriction_status in {"session_invalid", "frozen"}:
                continue
            return False
        return True

    async def _apply_post_send_actions(self, client, entity, sent_message, sent_messages, policy: DMTaskPolicy, *, task_id: int, account_id: int, recipient_id: int) -> None:
        elapsed = 0.0
        if policy.pin_after_send and sent_message is not None:
            try:
                pin_delay = max(0.0, float(policy.pin_delay_seconds or 0))
                if pin_delay > 0:
                    await asyncio.sleep(pin_delay)
                    elapsed += pin_delay
                await client.pin_message(entity, sent_message, notify=False)
                self.repository.add_send_log(
                    task_id=task_id,
                    account_id=account_id,
                    recipient_id=recipient_id,
                    action="pin",
                    status="success",
                    message=f"发送后 {int(policy.pin_delay_seconds or 0)} 秒已自动置顶",
                )
            except Exception as exc:  # noqa: BLE001
                raw = self.collection_manager._short_error(exc)
                _, friendly, _ = self._classify_send_error(exc)
                self.repository.add_send_log(
                    task_id=task_id,
                    account_id=account_id,
                    recipient_id=recipient_id,
                    action="pin",
                    status="failed",
                    message=f"置顶失败｜{friendly}",
                    raw_error=raw,
                )

        if not policy.delete_dialog_after_send:
            return

        message_ids = self._extract_message_ids(sent_messages)
        if not message_ids:
            return

        try:
            delete_delay = max(0.0, float(policy.delete_dialog_delay_seconds or 0))
            remaining = max(0.0, delete_delay - elapsed)
            if remaining > 0:
                await asyncio.sleep(remaining)
            await client.delete_messages(entity, message_ids, revoke=False)
            self.repository.add_send_log(
                task_id=task_id,
                account_id=account_id,
                recipient_id=recipient_id,
                action="delete",
                status="success",
                message=f"发送后 {int(policy.delete_dialog_delay_seconds or 0)} 秒已自动删除自己的聊天框消息",
            )
        except Exception as exc:  # noqa: BLE001
            raw = self.collection_manager._short_error(exc)
            _, friendly, _ = self._classify_send_error(exc)
            self.repository.add_send_log(
                task_id=task_id,
                account_id=account_id,
                recipient_id=recipient_id,
                action="delete",
                status="failed",
                message=f"删除对话框失败｜{friendly}",
                raw_error=raw,
            )

    @staticmethod
    def _normalize_sent_message(result):
        if isinstance(result, list):
            return result[-1] if result else None
        return result

    @staticmethod
    def _collect_sent_messages(result) -> list:
        if result is None:
            return []
        if isinstance(result, list):
            return [item for item in result if item is not None]
        return [result]

    @staticmethod
    def _extract_message_ids(messages) -> list[int]:
        message_ids: list[int] = []
        for item in messages or []:
            message_id = getattr(item, "id", None)
            if message_id is None:
                continue
            try:
                message_ids.append(int(message_id))
            except (TypeError, ValueError):
                continue
        return message_ids

    async def _send_with_recovery(self, client, entity, sender, *, expected_text: str = "", expect_media: bool = False):
        started_at_ms = int(time.time() * 1000)
        try:
            return await sender()
        except Exception:
            recovered = await self._find_actually_sent_result(
                client,
                entity,
                started_at_ms=started_at_ms,
                expected_text=expected_text,
                expect_media=expect_media,
            )
            if recovered is not None:
                return recovered
            raise

    async def _find_actually_sent_result(self, client, entity, *, started_at_ms: int, expected_text: str = "", expect_media: bool = False):
        normalized_expected = self._normalize_compare_text(expected_text)
        fallback_message = None
        for attempt in range(4):
            messages = await client.get_messages(entity, limit=8)
            if messages:
                for message in messages:
                    if not bool(getattr(message, "out", False)):
                        continue
                    sent_at_ms = self._read_message_date_ms(message)
                    if isinstance(sent_at_ms, int) and sent_at_ms + 20000 < started_at_ms:
                        continue
                    if fallback_message is None:
                        fallback_message = message
                    actual_text = self._normalize_compare_text(self._read_message_text(message))
                    actual_has_media = getattr(message, "media", None) is not None
                    text_matched = True
                    if normalized_expected:
                        text_matched = (
                            actual_text == normalized_expected
                            or normalized_expected in actual_text
                            or actual_text in normalized_expected
                        )
                    media_matched = actual_has_media if expect_media else True
                    if text_matched and media_matched:
                        return message
            if attempt < 3:
                await asyncio.sleep(0.45)
        return fallback_message

    @staticmethod
    def _normalize_compare_text(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def _read_message_text(message) -> str:
        for key in ("raw_text", "rawText", "message", "text"):
            value = getattr(message, key, None)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    @staticmethod
    def _read_message_date_ms(message) -> int | None:
        value = getattr(message, "date", None)
        if value is None:
            return None
        timestamp = getattr(value, "timestamp", None)
        if callable(timestamp):
            try:
                return int(timestamp() * 1000)
            except Exception:
                return None
        return None

    def _classify_send_error(self, exc: Exception) -> tuple[str, str, bool]:
        short = self.collection_manager._short_error(exc)
        lowered = short.lower()
        if "cannot send requests while disconnected" in lowered or ("disconnected" in lowered and "request" in lowered):
            return "account_disconnected", "账号掉线了，连接已经断开，没法继续发请求", False
        if "too many requests" in lowered or "retry after" in lowered:
            return "too_many_requests", "请求过于频繁", True
        if "the key is not registered in the system" in lowered:
            return "session_invalid", "账号鉴权 key 无效或未注册，当前账号不能继续发送", False
        if any(keyword in lowered for keyword in ("auth key", "authkey", "unauthorized", "session revoked", "user deactivated", "input_user_deactivated", "phone number banned", "user_deactivated_ban", "deactivated", "banned", "revoked")):
            return "session_invalid", "账号失效或被封禁了，当前 session 不能继续使用", False
        if "peerflood" in lowered:
            return "peer_flood", "官方判定发送过于频繁", True
        if "you can't write in this chat" in lowered or "chat_write_forbidden" in lowered or "settypingrequest" in lowered:
            return "chat_write_forbidden", "这个会话当前不允许发送消息", False
        if "privacy" in lowered or "privacyrestricted" in lowered or "user is not mutual contact" in lowered:
            return "privacy_restricted", "对方隐私限制，无法私信", False
        if "allow_payment_required" in lowered:
            return "privacy_restricted", "对方开启了付费私信，当前账号无法直接发送", False
        if "chat_send_media_forbidden" in lowered or ("send media" in lowered and "forbidden" in lowered):
            return "media_forbidden", "当前聊天不允许发送媒体", False
        if "chat_send_plain_forbidden" in lowered:
            return "text_forbidden", "当前聊天不允许发送文本", False
        if "chat_admin_required" in lowered or ("pin" in lowered and "admin" in lowered):
            return "admin_required", "当前账号在这个会话里没有管理员权限", False
        if "message author required" in lowered or "forwards are restricted" in lowered or ("forward" in lowered and "forbidden" in lowered):
            return "forward_forbidden", "这个目标不允许转发该帖子内容", False
        if "inline bot" in lowered or "bot response timeout" in lowered or "next_offset_invalid" in lowered:
            return "postbot_failed", "PostBot 内联结果获取失败", False
        if "等待对方回复超时" in short:
            return "reply_timeout", "等了很久，对方一直没回复", False
        if "phone_number_invalid" in lowered or "phone number invalid" in lowered:
            return "user_not_found", "目标手机号格式不正确或无法导入为联系人", False
        if "username not occupied" in lowered or "cannot find" in lowered or "no user has" in lowered or "entity not found" in lowered or "nobody is using this username" in lowered or "username is unacceptable" in lowered:
            return "user_not_found", "目标用户名无效或当前账号无法解析", False
        if "bot method invalid" in lowered or "bot invalid" in lowered:
            return "bot_target", "目标不是可私信的普通用户", False
        if "user is blocked" in lowered or "you blocked" in lowered:
            return "blocked", "对方已拉黑或账号关系异常", False
        if "user is restricted" in lowered or "mutual" in lowered:
            return "mutual_limit", "账号存在双向或发送限制", True
        if "frozen" in lowered:
            return "frozen", "账号疑似冻结", True
        return "send_failed", short or exc.__class__.__name__, False

    async def _resolve_target_entity(self, client, target: str):
        normalized = str(target or "").strip()
        if not normalized:
            raise RuntimeError("目标为空，无法解析")

        if re.fullmatch(r"\+?\d{6,20}", normalized):
            phone = normalized if normalized.startswith("+") else f"+{normalized}"
            imported = await client(
                functions.contacts.ImportContactsRequest(
                    contacts=[
                        types.InputPhoneContact(
                            client_id=int(time.time() * 1000),
                            phone=phone,
                            first_name="Direct",
                            last_name="Message",
                        )
                    ]
                )
            )
            users = list(getattr(imported, "users", []) or [])
            imported_user = users[0] if users else None
            if imported_user is None:
                raise ValueError("PHONE_NUMBER_INVALID")
            entity = await client.get_input_entity(imported_user)

            async def cleanup():
                try:
                    await client(functions.contacts.DeleteByPhonesRequest(phones=[phone]))
                except Exception:
                    pass

            return entity, cleanup

        last_error: Exception | None = None
        candidates = [normalized]
        if normalized.startswith("@"):
            stripped = normalized[1:].strip()
            if stripped:
                candidates.append(stripped)
        for candidate in candidates:
            if not candidate:
                continue
            try:
                return await client.get_input_entity(candidate), None
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            try:
                entity = await client.get_entity(candidate)
                return await client.get_input_entity(entity), None
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("目标为空，无法解析")

    async def _run_entity_cleanup(self, cleanup) -> None:
        if cleanup is None:
            return
        try:
            await cleanup()
        except Exception:
            logger.debug("清理临时联系人失败", exc_info=True)

    async def _apply_post_send_actions(self, client, entity, sent_message, sent_messages, policy: DMTaskPolicy, *, task_id: int, account_id: int, recipient_id: int) -> None:
        elapsed = 0.0
        if policy.pin_after_send and sent_message is not None:
            try:
                pin_delay = max(0.0, float(policy.pin_delay_seconds or 0))
                if pin_delay > 0:
                    await asyncio.sleep(pin_delay)
                    elapsed += pin_delay
                message_id = int(getattr(sent_message, "id", 0) or 0)
                if message_id <= 0:
                    raise ValueError("MESSAGE_ID_INVALID")
                await client(
                    functions.messages.UpdatePinnedMessageRequest(
                        peer=entity,
                        id=message_id,
                        silent=True,
                        pm_oneside=True,
                    )
                )
                self.repository.add_send_log(
                    task_id=task_id,
                    account_id=account_id,
                    recipient_id=recipient_id,
                    action="pin",
                    status="success",
                    message=f"发送后 {int(policy.pin_delay_seconds or 0)} 秒已自动置顶",
                )
            except Exception as exc:  # noqa: BLE001
                raw = self.collection_manager._short_error(exc)
                _, friendly, _ = self._classify_send_error(exc)
                self.repository.add_send_log(
                    task_id=task_id,
                    account_id=account_id,
                    recipient_id=recipient_id,
                    action="pin",
                    status="failed",
                    message=f"置顶失败｜{friendly}",
                    raw_error=raw,
                )

        if not policy.delete_dialog_after_send:
            return

        message_ids = self._extract_message_ids(sent_messages)
        try:
            delete_delay = max(0.0, float(policy.delete_dialog_delay_seconds or 0))
            remaining = max(0.0, delete_delay - elapsed)
            if remaining > 0:
                await asyncio.sleep(remaining)
            if message_ids:
                await client.delete_messages(entity, message_ids, revoke=False)
            await client(
                functions.messages.DeleteHistoryRequest(
                    peer=entity,
                    max_id=0,
                    just_clear=True,
                )
            )
            self.repository.add_send_log(
                task_id=task_id,
                account_id=account_id,
                recipient_id=recipient_id,
                action="delete",
                status="success",
                message=f"发送后 {int(policy.delete_dialog_delay_seconds or 0)} 秒已自动清空对话框",
            )
        except Exception as exc:  # noqa: BLE001
            raw = self.collection_manager._short_error(exc)
            _, friendly, _ = self._classify_send_error(exc)
            self.repository.add_send_log(
                task_id=task_id,
                account_id=account_id,
                recipient_id=recipient_id,
                action="delete",
                status="failed",
                message=f"删除对话框失败｜{friendly}",
                raw_error=raw,
            )

    async def _send_text_message(self, client, entity, text: str, policy: DMTaskPolicy, *, parse_mode: str | None = None):
        return await self.transport.send_text_message(client, entity, text, policy, parse_mode=parse_mode)

    async def _dispatch_single_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        return await self.transport.dispatch_single_payload(client, entity, payload, content_type, policy)

    async def _dispatch_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        return await self.transport.dispatch_payload(client, entity, payload, content_type, policy)

    async def _dispatch_reply_payload(self, client, entity, payload: dict, policy: DMTaskPolicy):
        return await self.transport.dispatch_reply_payload(client, entity, payload, policy)

    async def _resolve_channel_post_link(self, client, link: str):
        return await self.transport.resolve_channel_post_link(client, link)

    @staticmethod
    def _dispatch_progress_message(content_type: str, payload: dict, policy: DMTaskPolicy) -> str:
        del payload
        return TgMatrixDmTransport.dispatch_progress_message(content_type, policy)

    async def _send_with_recovery(self, client, entity, sender, *, expected_text: str = "", expect_media: bool = False):
        return await self.transport.send_with_recovery(
            client,
            entity,
            sender,
            expected_text=expected_text,
            expect_media=expect_media,
        )

    def _classify_send_error(self, exc: Exception) -> tuple[str, str, bool]:
        return self.transport.classify_send_error(exc)

    async def _resolve_target_entity(self, client, target: str):
        return await self.transport.resolve_target_entity(client, target)

    async def _run_entity_cleanup(self, cleanup) -> None:
        await self.transport.run_entity_cleanup(cleanup)

    async def _apply_post_send_actions(self, client, entity, sent_message, sent_messages, policy: DMTaskPolicy, *, task_id: int, account_id: int, recipient_id: int) -> None:
        await self.transport.apply_post_send_actions(
            client,
            entity,
            sent_message,
            sent_messages,
            policy,
            task_id=task_id,
            account_id=account_id,
            recipient_id=recipient_id,
        )

    async def _emit_progress(self, task_id: int) -> None:
        if self.on_progress:
            await self.on_progress(task_id)

    async def _emit_complete(self, task_id: int) -> None:
        if self.on_complete:
            await self.on_complete(task_id)

    @staticmethod
    def _action_success_message(content_type: str) -> str:
        return {
            "text": "文本发送成功",
            "reply": "回复模式发送成功",
            "post": "PostBot 内联文案发送成功",
            "media": "媒体发送成功",
            "forward": "频道帖子转发成功",
        }.get(str(content_type or "text"), "发送成功")

    @staticmethod
    def _append_limit_progress(message: str, current_count: int, policy_or_limit) -> str:
        if isinstance(policy_or_limit, DMTaskPolicy):
            limit = int(policy_or_limit.per_account_success_limit or 0)
        else:
            limit = int(policy_or_limit or 0)
        if limit <= 0:
            return message
        current = max(0, int(current_count or 0))
        return f"{message}[{current}/{limit}]"

    async def _latest_incoming_message_id(self, client, entity) -> int:
        last_id = 0
        async for message in client.iter_messages(entity, limit=10):
            if message.out:
                continue
            last_id = max(last_id, int(getattr(message, "id", 0) or 0))
        return last_id

    async def _wait_for_reply_message(self, client, entity, *, after_message_id: int, timeout_seconds: float):
        deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
        while asyncio.get_running_loop().time() < deadline:
            async for message in client.iter_messages(entity, limit=10):
                if message.out:
                    continue
                if int(getattr(message, "id", 0) or 0) <= after_message_id:
                    continue
                return message
            await asyncio.sleep(1)
        raise RuntimeError("等待对方回复超时")

    @staticmethod
    def _pick_reply_text(incoming_message, default_reply: str, keyword_rules: list[dict]) -> tuple[str, list[str]]:
        incoming_text = str(
            getattr(incoming_message, "raw_text", None)
            or getattr(incoming_message, "message", None)
            or ""
        ).strip().lower()
        matched_keywords: list[str] = []
        for row in keyword_rules or []:
            keywords = [str(item or "").strip().lower() for item in (row.get("keywords") or []) if str(item or "").strip()]
            if not keywords:
                continue
            if any(keyword in incoming_text for keyword in keywords):
                matched_keywords = keywords
                return str(row.get("reply") or "").strip(), matched_keywords
        return default_reply, matched_keywords

    @staticmethod
    def _failure_backoff_delay(error_code: str, policy: DMTaskPolicy) -> float:
        if error_code in {"too_many_requests", "peer_flood", "mutual_limit", "frozen", "flood_wait_exhausted"}:
            return max(2.0, min(6.0, float(policy.delay_window.min_seconds or 0)))
        if error_code in {"account_disconnected", "session_invalid"}:
            return 0.0
        return 0.35

    @staticmethod
    def _detect_dm_media_kind(path: Path, media_kind: str | None) -> str:
        normalized = str(media_kind or "document").lower()
        if normalized in {"photo", "video"}:
            return normalized
        suffix = path.suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
            return "photo"
        if suffix in {".mp4", ".mov", ".mkv", ".webm"}:
            return "video"
        return "document"

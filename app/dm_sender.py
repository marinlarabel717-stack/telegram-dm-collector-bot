from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from telethon.errors import FloodWaitError, RPCError

from .collector import CollectionManager
from .database import Database
from .dm_links import parse_channel_post_link
from .dm_logging import compose_log
from .dm_postbot import fetch_postbot_inline_result
from .dm_policy import DMTaskPolicy, DelayWindow, RetryPolicy
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
        if not accounts:
            reason_text = "；".join(unavailable_reasons[:3]) if unavailable_reasons else "没有可用账号"
            summary = self._build_task_stop_summary(task_id, fallback_total_accounts=len(task_accounts), fallback_pending=len(recipients))
            self.repository.mark_dm_task_status(task_id, "stopped", last_error=f"没有可用账号，任务已停止｜{summary}｜{reason_text}")
            self.repository.sync_dm_task_metrics(task_id)
            await self._emit_complete(task_id)
            return

        queue: asyncio.Queue = asyncio.Queue()
        for row in recipients:
            queue.put_nowait(row)

        account_queue: asyncio.Queue = asyncio.Queue()
        for row in accounts:
            account_queue.put_nowait(row)

        worker_count = min(int(task["worker_count"] or 1), len(accounts), queue.qsize())
        self.repository.mark_dm_task_status(task_id, "running")
        await self._emit_progress(task_id)
        logger.info(compose_log(f"启动｜目标={queue.qsize()}｜账号={len(accounts)}｜并发={worker_count}", task_id=task_id))

        workers = [
            asyncio.create_task(self._account_slot_worker(task_id, queue, account_queue, active_policy, slot_index=index + 1))
            for index in range(worker_count)
        ]
        try:
            await asyncio.gather(*workers, return_exceptions=False)
        except asyncio.CancelledError:
            current_task = self.repository.get_dm_task(task_id)
            stop_reason = str((current_task["last_error"] if current_task else "") or "").strip()
            if stop_reason == "管理员手动停止任务":
                final_reason = stop_reason
            else:
                final_reason = stop_reason or "任务运行被中断，可能是机器人重启或进程被打断"
            self.repository.request_dm_task_stop(task_id)
            self.repository.mark_dm_task_status(task_id, "stopped", last_error=final_reason)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(compose_log(f"发送器异常｜{exc}", task_id=task_id))
            self.repository.mark_dm_task_status(task_id, "error", last_error=self.collection_manager._short_error(exc))
        else:
            pending = self.repository.count_dm_pending_recipients(task_id)
            current_task = self.repository.get_dm_task(task_id)
            if self.repository.should_stop_dm_task(task_id):
                stop_reason = str((current_task["last_error"] if current_task else "") or "").strip() or "管理员手动停止任务"
                self.repository.mark_dm_task_status(task_id, "stopped", last_error=stop_reason)
            elif pending > 0:
                summary = self._build_task_stop_summary(task_id, fallback_total_accounts=len(task_accounts), fallback_pending=pending)
                self.repository.mark_dm_task_status(task_id, "stopped", last_error=f"可用账号已用尽，任务已停止｜{summary}")
            else:
                self.repository.mark_dm_task_status(task_id, "completed")
        self.repository.sync_dm_task_metrics(task_id)
        export = self.repository.export_task_results(task_id, self.collection_manager.settings.export_dir)
        self.repository.set_dm_task_result_file(task_id, str(export.success_txt))
        await self._emit_complete(task_id)

    def _account_can_send(self, account_row) -> tuple[bool, str]:
        runtime_status = str(account_row["account_runtime_status"] or "")
        restriction_status = str(account_row["restriction_status"] or "unknown")
        restriction_reason = str(account_row["restriction_reason"] or "").strip()
        account_last_error = str(account_row["account_last_error"] or "").strip()
        if runtime_status != "active":
            return False, account_last_error or f"账号状态={runtime_status}"
        if restriction_status in {"session_invalid", "frozen"}:
            return False, restriction_reason or f"限制状态={restriction_status}"
        return True, ""

    async def _account_slot_worker(self, task_id: int, queue: asyncio.Queue, account_queue: asyncio.Queue, policy: DMTaskPolicy, *, slot_index: int) -> None:
        while not queue.empty() and not self.repository.should_stop_dm_task(task_id):
            try:
                account_row = account_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            account_id = int(account_row["account_id"])
            logger.info(compose_log(f"槽位接管账号｜slot={slot_index}", task_id=task_id, account_id=account_id))
            try:
                await self._account_worker(task_id, queue, account_row, policy)
            finally:
                account_queue.task_done()

    async def _account_worker(self, task_id: int, queue: asyncio.Queue, account_row, policy: DMTaskPolicy) -> None:
        account_id = int(account_row["account_id"])
        session_file = Path(account_row["session_file"])
        client = None
        success_count = int(account_row["sent_success_count"] or 0)
        frequent_errors = int(account_row["frequent_error_count"] or 0)
        too_many_requests_hits = 0
        task = self.repository.get_dm_task(task_id)
        payload = json.loads(str(task["payload_json"] or "{}")) if task else {}
        content_type = str((task["content_type"] if task else None) or payload.get("content_type") or "text")
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
                    if not policy.auto_switch_account:
                        self.repository.request_dm_task_stop(task_id)
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
                try:
                    entity = await client.get_input_entity(target)
                    sent_message = await self._dispatch_payload(client, entity, payload, content_type, policy)
                    await self._apply_post_send_actions(client, entity, sent_message, policy, task_id=task_id, account_id=account_id, recipient_id=recipient_id)
                    success_count += 1
                    post_attempt_delay = policy.delay_window.next_delay()
                    self.repository.mark_dm_recipient_result(task_id, recipient_id, account_id=account_id, status="success")
                    self.repository.update_dm_task_account(task_id, account_id, success_delta=1, last_error=None)
                    self.repository.add_send_log(task_id=task_id, account_id=account_id, recipient_id=recipient_id, action="send", status="success", message=self._action_success_message(content_type))
                    logger.info(compose_log("发送成功", task_id=task_id, account_id=account_id, recipient=target))
                except FloodWaitError as exc:
                    wait_seconds = int(getattr(exc, "seconds", 0) or 0)
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
                    error_code, error_message, frequent_hit = self._classify_send_error(exc)
                    post_attempt_delay = self._failure_backoff_delay(error_code, policy)
                    if frequent_hit and error_code != "too_many_requests":
                        frequent_errors += 1
                    if error_code == "too_many_requests":
                        too_many_requests_hits += 1
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
                        last_error=error_message,
                    )
                    log_message = error_message
                    if error_code == "too_many_requests":
                        log_message = self._append_limit_progress(
                            log_message,
                            too_many_requests_hits,
                            policy.retry_policy.stop_account_after_too_many_requests,
                        )
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
                        self.repository.update_dm_task_account(
                            task_id,
                            account_id,
                            status="stopped",
                            last_error=f"请求过于频繁次数达到阈值（{too_many_requests_hits}/{policy.retry_policy.stop_account_after_too_many_requests}）",
                        )
                        if not policy.auto_switch_account:
                            self.repository.request_dm_task_stop(task_id)
                        break
                    if policy.should_stop_account_for_frequent(frequent_errors):
                        logger.warning(compose_log(f"达到频繁阈值，停止该账号｜count={frequent_errors}", task_id=task_id, account_id=account_id))
                        if not policy.auto_switch_account:
                            self.repository.request_dm_task_stop(task_id)
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
            return await client.send_message(entity, content, parse_mode=parse_mode)
        return await client.send_message(entity, content)

    async def _dispatch_single_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        if content_type == "media":
            media_path = Path(str(payload.get("media_path") or "")).expanduser()
            if not media_path.exists():
                raise FileNotFoundError(f"媒体文件不存在: {media_path}")
            caption = str(payload.get("caption") or "").strip() or None
            media_kind = self._detect_dm_media_kind(media_path, str(payload.get("media_kind") or "document"))
            return await client.send_file(
                entity,
                file=str(media_path),
                caption=caption,
                force_document=(media_kind == "document"),
                supports_streaming=(media_kind == "video"),
            )
        if content_type == "post":
            post_code = str(payload.get("body") or payload.get("post_code") or payload.get("text") or "").strip()
            _, inline_result = await fetch_postbot_inline_result(client, post_code)
            return self._normalize_sent_message(await inline_result.click(entity))
        if content_type == "forward":
            forward_link = str(payload.get("forward_link") or "").strip()
            if forward_link:
                source_peer, source_message_id = await self._resolve_channel_post_link(client, forward_link)
                return self._normalize_sent_message(await client.forward_messages(entity, messages=source_message_id, from_peer=source_peer))
            source_chat_id = payload.get("source_chat_id")
            source_message_id = payload.get("source_message_id")
            if not source_chat_id or not source_message_id:
                raise ValueError("频道帖子链接不能为空")
            from_peer = await client.get_input_entity(int(source_chat_id))
            return self._normalize_sent_message(await client.forward_messages(entity, messages=int(source_message_id), from_peer=from_peer))
        main_text = str(payload.get("body") or payload.get("text") or "").strip()
        return await self._send_text_message(client, entity, main_text, policy)

    async def _dispatch_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        mode = str(payload.get("mode") or "single")
        if mode != "three_stage":
            return await self._dispatch_single_payload(client, entity, payload, content_type, policy)

        greeting = str(payload.get("greeting") or "").strip()
        closing = str(payload.get("closing") or "").strip()
        if greeting:
            await self._send_text_message(client, entity, greeting, policy)
            await asyncio.sleep(max(0.0, float(policy.stage1_delay_seconds or 0)))

        main_sent = await self._dispatch_single_payload(client, entity, payload, content_type, policy)

        if closing:
            await asyncio.sleep(max(0.0, float(policy.stage2_delay_seconds or 0)))
            await self._send_text_message(client, entity, closing, policy)
        return main_sent

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
        if "username not occupied" in lowered or "cannot find" in lowered or "no user has" in lowered or "entity not found" in lowered:
            return "user_not_found", "用户不存在或无法解析", False
        if "bot method invalid" in lowered or "bot invalid" in lowered:
            return "bot_target", "目标不是可私信的普通用户", False
        if "user is blocked" in lowered or "you blocked" in lowered:
            return "blocked", "对方已拉黑或账号关系异常", False
        if "too many requests" in lowered or "retry after" in lowered:
            return "too_many_requests", "请求过于频繁", True
        if "user is restricted" in lowered or "mutual" in lowered:
            return "mutual_limit", "账号存在双向或发送限制", True
        if "frozen" in lowered:
            return "frozen", "账号疑似冻结", True
        return "send_failed", short or exc.__class__.__name__, False

    def _is_account_terminal_error(self, error_code: str) -> bool:
        return error_code in {"account_disconnected", "session_invalid", "frozen"}

    def _handle_terminal_account_error(
        self,
        task_id: int,
        account_id: int,
        *,
        error_code: str,
        error_message: str,
        raw_error: str,
        auto_switch_account: bool,
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
        if self._task_has_no_other_usable_accounts(task_id, exclude_account_id=account_id):
            summary = self._build_task_stop_summary(task_id)
            reason = f"{error_message}，且没有其他可用账号，任务已停止｜{summary}"
            current_task = self.repository.get_dm_task(task_id)
            current_status = str((current_task["status"] if current_task else "") or "")
            self.repository.mark_dm_task_status(
                task_id,
                current_status if current_status in {"queued", "running", "paused"} else "running",
                last_error=reason,
            )
            self.repository.request_dm_task_stop(task_id)
            return reason
        return f"{error_message}，已停止当前账号并切到其他账号"

    def _build_task_stop_summary(self, task_id: int, *, fallback_total_accounts: int | None = None, fallback_pending: int | None = None) -> str:
        account_rows = self.repository.list_dm_task_accounts(task_id)
        total_accounts = fallback_total_accounts if fallback_total_accounts is not None else len(account_rows)
        stopped_accounts = sum(1 for row in account_rows if str(row["status"] or "") in {"stopped", "error", "completed"})
        pending = fallback_pending if fallback_pending is not None else self.repository.count_dm_pending_recipients(task_id)
        return f"已停账号 {stopped_accounts}/{total_accounts} 个，剩余目标 {pending} 个"

    def _task_has_no_other_usable_accounts(self, task_id: int, *, exclude_account_id: int) -> bool:
        for row in self.repository.list_dm_task_accounts(task_id):
            if int(row["account_id"]) == exclude_account_id:
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

    async def _apply_post_send_actions(self, client, entity, sent_message, policy: DMTaskPolicy, *, task_id: int, account_id: int, recipient_id: int) -> None:
        if not policy.pin_after_send or sent_message is None:
            return
        try:
            await asyncio.sleep(max(0.0, float(policy.pin_delay_seconds or 0)))
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

    @staticmethod
    def _normalize_sent_message(result):
        if isinstance(result, list):
            return result[-1] if result else None
        return result

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

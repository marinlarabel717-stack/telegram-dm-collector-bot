from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from telethon.tl import functions, types

from .dm_links import parse_channel_post_link
from .dm_policy import DMTaskPolicy
from .dm_postbot import fetch_postbot_inline_result

logger = logging.getLogger(__name__)


class TgMatrixDmTransport:
    def __init__(self, owner) -> None:
        self.owner = owner
        self.repository = owner.repository
        self.collection_manager = owner.collection_manager

    async def send_text_message(self, client, entity, text: str, policy: DMTaskPolicy, *, parse_mode: str | None = None):
        content = str(text or "").strip()
        if not content:
            return None
        if policy.typing_simulation:
            try:
                async with client.action(entity, "typing"):
                    await asyncio.sleep(min(2.5, max(0.5, policy.delay_window.next_delay() / 3)))
            except Exception:
                logger.debug("typing simulation failed, fallback to direct send", exc_info=True)
        if parse_mode:
            return await self.send_with_recovery(
                client,
                entity,
                lambda: client.send_message(entity, content, parse_mode=parse_mode),
                expected_text=content,
            )
        return await self.send_with_recovery(
            client,
            entity,
            lambda: client.send_message(entity, content),
            expected_text=content,
        )

    async def dispatch_single_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        if content_type == "media":
            media_path = Path(str(payload.get("media_path") or "")).expanduser()
            if not media_path.exists():
                raise FileNotFoundError(f"media file not found: {media_path}")
            caption = str(payload.get("caption") or "").strip() or None
            media_kind = self.owner._detect_dm_media_kind(media_path, str(payload.get("media_kind") or "document"))
            return await self.send_with_recovery(
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
            return await self.send_with_recovery(
                client,
                entity,
                lambda: inline_result.click(entity),
            )
        if content_type == "forward":
            forward_link = str(payload.get("forward_link") or "").strip()
            if forward_link:
                source_peer, source_message_id = await self.resolve_channel_post_link(client, forward_link)
                return await self.send_with_recovery(
                    client,
                    entity,
                    lambda: client.forward_messages(entity, messages=source_message_id, from_peer=source_peer, drop_author=False),
                    expect_media=True,
                )
            source_chat_id = payload.get("source_chat_id")
            source_message_id = payload.get("source_message_id")
            if not source_chat_id or not source_message_id:
                raise ValueError("channel post link cannot be empty")
            from_peer = await client.get_input_entity(int(source_chat_id))
            return await self.send_with_recovery(
                client,
                entity,
                lambda: client.forward_messages(entity, messages=int(source_message_id), from_peer=from_peer, drop_author=False),
                expect_media=True,
            )
        main_text = str(payload.get("body") or payload.get("text") or "").strip()
        return await self.send_text_message(client, entity, main_text, policy)

    async def dispatch_payload(self, client, entity, payload: dict, content_type: str, policy: DMTaskPolicy):
        if content_type == "reply":
            return await self.dispatch_reply_payload(client, entity, payload, policy)
        mode = str(payload.get("mode") or "single")
        if mode != "three_stage":
            main_result = await self.dispatch_single_payload(client, entity, payload, content_type, policy)
            return self.owner._normalize_sent_message(main_result), self.owner._collect_sent_messages(main_result)

        greeting = str(payload.get("greeting") or "").strip()
        closing = str(payload.get("closing") or "").strip()
        all_messages = []
        if greeting:
            greeting_result = await self.send_text_message(client, entity, greeting, policy)
            all_messages.extend(self.owner._collect_sent_messages(greeting_result))
            await asyncio.sleep(max(0.0, float(policy.stage1_delay_seconds or 0)))

        main_result = await self.dispatch_single_payload(client, entity, payload, content_type, policy)
        main_sent = self.owner._normalize_sent_message(main_result)
        all_messages.extend(self.owner._collect_sent_messages(main_result))

        if closing:
            await asyncio.sleep(max(0.0, float(policy.stage2_delay_seconds or 0)))
            closing_result = await self.send_text_message(client, entity, closing, policy)
            all_messages.extend(self.owner._collect_sent_messages(closing_result))
        return main_sent, all_messages

    async def dispatch_reply_payload(self, client, entity, payload: dict, policy: DMTaskPolicy):
        greeting = str(payload.get("greeting") or payload.get("text") or "").strip()
        default_reply = str(payload.get("body") or payload.get("reply_text") or "").strip()
        closing = str(payload.get("closing") or "").strip()
        keyword_rules = payload.get("reply_keyword_rules") or []

        all_messages = []
        baseline_incoming_id = await self.owner._latest_incoming_message_id(client, entity)
        if greeting:
            greeting_result = await self.send_text_message(client, entity, greeting, policy)
            all_messages.extend(self.owner._collect_sent_messages(greeting_result))

        incoming_message = await self.owner._wait_for_reply_message(
            client,
            entity,
            after_message_id=baseline_incoming_id,
            timeout_seconds=float(policy.reply_wait_timeout_seconds or 300),
        )
        reply_text, _ = self.owner._pick_reply_text(incoming_message, default_reply, keyword_rules)
        if not reply_text:
            raise RuntimeError("peer replied, but no matching reply text is configured")

        reply_delay = max(0.0, float(policy.reply_delay_seconds or 0))
        if reply_delay > 0:
            await asyncio.sleep(reply_delay)
        reply_result = await self.send_text_message(client, entity, reply_text, policy)
        all_messages.extend(self.owner._collect_sent_messages(reply_result))
        main_sent = self.owner._normalize_sent_message(reply_result)

        if closing:
            await asyncio.sleep(max(0.0, float(policy.stage2_delay_seconds or 0)))
            closing_result = await self.send_text_message(client, entity, closing, policy)
            all_messages.extend(self.owner._collect_sent_messages(closing_result))

        return main_sent, all_messages

    async def resolve_channel_post_link(self, client, link: str):
        parsed = parse_channel_post_link(link)
        if not parsed:
            raise ValueError("channel post link format invalid")
        if parsed["kind"] == "public":
            source_peer = await client.get_input_entity(f"@{parsed['username']}")
        else:
            source_peer = await client.get_input_entity(int(f"-100{parsed['channel_id']}"))
        return source_peer, int(parsed["message_id"])

    @staticmethod
    def dispatch_progress_message(content_type: str, policy: DMTaskPolicy) -> str:
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

    def classify_send_error(self, exc: Exception) -> tuple[str, str, bool]:
        short = self.collection_manager._short_error(exc)
        lowered = short.lower()
        if "cannot send requests while disconnected" in lowered or ("disconnected" in lowered and "request" in lowered):
            return "account_disconnected", "账号掉线了，连接已经断开，没法继续发请求", False
        if "the key is not registered in the system" in lowered or "invokewit" in lowered:
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
        if "too many requests" in lowered or "retry after" in lowered:
            return "too_many_requests", "请求过于频繁", True
        if "user is restricted" in lowered or "mutual" in lowered:
            return "mutual_limit", "账号存在双向或发送限制", True
        if "frozen" in lowered:
            return "frozen", "账号疑似冻结", True
        return "send_failed", short or exc.__class__.__name__, False

    async def resolve_target_entity(self, client, target: str):
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

    async def run_entity_cleanup(self, cleanup) -> None:
        if cleanup is None:
            return
        try:
            await cleanup()
        except Exception:
            logger.debug("cleanup temporary contact failed", exc_info=True)

    async def apply_post_send_actions(self, client, entity, sent_message, sent_messages, policy: DMTaskPolicy, *, task_id: int, account_id: int, recipient_id: int) -> None:
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
                _, friendly, _ = self.classify_send_error(exc)
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

        message_ids = self.owner._extract_message_ids(sent_messages)
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
            _, friendly, _ = self.classify_send_error(exc)
            self.repository.add_send_log(
                task_id=task_id,
                account_id=account_id,
                recipient_id=recipient_id,
                action="delete",
                status="failed",
                message=f"删除对话框失败｜{friendly}",
                raw_error=raw,
            )

    async def send_with_recovery(
        self,
        client,
        entity,
        sender: Callable[[], Awaitable[Any]],
        *,
        expected_text: str = "",
        expect_media: bool = False,
    ):
        started_at_ms = int(time.time() * 1000)
        try:
            return await sender()
        except Exception:
            recovered = await self.find_actually_sent_result(
                client,
                entity,
                started_at_ms=started_at_ms,
                expected_text=expected_text,
                expect_media=expect_media,
            )
            if recovered is not None:
                return recovered
            raise

    async def find_actually_sent_result(self, client, entity, *, started_at_ms: int, expected_text: str = "", expect_media: bool = False):
        normalized_expected = self.normalize_compare_text(expected_text)
        fallback_message = None
        for attempt in range(4):
            messages = await client.get_messages(entity, limit=8)
            if messages:
                for message in messages:
                    if not bool(getattr(message, "out", False)):
                        continue
                    sent_at_ms = self.read_message_date_ms(message)
                    if isinstance(sent_at_ms, int) and sent_at_ms + 20000 < started_at_ms:
                        continue
                    if fallback_message is None:
                        fallback_message = message
                    actual_text = self.normalize_compare_text(self.read_message_text(message))
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
    def normalize_compare_text(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def read_message_text(message) -> str:
        for key in ("raw_text", "rawText", "message", "text"):
            value = getattr(message, key, None)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    @staticmethod
    def read_message_date_ms(message) -> int | None:
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

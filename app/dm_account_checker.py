from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient

from .collector import CollectionManager
from .dm_logging import compose_log
from .dm_repository import DmRepository

logger = logging.getLogger(__name__)
BEIJING_TZ = timezone(timedelta(hours=8))
ACCOUNT_CHECK_RECONNECT_RETRIES = 2
SPAMBOT_REPLY_POLL_INTERVAL_SECONDS = 0.5
SPAMBOT_REPLY_MAX_POLLS = 16


@dataclass(slots=True)
class AccountStatusCheckResult:
    login_status: str
    restriction_status: str
    summary: str
    spambot_reply: str | None
    last_error: str | None


class DmAccountChecker:
    def __init__(self, repository: DmRepository, collection_manager: CollectionManager):
        self.repository = repository
        self.collection_manager = collection_manager

    async def check_account_status(self, account_row) -> AccountStatusCheckResult:
        account_id = int(account_row["id"])
        client: TelegramClient | None = None
        try:
            session_file = Path(account_row["session_file"])
            connect_attempt = 0
            while True:
                try:
                    if client is None:
                        client = await self.collection_manager.connect_client(session_file, account_row=account_row)
                    if not await client.is_user_authorized():
                        summary = self._humanize_session_issue("unauthorized", "session 未登录")
                        self.collection_manager.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                        self.repository.update_account_restriction(
                            account_id,
                            restriction_status="session_invalid",
                            restriction_reason=summary,
                            raw_reply=None,
                        )
                        logger.warning(compose_log(f"状态检查失败｜{summary}", account_id=account_id))
                        return AccountStatusCheckResult(
                            login_status="unauthorized",
                            restriction_status="session_invalid",
                            summary=summary,
                            spambot_reply=None,
                            last_error="session 未登录",
                        )
                    me = await client.get_me()
                    break
                except Exception as exc:  # noqa: BLE001
                    if connect_attempt >= ACCOUNT_CHECK_RECONNECT_RETRIES or not self.collection_manager._is_disconnect_error(exc):
                        raise
                    connect_attempt += 1
                    logger.warning(compose_log(f"状态检查连接中断，准备重连｜第{connect_attempt}次｜{self.collection_manager._short_error(exc)}", account_id=account_id))
                    client = await self.collection_manager._reconnect_client(client, session_file, account_row=account_row)

            self.collection_manager.db.update_account_status(
                account_id,
                status="active",
                last_error=None,
                tg_user_id=getattr(me, "id", None),
                phone=getattr(me, "phone", None),
                username=getattr(me, "username", None),
                display_name=getattr(me, "first_name", "") and me.first_name or getattr(me, "username", None),
            )
        except Exception as exc:  # noqa: BLE001
            last_error = self.collection_manager._short_error(exc)
            summary = self._humanize_session_issue("error", last_error)
            self.collection_manager.db.update_account_status(account_id, status="error", last_error=last_error)
            self.repository.update_account_restriction(
                account_id,
                restriction_status="session_invalid",
                restriction_reason=summary,
                raw_reply=None,
            )
            logger.warning(compose_log(f"状态检查失败｜{summary}", account_id=account_id))
            return AccountStatusCheckResult(
                login_status="error",
                restriction_status="session_invalid",
                summary=summary,
                spambot_reply=None,
                last_error=last_error,
            )

        self.repository.update_account_restriction(account_id, restriction_status="checking", restriction_reason="正在检测 SpamBot 状态")
        logger.info(compose_log("开始检测 SpamBot 状态", account_id=account_id))
        try:
            reply_text = await self._fetch_spambot_reply(account_row, client=client)
            restriction_status, summary = self._parse_spambot_reply(reply_text)
            self.repository.update_account_restriction(
                account_id,
                restriction_status=restriction_status,
                restriction_reason=summary,
                raw_reply=reply_text,
            )
            logger.info(compose_log(f"状态检查完成｜{summary}", account_id=account_id))
            return AccountStatusCheckResult(
                login_status="active",
                restriction_status=restriction_status,
                summary=summary,
                spambot_reply=reply_text,
                last_error=None,
            )
        except Exception as exc:  # noqa: BLE001
            summary = f"SpamBot 检测失败：{self.collection_manager._short_error(exc)}"
            self.repository.update_account_restriction(
                account_id,
                restriction_status="unknown",
                restriction_reason=summary,
                raw_reply=None,
            )
            logger.exception(compose_log(f"状态检查异常｜{summary}", account_id=account_id))
            return AccountStatusCheckResult(
                login_status="active",
                restriction_status="unknown",
                summary=summary,
                spambot_reply=None,
                last_error=str(exc),
            )
        finally:
            if client is not None:
                await client.disconnect()

    async def _fetch_spambot_reply(self, account_row, *, client: TelegramClient | None = None) -> str:
        owns_client = client is None
        try:
            if client is None:
                client = await self.collection_manager.connect_client(Path(account_row["session_file"]), account_row=account_row)
            if not await client.is_user_authorized():
                raise RuntimeError("session 未登录")
            entity = await client.get_input_entity("SpamBot")
            last_incoming_id = 0
            async for message in client.iter_messages(entity, limit=10):
                if message.out:
                    continue
                last_incoming_id = max(last_incoming_id, int(getattr(message, "id", 0) or 0))
            await client.send_message(entity, "/start")

            for _ in range(SPAMBOT_REPLY_MAX_POLLS):
                await asyncio.sleep(SPAMBOT_REPLY_POLL_INTERVAL_SECONDS)
                async for message in client.iter_messages(entity, limit=10):
                    if message.out:
                        continue
                    if int(getattr(message, "id", 0) or 0) <= last_incoming_id:
                        continue
                    text = (getattr(message, "raw_text", None) or getattr(message, "message", None) or "").strip()
                    if text:
                        return text
            raise RuntimeError("未收到 SpamBot 回复")
        finally:
            if owns_client and client is not None:
                await client.disconnect()

    @classmethod
    def _parse_spambot_reply(cls, reply_text: str) -> tuple[str, str]:
        text = (reply_text or "").strip()
        lowered = text.lower()
        restriction_until = cls._extract_restriction_until(text)

        if any(keyword in lowered for keyword in ["good news", "no limits are currently applied", "you can freely send messages", "free as a bird", "目前没有任何限制", "目前没有限制", "没有限制"]):
            return "unrestricted", "无限制"
        if "frozen" in lowered or "冻结" in text:
            return "frozen", "冻结"
        if any(keyword in lowered for keyword in [
            "some users in some regions",
            "some countries",
            "from some regions",
            "harsh response from our anti-spam systems",
            "submit a complaint to our moderators",
            "subscribe to telegram premium",
            "premium_offer?ref=spambot",
            "地区限制",
            "地理位置限制",
        ]):
            return "geo_limited", "地理限制"
        if any(keyword in lowered for keyword in ["mutual contact", "mutual contacts", "people who are in your contacts", "双向联系人", "双向"]):
            if restriction_until is not None or any(keyword in lowered for keyword in ["will be lifted", "temporarily", "temporary", "until", "暂时", "临时"]):
                return "temp_mutual", cls._format_temp_mutual_summary(restriction_until)
            return "permanent_mutual", "永久双向"
        if any(keyword in lowered for keyword in ["too many", "spam", "limited", "can only send", "can't send", "cannot send", "限制"]):
            if restriction_until is not None:
                return "temp_mutual", cls._format_temp_mutual_summary(restriction_until)
            return "permanent_mutual", "永久双向"
        return "unknown", "待人工确认"

    @staticmethod
    def _format_temp_mutual_summary(restriction_until: datetime | None) -> str:
        if restriction_until is None:
            return "临时双向"
        local_dt = restriction_until.astimezone(BEIJING_TZ)
        return f"临时双向（北京时间 {local_dt.strftime('%Y-%m-%d %H:%M:%S')} 解除）"

    @classmethod
    def _extract_restriction_until(cls, text: str) -> datetime | None:
        if not text:
            return None

        patterns: list[tuple[str, tuple[str, ...]]] = [
            (
                r"(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:utc)?)",
                ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M UTC", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"),
            ),
            (
                r"(\d{1,2}[./-]\d{1,2}[./-]\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:utc)?)",
                ("%d-%m-%Y %H:%M:%S UTC", "%d-%m-%Y %H:%M UTC", "%d.%m.%Y %H:%M:%S UTC", "%d.%m.%Y %H:%M UTC", "%d/%m/%Y %H:%M:%S UTC", "%d/%m/%Y %H:%M UTC", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"),
            ),
            (
                r"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2},\s*\d{4},?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:utc)?)",
                ("%B %d, %Y %H:%M:%S UTC", "%B %d, %Y %H:%M UTC", "%b %d, %Y %H:%M:%S UTC", "%b %d, %Y %H:%M UTC", "%B %d, %Y, %H:%M:%S UTC", "%B %d, %Y, %H:%M UTC", "%b %d, %Y, %H:%M:%S UTC", "%b %d, %Y, %H:%M UTC", "%B %d, %Y %H:%M:%S", "%B %d, %Y %H:%M", "%b %d, %Y %H:%M:%S", "%b %d, %Y %H:%M"),
            ),
            (
                r"(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{4},?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:utc)?)",
                ("%d %B %Y %H:%M:%S UTC", "%d %B %Y %H:%M UTC", "%d %b %Y %H:%M:%S UTC", "%d %b %Y %H:%M UTC", "%d %B %Y, %H:%M:%S UTC", "%d %B %Y, %H:%M UTC", "%d %b %Y, %H:%M:%S UTC", "%d %b %Y, %H:%M UTC", "%d %B %Y %H:%M:%S", "%d %B %Y %H:%M", "%d %b %Y %H:%M:%S", "%d %b %Y %H:%M"),
            ),
        ]

        normalized = text.replace("Sept", "Sep").replace("sept", "sep")
        for pattern, formats in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = re.sub(r"\s+", " ", match.group(1).strip())
            parsed = cls._parse_candidate_datetime(candidate, formats)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _parse_candidate_datetime(candidate: str, formats: tuple[str, ...]) -> datetime | None:
        value = candidate.strip().replace("，", ",").replace(" at ", " ")
        value = re.sub(r"\butc\b", "UTC", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value)
        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
            except ValueError:
                continue
            if "%Z" in fmt or value.endswith("UTC"):
                return dt.replace(tzinfo=timezone.utc)
            return dt.replace(tzinfo=timezone.utc)
        return None

    @staticmethod
    def _humanize_session_issue(status: str, last_error: str | None) -> str:
        raw = (last_error or "").strip()
        text = raw.lower()
        if status == "unauthorized" or any(key in text for key in ["user_deactivated", "banned", "revoked", "phone_number_banned"]):
            return "session 已失效或已封禁"
        if any(key in text for key in ["malformed", "not valid sqlite", "file is not a database", "缺少 sessions 表", "已损坏"]):
            return "session 已损坏"
        return raw or "账号状态异常"

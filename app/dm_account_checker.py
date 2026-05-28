from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from telethon import TelegramClient

from .collector import CollectionManager
from .dm_logging import compose_log
from .dm_repository import DmRepository

logger = logging.getLogger(__name__)


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
        verified = await self.collection_manager.verify_account(account_row)
        if verified.status != "active":
            summary = self._humanize_session_issue(verified.status, verified.last_error)
            self.repository.update_account_restriction(
                account_id,
                restriction_status="session_invalid",
                restriction_reason=summary,
                raw_reply=None,
            )
            logger.warning(compose_log(f"状态检查失败｜{summary}", account_id=account_id))
            return AccountStatusCheckResult(
                login_status=verified.status,
                restriction_status="session_invalid",
                summary=summary,
                spambot_reply=None,
                last_error=verified.last_error,
            )

        self.repository.update_account_restriction(account_id, restriction_status="checking", restriction_reason="正在检测 SpamBot 状态")
        logger.info(compose_log("开始检测 SpamBot 状态", account_id=account_id))
        try:
            reply_text = await self._fetch_spambot_reply(account_row)
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

    async def _fetch_spambot_reply(self, account_row) -> str:
        client: TelegramClient | None = None
        try:
            client = self.collection_manager._build_client(Path(account_row["session_file"]), account_row=account_row)
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("session 未登录")
            entity = await client.get_input_entity("SpamBot")
            last_incoming_id = 0
            async for message in client.iter_messages(entity, limit=10):
                if message.out:
                    continue
                last_incoming_id = max(last_incoming_id, int(getattr(message, "id", 0) or 0))
            await client.send_message(entity, "/start")

            for _ in range(15):
                await asyncio.sleep(1)
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
            if client is not None:
                await client.disconnect()

    @staticmethod
    def _parse_spambot_reply(reply_text: str) -> tuple[str, str]:
        text = (reply_text or "").strip()
        lowered = text.lower()

        if any(keyword in lowered for keyword in ["good news", "no limits are currently applied", "you can freely send messages", "free as a bird", "目前没有任何限制", "目前没有限制", "没有限制"]):
            return "unrestricted", "无限制"
        if "frozen" in lowered or "冻结" in text:
            return "frozen", "冻结"
        if any(keyword in lowered for keyword in ["some users in some regions", "some countries", "from some regions", "地区限制", "地理位置限制"]):
            return "geo_limited", "地理位置限制"
        if any(keyword in lowered for keyword in ["mutual contact", "mutual contacts", "people who are in your contacts", "双向联系人", "双向"]):
            if any(keyword in lowered for keyword in ["will be lifted", "temporarily", "temporary", "until", "暂时", "临时"]):
                return "temp_mutual", "临时双向"
            return "permanent_mutual", "永久双向"
        if any(keyword in lowered for keyword in ["too many", "spam", "limited", "can only send", "can't send", "cannot send", "限制"]):
            return "spam_limited", "官方限流"
        return "unknown", "待人工确认"

    @staticmethod
    def _humanize_session_issue(status: str, last_error: str | None) -> str:
        raw = (last_error or "").strip()
        text = raw.lower()
        if status == "unauthorized" or any(key in text for key in ["user_deactivated", "banned", "revoked", "phone_number_banned"]):
            return "session 已失效或已封禁"
        if any(key in text for key in ["malformed", "not valid sqlite", "file is not a database", "缺少 sessions 表", "已损坏"]):
            return "session 已损坏"
        return raw or "账号状态异常"

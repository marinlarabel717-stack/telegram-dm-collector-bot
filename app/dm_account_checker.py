from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient

from .collector import CollectionManager
from .dm_logging import compose_log
from .dm_repository import DmRepository

logger = logging.getLogger(__name__)
BEIJING_TZ = timezone(timedelta(hours=8))
ACCOUNT_CHECK_RECONNECT_RETRIES = 2
SPAMBOT_REPLY_TIMEOUT_SECONDS = 5


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
        try:
            return await self._check_account_status_via_tgmatrix(account_row)
        except FileNotFoundError as exc:
            logger.warning("TG-Matrix 检测脚本不存在，回退旧检查逻辑｜%s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("TG-Matrix 检测逻辑执行失败，回退旧检查逻辑｜account_id=%s｜%s", account_row["id"], exc)
        return await self._check_account_status_legacy(account_row)

    async def _check_account_status_via_tgmatrix(self, account_row) -> AccountStatusCheckResult:
        account_id = int(account_row["id"])
        session_file = Path(account_row["session_file"])
        raw_result = await self._run_tgmatrix_spambot_check(session_file, account_row=account_row)

        status = str(raw_result.get("status") or "unknown").strip().lower()
        reason = str(raw_result.get("reason") or "").strip()
        reply_text = str(raw_result.get("reply_text") or "").strip() or None
        tg_user_id = raw_result.get("user_id")
        phone = raw_result.get("phone")
        username = raw_result.get("username")
        display_name = (
            raw_result.get("first_name")
            or raw_result.get("username")
            or account_row["display_name"]
            or account_row["session_name"]
        )

        if status == "reply":
            restriction_status, summary = self._map_tgmatrix_reply_status(reply_text or "")
            if restriction_status == "session_invalid":
                login_status = "unauthorized"
                runtime_status = "unauthorized"
                last_error = summary
            else:
                login_status = "active"
                runtime_status = "active"
                last_error = summary if restriction_status == "frozen" else None
        elif status in {"alive", "ok"}:
            restriction_status = "unrestricted"
            summary = "无限制"
            login_status = "active"
            runtime_status = "active"
            last_error = None
        elif status in {"banned", "not_logged_in", "session_expired"}:
            restriction_status = "session_invalid"
            summary = self._map_tgmatrix_invalid_summary(status)
            login_status = "unauthorized"
            runtime_status = "unauthorized"
            last_error = reason or summary
        elif status == "frozen":
            restriction_status = "frozen"
            summary = self._format_tgmatrix_frozen_summary(raw_result)
            login_status = "active"
            runtime_status = "active"
            last_error = reason or summary
        elif status == "timeout":
            restriction_status = "unknown"
            summary = "连接 Telegram 超时，请稍后重试"
            login_status = "active"
            runtime_status = self._safe_runtime_status(account_row["status"])
            last_error = reason or summary
        else:
            restriction_status = "unknown"
            summary = self._humanize_tgmatrix_unknown(reason)
            login_status = "active"
            runtime_status = self._safe_runtime_status(account_row["status"])
            last_error = reason or summary

        self.collection_manager.db.update_account_status(
            account_id,
            status=runtime_status,
            last_error=last_error,
            tg_user_id=tg_user_id,
            phone=phone,
            username=username,
            display_name=display_name,
        )
        self.repository.update_account_restriction(
            account_id,
            restriction_status=restriction_status,
            restriction_reason=summary,
            raw_reply=reply_text,
        )

        if restriction_status in {"session_invalid", "frozen"}:
            logger.warning(compose_log(f"状态检查完成｜{summary}", account_id=account_id))
        else:
            logger.info(compose_log(f"状态检查完成｜{summary}", account_id=account_id))

        return AccountStatusCheckResult(
            login_status=login_status,
            restriction_status=restriction_status,
            summary=summary,
            spambot_reply=reply_text,
            last_error=last_error,
        )

    async def _check_account_status_legacy(self, account_row) -> AccountStatusCheckResult:
        account_id = int(account_row["id"])
        client: TelegramClient | None = None
        try:
            session_file = Path(account_row["session_file"])
            connect_attempt = 0
            while True:
                try:
                    if client is None:
                        client = await self.collection_manager.connect_client(session_file, account_row=account_row, receive_updates=True)
                    if not await client.is_user_authorized():
                        restriction_status, summary = self._classify_session_issue("unauthorized", "session 未登录")
                        self.collection_manager.db.update_account_status(account_id, status="unauthorized", last_error="session 未登录")
                        self.repository.update_account_restriction(
                            account_id,
                            restriction_status=restriction_status,
                            restriction_reason=summary,
                            raw_reply=None,
                        )
                        logger.warning(compose_log(f"状态检查失败｜{summary}", account_id=account_id))
                        return AccountStatusCheckResult(
                            login_status="unauthorized",
                            restriction_status=restriction_status,
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
                    client = await self.collection_manager._reconnect_client(client, session_file, account_row=account_row, receive_updates=True)

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
            restriction_status, summary = self._classify_session_issue("error", last_error)
            runtime_status = "error" if restriction_status == "session_invalid" else str(account_row["status"] or "active")
            if runtime_status not in {"active", "checking", "collecting"}:
                runtime_status = "active"
            self.collection_manager.db.update_account_status(account_id, status=runtime_status, last_error=last_error)
            self.repository.update_account_restriction(
                account_id,
                restriction_status=restriction_status,
                restriction_reason=summary,
                raw_reply=None,
            )
            logger.warning(compose_log(f"状态检查失败｜{summary}", account_id=account_id))
            return AccountStatusCheckResult(
                login_status=runtime_status,
                restriction_status=restriction_status,
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

    async def _run_tgmatrix_spambot_check(self, session_file: Path, *, account_row) -> dict[str, Any]:
        script_path = self.collection_manager.settings.tg_matrix_dir / "electron" / "accounts" / "check-engine" / "telethon_spambot_check.py"
        if not script_path.exists():
            raise FileNotFoundError(str(script_path))

        timeout_seconds = max(5, int(getattr(self.collection_manager.settings, "account_check_timeout_seconds", 25) or 25))
        proxy_attempts = self._build_proxy_attempts(account_row)
        last_result: dict[str, Any] | None = None

        for proxy_row in proxy_attempts:
            raw_proxy = self._build_tgmatrix_proxy_payload(proxy_row)
            command = [sys.executable, str(script_path), str(session_file), str(timeout_seconds)]
            if raw_proxy is not None:
                command.append(raw_proxy)

            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.collection_manager.settings.tg_matrix_dir),
            )
            stdout, stderr = await process.communicate()
            result = self._parse_tgmatrix_script_result(process.returncode, stdout, stderr)
            last_result = result

            if proxy_row is not None and self._is_tgmatrix_proxy_retryable(result):
                logger.warning(
                    compose_log(
                        f"TG-Matrix 检测代理重试｜{self.collection_manager._short_error(Exception(str(result.get('reason') or 'unknown')))}",
                        account_id=int(account_row["id"]),
                    )
                )
                continue

            if proxy_row is not None:
                self.collection_manager._remember_working_proxy(proxy_row)
            return result

        return last_result or {"status": "unknown", "reason": "account_check_no_result"}

    def _build_proxy_attempts(self, account_row) -> list[dict[str, Any] | None]:
        owner_id = 0
        try:
            owner_id = int(account_row["owner_id"] or 0)
        except Exception:
            owner_id = 0
        proxy_pool = self.collection_manager.db.get_global_proxies(owner_id=owner_id) if owner_id else []
        if not proxy_pool:
            return [None]
        ordered_pool = self.collection_manager._ordered_proxy_pool(proxy_pool)
        return ordered_pool if len(ordered_pool) > 1 else ordered_pool * 2

    @staticmethod
    def _build_tgmatrix_proxy_payload(proxy_row) -> str | None:
        if not proxy_row:
            return None
        payload = {
            "type": str(proxy_row.get("proxy_type") or "").strip().lower(),
            "host": str(proxy_row.get("proxy_host") or "").strip(),
            "port": int(proxy_row.get("proxy_port") or 0),
            "username": str(proxy_row.get("proxy_username") or "").strip(),
            "password": str(proxy_row.get("proxy_password") or "").strip(),
        }
        if not payload["type"] or not payload["host"] or payload["port"] <= 0:
            return None
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _parse_tgmatrix_script_result(returncode: int, stdout: bytes, stderr: bytes) -> dict[str, Any]:
        raw_stdout = stdout.decode("utf-8", errors="ignore").strip()
        raw_stderr = stderr.decode("utf-8", errors="ignore").strip()
        if returncode != 0:
            return {"status": "unknown", "reason": raw_stderr or raw_stdout or f"process_exit_{returncode}"}
        if not raw_stdout:
            return {"status": "unknown", "reason": raw_stderr or "empty_stdout"}
        try:
            payload = json.loads(raw_stdout)
        except json.JSONDecodeError:
            return {"status": "unknown", "reason": f"invalid_json:{raw_stdout[:200]}"}
        if not isinstance(payload, dict):
            return {"status": "unknown", "reason": "invalid_payload"}
        return payload

    @staticmethod
    def _is_tgmatrix_proxy_retryable(result: dict[str, Any]) -> bool:
        status = str(result.get("status") or "").strip().lower()
        reason = str(result.get("reason") or "").strip().lower()
        if status == "timeout":
            return True
        retryable_keywords = (
            "timeout",
            "timed out",
            "time out",
            "proxy",
            "connection reset",
            "connection aborted",
            "connection closed",
            "server closed the connection",
            "not connected",
            "network is unreachable",
            "failed to establish a new connection",
        )
        return status == "unknown" and any(keyword in reason for keyword in retryable_keywords)

    @staticmethod
    def _safe_runtime_status(current_status: str | None) -> str:
        runtime_status = str(current_status or "active")
        if runtime_status not in {"active", "checking", "collecting"}:
            return "active"
        return runtime_status

    @classmethod
    def _map_tgmatrix_reply_status(cls, reply_text: str) -> tuple[str, str]:
        text = re.sub(r"\s+", " ", reply_text or "").strip()
        lowered = text.lower()
        restriction_until = cls._extract_restriction_until(text)

        tgmatrix_rules: list[tuple[str, str, tuple[str, ...]]] = [
            ("frozen", "冻结", ("frozen", "freeze state", "account frozen", "violations of the telegram terms of service", "已冻结", "冻结")),
            ("restricted", "多 IP / 异地登录风险", ("multiple ip", "different ip", "many locations", "多ip", "异地登录")),
            ("session_invalid", "session 已失效或已封禁", ("phone number banned", "this number is banned", "账号已封禁", "封禁")),
            ("temp_mutual", "临时双向", ("temporary", "temporarily", "for now", "暂时限制", "临时双向")),
            ("restricted", "双向限制", ("while the account is limited", "you will not be able to send messages to people who do not have your number", "add them to groups and channels", "cannot send messages", "some phone numbers may not receive your messages", "双向限制", "被限制", "limited")),
            ("geo_limited", "地理位置限制", ("some phone numbers may trigger a harsh response", "some phone numbers may trigger", "地理位置限制")),
            ("unrestricted", "无限制", ("no limits are currently applied", "good news", "free as a bird", "没有限制", "一切正常")),
        ]

        normalized_text = lowered.replace("multiple   ip", "multiple ip")
        for restriction_status, summary, patterns in tgmatrix_rules:
            if any(pattern in normalized_text or pattern in text for pattern in patterns):
                if restriction_status == "temp_mutual":
                    return "temp_mutual", cls._format_temp_mutual_summary(restriction_until)
                return restriction_status, summary

        if any(keyword in lowered for keyword in ("spam", "can't send", "can only send", "cannot send")):
            if restriction_until is not None:
                return "temp_mutual", cls._format_temp_mutual_summary(restriction_until)
            return "restricted", "双向限制"
        return "unknown", "待人工确认"

    @staticmethod
    def _map_tgmatrix_invalid_summary(status: str) -> str:
        mapping = {
            "banned": "session 已失效或已封禁",
            "not_logged_in": "session 未登录",
            "session_expired": "session 已失效",
        }
        return mapping.get(status, "session 已失效或已封禁")

    @staticmethod
    def _format_tgmatrix_frozen_summary(raw_result: dict[str, Any]) -> str:
        freeze_until = str(raw_result.get("freeze_until_text") or "").strip()
        freeze_since = str(raw_result.get("freeze_since_text") or "").strip()
        if freeze_until:
            return f"冻结（预计 {freeze_until} 解冻）"
        if freeze_since:
            return f"冻结（冻结时间 {freeze_since}）"
        return "冻结"

    @staticmethod
    def _humanize_tgmatrix_unknown(reason: str | None) -> str:
        raw = str(reason or "").strip()
        text = raw.lower()
        if not raw:
            return "待人工确认"
        if any(keyword in text for keyword in ("timeout", "timed out", "time out")):
            return "连接 Telegram 超时，请稍后重试"
        if any(keyword in text for keyword in ("proxy", "failed to establish a new connection", "network is unreachable")):
            return "代理或网络异常，请稍后重试"
        if any(keyword in text for keyword in ("not connected", "connection closed", "connection reset", "server closed the connection")):
            return "连接 Telegram 中断，请稍后重试"
        return raw

    async def _fetch_spambot_reply(self, account_row, *, client: TelegramClient | None = None) -> str:
        owns_client = client is None
        try:
            if client is None:
                client = await self.collection_manager.connect_client(Path(account_row["session_file"]), account_row=account_row, receive_updates=True)
            if not await client.is_user_authorized():
                raise RuntimeError("session 未登录")
            entity = await client.get_input_entity("SpamBot")
            async with client.conversation(entity, timeout=SPAMBOT_REPLY_TIMEOUT_SECONDS) as conversation:
                await conversation.send_message("/start")
                message = await conversation.get_response()
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
        if any(keyword in lowered for keyword in [
            "blocked for violations",
            "terms of service",
            "user reports confirmed by our moderators",
            "confirmed by our moderators",
            "your account was blocked",
            "violations of the telegram terms of service",
        ]):
            if restriction_until is not None:
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
        return DmAccountChecker._classify_session_issue(status, last_error)[1]

    @staticmethod
    def _classify_session_issue(status: str, last_error: str | None) -> tuple[str, str]:
        raw = (last_error or "").strip()
        text = raw.lower()
        if any(key in text for key in ["session 格式与当前环境不兼容", "too many values to unpack"]):
            return "session_invalid", "session 格式不兼容（原文件未改写）"
        if status == "unauthorized" or any(
            key in text
            for key in [
                "user_deactivated",
                "input_user_deactivated",
                "banned",
                "revoked",
                "phone_number_banned",
                "auth key duplicated",
                "the key is not registered in the system",
            ]
        ):
            return "session_invalid", "session 已失效或已封禁"
        if any(key in text for key in ["malformed", "not valid sqlite", "file is not a database", "缺少 sessions 表", "已损坏"]):
            return "session_invalid", "session 已损坏"
        if any(key in text for key in ["timeout", "timed out", "time out", "proxy"]):
            return "unknown", "连接 Telegram 超时，请稍后重试"
        if any(
            key in text
            for key in [
                "while disconnected",
                "connection closed while receiving data",
                "server closed the connection",
                "automatic reconnection failed",
                "not connected",
                "connection was closed",
                "connection reset",
                "incompletereaderror",
                "bytes read on a total",
                "already waiting for incoming data",
            ]
        ):
            return "unknown", "连接 Telegram 中断，请稍后重试"
        if "floodwait" in text:
            return "unknown", "触发 Telegram 限流，请稍后再试"
        return "unknown", raw or "账号状态异常"

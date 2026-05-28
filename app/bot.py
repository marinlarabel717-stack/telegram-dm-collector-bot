from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from dataclasses import asdict
import html
import json
import re
import tempfile
import time
import zipfile
from math import ceil
from pathlib import Path

import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .collector import CollectionManager
from .config import Settings
from .database import Database
from .dm_account_checker import DmAccountChecker
from .dm_content import content_type_label, message_mode_label, payload_preview
from .dm_links import normalize_channel_post_link, parse_channel_post_link
from .dm_postbot import describe_postbot_inline_result, fetch_postbot_inline_result
from .dm_repository import DmRepository, dm_log_action_label, dm_log_status_label
from .dm_sender import DmSenderManager
from .dm_targets import ParsedTarget, parse_targets_text
from .emoji import premium_button, restriction_badge, status_badge, tg_emoji
from .version import __version__

logger = logging.getLogger(__name__)


class DmCollectorBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = Database(settings.db_path)
        self.dm_repository = DmRepository(self.db)
        self.collection_manager = CollectionManager(
            settings,
            self.db,
            on_progress=self._on_task_progress,
            on_complete=self._on_task_complete,
        )
        self.dm_account_checker = DmAccountChecker(self.dm_repository, self.collection_manager)
        self.dm_sender = DmSenderManager(
            self.dm_repository,
            self.db,
            self.collection_manager,
            on_progress=self._on_dm_task_progress,
            on_complete=self._on_dm_task_complete,
        )
        self.application = Application.builder().token(settings.bot_token).build()
        self.user_states: dict[int, dict] = {}
        self.task_runners: dict[int, asyncio.Task] = {}
        self.dm_task_runners: dict[int, asyncio.Task] = {}
        self.task_watchers: dict[int, asyncio.Task] = {}
        self.progress_throttle: dict[int, float] = {}
        self.dm_progress_throttle: dict[int, float] = {}
        self.progress_snapshots: dict[int, dict[str, float | int]] = {}
        self.application.bot_data["settings"] = settings
        self.application.bot_data["db"] = self.db
        recovered = self.db.recover_interrupted_tasks(reason="机器人重启，已停止上次未完成任务并释放账号")
        if recovered:
            logger.info("启动时已回收中断采集任务: %s", recovered)
        recovered_dm = self.dm_repository.recover_interrupted_dm_tasks(reason="机器人重启，已停止上次未完成私信任务并重置发送进度")
        if recovered_dm:
            logger.info("启动时已回收中断私信任务: %s", recovered_dm)
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.application.add_handler(CommandHandler("start", self.start), group=0)
        self.application.add_handler(CommandHandler("stats", self.stats), group=0)
        self.application.add_handler(CommandHandler("export", self.export_data), group=0)
        self.application.add_handler(CallbackQueryHandler(self.handle_callback), group=0)
        self.application.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & filters.Document.ALL, self.handle_document_upload),
            group=0,
        )
        self.application.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, self.handle_admin_text),
            group=0,
        )
        self.application.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, self.capture_private_message),
            group=1,
        )
        self.application.add_error_handler(self.handle_error)

    async def handle_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        exc = context.error
        logger.exception("PTB update 处理失败: %s", exc)

        effective_message = getattr(update, "effective_message", None)
        if not effective_message:
            return
        try:
            await effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 刚刚这一步执行失败了，我已经记下日志。请重试一次；如果还报错，把刚刚的操作再发我。",
                parse_mode=ParseMode.HTML,
            )
        except Exception:  # noqa: BLE001
            logger.exception("发送统一错误提示失败")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat or not update.effective_message:
            return

        self.db.upsert_user(update.effective_user, chat_id=update.effective_chat.id, increment_start=True)
        text = self._build_welcome_text(update.effective_user.id)
        markup = self._build_main_menu(update.effective_user.id)
        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=markup,
        )

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        text = self._build_stats_text()
        await self._reply_or_edit(update, text, self._build_main_menu(update.effective_user.id))

    async def export_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await self._send_dm_exports(update.effective_chat.id)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not update.effective_user:
            return
        if not await self._ensure_admin(update):
            return

        data = query.data or ""
        if data.startswith("preview:noop"):
            await query.answer("这是预览按钮。真实私信时会按原始按钮生效。", show_alert=False)
            return
        await query.answer()

        if data == "menu:main":
            self._clear_state(update.effective_user.id)
            await self._safe_edit(query, self._build_welcome_text(update.effective_user.id), self._build_main_menu(update.effective_user.id))
            return
        if data == "menu:stats":
            await self._safe_edit(query, self._build_stats_text(), self._build_main_menu(update.effective_user.id))
            return
        if data == "menu:accounts":
            await self._show_accounts_menu(query, page=1)
            return
        if data == "menu:collect":
            await self._show_collect_menu(query)
            return
        if data == "menu:dm":
            await self._show_dm_menu(query)
            return
        if data == "menu:history":
            await self._show_history(query, page=1)
            return
        if data.startswith("menu:history:"):
            page = int(data.split(":")[-1])
            await self._show_history(query, page=page)
            return
        if data == "account:upload":
            self.user_states[update.effective_user.id] = {"mode": "await_session_upload"}
            await self._safe_edit(query, self._upload_prompt_text(), self._single_back_keyboard("menu:accounts"))
            return
        if data.startswith("account:list:"):
            page = int(data.split(":")[-1])
            await self._show_account_list(query, page=page)
            return
        if data.startswith("account:view:"):
            account_id = int(data.split(":")[-1])
            await self._show_account_detail(query, account_id)
            return
        if data == "account:check_all":
            await self._check_all_accounts(query)
            return
        if data == "account:purge_invalid":
            await self._purge_invalid_accounts(query)
            return
        if data.startswith("account:export:"):
            bucket = data.split(":")[-1]
            await self._export_accounts_by_bucket(query, update.effective_chat.id, bucket)
            return
        if data.startswith("account:check:"):
            account_id = int(data.split(":")[-1])
            await self._check_account(query, account_id)
            return
        if data.startswith("account:delete:"):
            account_id = int(data.split(":")[-1])
            await self._delete_account(query, account_id)
            return
        if data == "collect:new":
            await self._show_collect_create_menu(query)
            return
        if data == "dm:new":
            await self._start_dm_wizard(query, update.effective_user.id)
            return
        if data == "dm:tasks":
            await self._show_dm_task_list(query, page=1)
            return
        if data == "dm:tasks:clear":
            await self._confirm_clear_dm_tasks(query)
            return
        if data == "dm:tasks:clear:confirm":
            await self._clear_dm_tasks(query)
            return
        if data.startswith("dm:tasks:"):
            page = int(data.split(":")[-1])
            await self._show_dm_task_list(query, page=page)
            return
        if data.startswith("dm:view:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            await self._show_dm_task_detail(query, task_id, page=page)
            return
        if data.startswith("dm:refresh:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            await self._show_dm_task_detail(query, task_id, page=page)
            return
        if data.startswith("dm:stop:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            await self._stop_dm_task(query, task_id, page=page)
            return
        if data.startswith("dm:export:"):
            task_id = int(data.split(":")[-1])
            await self._send_dm_task_result(update.effective_chat.id, task_id)
            return
        if data == "collect:new:channel":
            await self._start_collect_wizard(query, update.effective_user.id)
            return
        if data == "collect:new:group":
            await self._start_group_collect_wizard(query, update.effective_user.id)
            return
        if data == "collect:tasks":
            await self._show_task_list(query, page=1)
            return
        if data.startswith("collect:tasks:"):
            page = int(data.split(":")[-1])
            await self._show_task_list(query, page=page)
            return
        if data.startswith("task:view:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            source = parts[4] if len(parts) > 4 else "tasks"
            await self._show_task_detail(query, task_id, page=page, source=source, force=True)
            return
        if data.startswith("task:refresh:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            source = parts[4] if len(parts) > 4 else "tasks"
            await self._show_task_detail(query, task_id, page=page, source=source, force=True)
            return
        if data.startswith("task:stop:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            source = parts[4] if len(parts) > 4 else "tasks"
            self.db.stop_collect_task_now(task_id, reason="管理员手动停止任务，已保留当前已采集结果并释放账号")
            runner = self.task_runners.get(task_id)
            if runner and not runner.done():
                runner.cancel()
                try:
                    await runner
                except asyncio.CancelledError:
                    pass
            await self._show_task_detail(query, task_id, page=page, source=source, force=True)
            return
        if data.startswith("task:delete:"):
            parts = data.split(":")
            task_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 1
            source = parts[4] if len(parts) > 4 else "tasks"
            await self._delete_task(query, task_id, page=page, source=source)
            return
        if data == "task:clear_history":
            await self._clear_task_history(query)
            return
        if data.startswith("task:export:"):
            task_id = int(data.split(":")[-1])
            await self._send_task_result(update.effective_chat.id, task_id)
            return
        if data == "wizard:cancel":
            self._clear_state(update.effective_user.id)
            await self._safe_edit(query, self._build_welcome_text(update.effective_user.id), self._build_main_menu(update.effective_user.id))
            return
        if data == "dm:wizard:cancel":
            self._clear_state(update.effective_user.id)
            await self._show_dm_menu(query)
            return
        if data == "dm:wizard:back:targets":
            state = self.user_states.get(update.effective_user.id) or {}
            state["mode"] = "await_dm_targets"
            await self._safe_edit(query, self._dm_targets_prompt_text(), self._single_back_keyboard("dm:wizard:cancel"))
            return
        if data == "dm:wizard:acc:auto":
            await self._dm_wizard_auto_accounts(query, update.effective_user.id)
            return
        if data == "dm:wizard:acc:page_all":
            await self._dm_wizard_select_current_page(query, update.effective_user.id)
            return
        if data.startswith("dm:wizard:acc:page:"):
            page = int(data.split(":")[-1])
            await self._dm_wizard_change_account_page(query, update.effective_user.id, page)
            return
        if data.startswith("dm:wizard:acc:toggle:"):
            account_id = int(data.split(":")[-1])
            await self._dm_wizard_toggle_account(query, update.effective_user.id, account_id)
            return
        if data == "dm:wizard:acc:done":
            await self._dm_wizard_finish_accounts(query, update.effective_user.id)
            return
        if data == "dm:wizard:cfg:done":
            await self._dm_wizard_finish_config(query, update.effective_user.id)
            return
        if data == "dm:wizard:back:config":
            state = self.user_states.get(update.effective_user.id) or {}
            draft = state.get("draft") or {}
            state["mode"] = "dm_config"
            await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))
            return
        if data == "dm:wizard:back_accounts":
            state = self.user_states.get(update.effective_user.id) or {}
            draft = state.get("draft") or {}
            state["mode"] = "dm_select_accounts"
            await self._render_dm_account_selection(query, draft)
            return
        if data == "dm:wizard:back:greeting":
            state = self.user_states.get(update.effective_user.id) or {}
            draft = state.get("draft") or {}
            state["mode"] = "await_dm_greeting"
            await self._safe_edit(query, self._dm_message_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:config"))
            return
        if data == "dm:wizard:back:body":
            state = self.user_states.get(update.effective_user.id) or {}
            draft = state.get("draft") or {}
            content_type = draft.get("content_type") or "text"
            state["mode"] = "await_dm_body" if content_type in {"text", "post"} else ("await_dm_media" if content_type == "media" else "await_dm_forward")
            await self._safe_edit(query, self._dm_body_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:greeting"))
            return
        if data == "dm:wizard:back:input":
            await self._dm_wizard_back_to_input(query, update.effective_user.id)
            return
        if data == "dm:wizard:mode:toggle":
            await self._dm_wizard_toggle_mode(query, update.effective_user.id)
            return
        if data == "dm:wizard:content:cycle":
            await self._dm_wizard_cycle_content_type(query, update.effective_user.id)
            return
        if data == "dm:wizard:limit:cycle":
            await self._dm_wizard_cycle_limit(query, update.effective_user.id)
            return
        if data == "dm:wizard:delay:cycle":
            await self._dm_wizard_cycle_delay(query, update.effective_user.id)
            return
        if data == "dm:wizard:worker:cycle":
            await self._dm_wizard_cycle_worker_count(query, update.effective_user.id)
            return
        if data == "dm:wizard:stage1:cycle":
            await self._dm_wizard_cycle_stage1_delay(query, update.effective_user.id)
            return
        if data == "dm:wizard:stage2:cycle":
            await self._dm_wizard_cycle_stage2_delay(query, update.effective_user.id)
            return
        if data == "dm:wizard:pin:toggle":
            await self._dm_wizard_toggle_pin(query, update.effective_user.id)
            return
        if data == "dm:wizard:pin_delay:cycle":
            await self._dm_wizard_cycle_pin_delay(query, update.effective_user.id)
            return
        if data == "dm:wizard:preview":
            await self._dm_wizard_preview(query, update.effective_user.id)
            return
        if data == "dm:wizard:typing:toggle":
            await self._dm_wizard_toggle_typing(query, update.effective_user.id)
            return
        if data == "dm:wizard:switch:toggle":
            await self._dm_wizard_toggle_switch(query, update.effective_user.id)
            return
        if data == "dm:wizard:start":
            await self._dm_wizard_start_task(query, update.effective_user.id)
            return
        if data.startswith("wizard:gflt:toggle:"):
            key = data.split(":")[-1]
            await self._toggle_group_filter(query, update.effective_user.id, key)
            return
        if data == "wizard:gflt:done":
            await self._wizard_finish_group_filters(query, update.effective_user.id)
            return
        if data.startswith("wizard:days:"):
            days = int(data.split(":")[-1])
            await self._wizard_set_days(query, update.effective_user.id, days)
            return
        if data == "wizard:days_custom":
            state = self.user_states.setdefault(update.effective_user.id, {"draft": {}})
            state["mode"] = "await_custom_days"
            await self._safe_edit(query, self._custom_days_prompt_text(), self._single_back_keyboard("collect:new"))
            return
        if data == "wizard:acc:auto":
            await self._wizard_auto_accounts(query, update.effective_user.id)
            return
        if data.startswith("wizard:acc:toggle:"):
            account_id = int(data.split(":")[-1])
            await self._wizard_toggle_account(query, update.effective_user.id, account_id)
            return
        if data == "wizard:acc:done":
            await self._wizard_finish_accounts(query, update.effective_user.id)
            return
        if data == "wizard:wrk_custom":
            state = self.user_states.setdefault(update.effective_user.id, {"draft": {}})
            draft = state.setdefault("draft", {})
            state["mode"] = "await_custom_workers"
            await self._safe_edit(query, self._custom_workers_prompt_text(draft), self._single_back_keyboard("collect:new"))
            return
        if data.startswith("wizard:wrk:"):
            worker_count = int(data.split(":")[-1])
            await self._wizard_set_workers(query, update.effective_user.id, worker_count)
            return
        if data == "wizard:start":
            await self._wizard_start_task(query, update.effective_user.id)
            return

    async def handle_document_upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_message or not update.effective_chat:
            return
        if not self._is_admin(update.effective_user.id):
            return

        document = update.effective_message.document
        if not document:
            return

        file_name = (document.file_name or "").lower()
        state = self.user_states.get(update.effective_user.id) or {}
        if state.get("mode") == "await_dm_media":
            await self._handle_dm_media_input(update, state)
            return
        if file_name.endswith(".txt"):
            await self._handle_target_txt_upload(update, document, state)
            return
        if not (file_name.endswith(".session") or file_name.endswith(".zip")):
            return

        await update.effective_message.reply_text(
            f"{tg_emoji(self.settings.emoji_waiting_id, '🕜')} 已收到文件，正在保存并检测，请稍等……",
            parse_mode=ParseMode.HTML,
        )

        try:
            await self.application.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
            session_files = await self._save_uploaded_session_files(document)
        except zipfile.BadZipFile:
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 这个 zip 看起来不是有效压缩包，请重新打包后再传。",
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("处理上传文件失败: %s", document.file_name)
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 处理上传文件时出错：<code>{html.escape(str(exc) or exc.__class__.__name__, quote=False)[:300]}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        if not session_files:
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 压缩包里没找到可用的 <code>.session</code> 文件。",
                parse_mode=ParseMode.HTML,
            )
            return

        imported_accounts = []
        deleted_broken: list[tuple[str, str]] = []
        deleted_banned: list[tuple[str, str]] = []
        kept_other_errors: list[tuple[str, str]] = []
        for session_file in session_files:
            result = await self.collection_manager.verify_session_file(session_file)
            issue_text = self._humanize_account_issue(result.status, result.last_error)
            if result.status == "active":
                account = self.db.upsert_account(
                    session_name=session_file.stem,
                    session_file=str(session_file),
                    tg_user_id=result.tg_user_id,
                    phone=result.phone,
                    username=result.username,
                    display_name=result.display_name,
                    status=result.status,
                    last_error=result.last_error,
                )
                imported_accounts.append(account)
                continue

            self._purge_session_artifacts(session_file)
            if self._should_auto_purge_account(result.status, result.last_error):
                if "损坏" in issue_text:
                    deleted_broken.append((session_file.name, issue_text))
                else:
                    deleted_banned.append((session_file.name, issue_text))
            else:
                kept_other_errors.append((session_file.name, issue_text))

        self._clear_state(update.effective_user.id)
        if len(imported_accounts) == 1 and not deleted_broken and not deleted_banned and not kept_other_errors:
            await update.effective_message.reply_text(
                self._format_account_text(imported_accounts[0]),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._build_account_detail_keyboard(imported_accounts[0]["id"]),
            )
            return

        lines = [
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} <b>导入处理完成</b>",
            f"成功导入：<code>{len(imported_accounts)}</code>",
            f"自动删除损坏：<code>{len(deleted_broken)}</code>",
            f"自动删除封禁/失效：<code>{len(deleted_banned)}</code>",
        ]
        if not imported_accounts:
            lines.append("\n<b>本次没有保留下任何可用账号</b>")
        if imported_accounts:
            lines.append("")
            lines.append("<b>已保留可用账号</b>")
            for account in imported_accounts[:10]:
                label = account["username"] or account["phone"] or account["display_name"] or account["session_name"]
                lines.append(f"• #{self._account_display_code(account)} {html.escape(str(label), quote=False)} · {status_badge(account['status'])}")
        if deleted_broken:
            lines.append("")
            lines.append("<b>已删除：session 已损坏</b>")
            for failed_name, failed_error in deleted_broken[:5]:
                lines.append(f"• <code>{html.escape(failed_name, quote=False)}</code> · <code>{html.escape(failed_error, quote=False)[:120]}</code>")
        if deleted_banned:
            lines.append("")
            lines.append("<b>已删除：封禁 / 失效</b>")
            for failed_name, failed_error in deleted_banned[:5]:
                lines.append(f"• <code>{html.escape(failed_name, quote=False)}</code> · <code>{html.escape(failed_error, quote=False)[:120]}</code>")
        if kept_other_errors:
            lines.append("")
            lines.append("<b>暂未保留到账号列表：其他异常</b>")
            for failed_name, failed_error in kept_other_errors[:5]:
                lines.append(f"• <code>{html.escape(failed_name, quote=False)}</code> · <code>{html.escape(failed_error, quote=False)[:120]}</code>")
        await update.effective_message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._single_back_keyboard("account:list:1"),
        )

    async def _handle_target_txt_upload(self, update: Update, document, state: dict) -> None:
        mode = state.get("mode")
        if mode == "await_dm_targets":
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_waiting_id, '🕜')} 已收到用户名单，正在读取并解析，请稍等……",
                parse_mode=ParseMode.HTML,
            )
            await self.application.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
            tg_file = await document.get_file()
            raw_bytes = await tg_file.download_as_bytearray()
            try:
                text = bytes(raw_bytes).decode("utf-8")
            except UnicodeDecodeError:
                text = bytes(raw_bytes).decode("utf-8-sig", errors="ignore")
            targets, invalid, duplicates = parse_targets_text(text)
            if not targets:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} txt 里没识别到有效用户名 / 手机号，请检查后重传。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            draft["targets"] = [asdict(item) for item in targets]
            draft["invalid_targets"] = invalid
            draft["duplicate_targets"] = duplicates
            draft["account_page"] = 1
            state["mode"] = "dm_select_accounts"
            await update.effective_message.reply_text(
                self._dm_select_accounts_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_dm_account_selection_keyboard(draft),
            )
            return
        if mode not in {"await_channels", "await_group_targets"}:
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_idea_id, '💡')} 这个 txt 只在 <b>新建采集任务</b> 时使用。先点“新建采集任务”，再上传 txt。",
                parse_mode=ParseMode.HTML,
            )
            return

        processing_text = "已收到群组文件，正在读取并解析，请稍等……" if mode == "await_group_targets" else "已收到频道文件，正在读取并解析，请稍等……"
        await update.effective_message.reply_text(
            f"{tg_emoji(self.settings.emoji_waiting_id, '🕜')} {processing_text}",
            parse_mode=ParseMode.HTML,
        )
        await self.application.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
        tg_file = await document.get_file()
        raw_bytes = await tg_file.download_as_bytearray()
        try:
            text = bytes(raw_bytes).decode("utf-8")
        except UnicodeDecodeError:
            text = bytes(raw_bytes).decode("utf-8-sig", errors="ignore")
        parser = self._parse_group_targets if mode == "await_group_targets" else self._parse_channels
        targets = parser(text)
        if not targets:
            fail_text = "txt 里没识别到有效群组，请检查内容后重传。" if mode == "await_group_targets" else "txt 里没识别到有效频道，请检查内容后重传。"
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} {fail_text}",
                parse_mode=ParseMode.HTML,
            )
            return
        draft = state.setdefault("draft", {})
        draft["channels"] = targets
        state["mode"] = "select_days"
        await update.effective_message.reply_text(
            self._select_days_text(targets, task_type=draft.get("task_type", "channel")),
            parse_mode=ParseMode.HTML,
            reply_markup=self._build_days_keyboard(),
        )

    async def _handle_dm_media_input(self, update: Update, state: dict) -> None:
        message = update.effective_message
        if not message:
            return
        try:
            payload = await self._save_dm_media_payload(message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("保存私信媒体失败")
            await message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 保存媒体失败：<code>{html.escape(self._humanize_dm_error(None, str(exc) or exc.__class__.__name__), quote=False)[:200]}</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if not payload:
            await message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 请发送图片 / 视频 / 文件，支持附带 caption。",
                parse_mode=ParseMode.HTML,
            )
            return
        draft = state.setdefault("draft", {})
        draft["content_type"] = "media"
        draft["media_kind"] = payload["media_kind"]
        draft["media_path"] = payload["media_path"]
        draft["media_file_name"] = payload["file_name"]
        draft["media_caption"] = payload.get("caption") or ""
        if draft.get("message_mode") == "three_stage":
            state["mode"] = "await_dm_closing"
            await message.reply_text(
                self._dm_closing_prompt_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._single_back_keyboard("dm:wizard:back:body"),
            )
            return
        state["mode"] = "dm_confirm"
        await message.reply_text(
            self._dm_confirm_text(draft),
            parse_mode=ParseMode.HTML,
            reply_markup=self._build_dm_confirm_keyboard("dm:wizard:back:input"),
        )

    async def _handle_dm_forward_input(self, update: Update, state: dict) -> None:
        message = update.effective_message
        if not message:
            return
        raw_link = (message.text or message.caption or "").strip()
        link = normalize_channel_post_link(raw_link)
        if not link:
            await message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 频道帖子链接格式不正确。",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return
        preview_text, preview_error = await self._fetch_channel_post_preview(link, state)
        draft = state.setdefault("draft", {})
        draft["content_type"] = "forward"
        draft["forward_link"] = link
        draft["forward_preview"] = link
        draft["forward_message_preview"] = preview_text or ""
        draft["forward_preview_error"] = preview_error or ""
        if draft.get("message_mode") == "three_stage":
            state["mode"] = "await_dm_closing"
            await message.reply_text(
                self._dm_closing_prompt_text(draft),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._single_back_keyboard("dm:wizard:back:body"),
            )
            return
        state["mode"] = "dm_confirm"
        await message.reply_text(
            self._dm_confirm_text(draft),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._build_dm_confirm_keyboard("dm:wizard:back:input"),
        )

    async def _fetch_channel_post_preview(self, link: str, state: dict) -> tuple[str | None, str | None]:
        parsed = parse_channel_post_link(link)
        if not parsed:
            return None, "链接格式无法解析"
        draft = state.get("draft") or {}
        account_ids = [int(item) for item in (draft.get("account_ids") or [])]
        if not account_ids:
            return None, "未选择预览账号"
        account = None
        for account_id in account_ids:
            row = self.db.get_account(account_id)
            if row and row["status"] == "active":
                account = row
                break
        if not account:
            return None, "没有可用账号可抓取帖子预览"

        client = None
        try:
            client = self.collection_manager._build_client(Path(account["session_file"]))
            await client.connect()
            if not await client.is_user_authorized():
                return None, "预览账号 session 已失效"
            entity = None
            if parsed["kind"] == "public":
                entity = await client.get_entity(f"@{parsed['username']}")
            else:
                entity = await client.get_entity(int(f"-100{parsed['channel_id']}"))
            post = await client.get_messages(entity, ids=int(parsed["message_id"]))
            if not post:
                return None, "帖子不存在或当前账号无权查看"
            text = (getattr(post, "message", None) or getattr(post, "text", None) or getattr(post, "raw_text", None) or "").strip()
            media_parts: list[str] = []
            if getattr(post, "photo", None):
                media_parts.append("图片")
            if getattr(post, "video", None):
                media_parts.append("视频")
            if getattr(post, "document", None):
                media_parts.append("文件")
            media_label = f"[{'+'.join(media_parts)}] " if media_parts else ""
            summary = (media_label + text).strip() or (media_label + "无正文，仅含媒体").strip() or "空消息"
            return summary[:180], None
        except Exception as exc:  # noqa: BLE001
            logger.warning("抓取频道帖子预览失败: %s", self.collection_manager._short_error(exc))
            return None, self._humanize_dm_error(None, self.collection_manager._short_error(exc))
        finally:
            if client is not None:
                await client.disconnect()

    async def _save_dm_media_payload(self, message) -> dict | None:
        media = None
        original_name = None
        media_kind = None
        mime_type = None
        if getattr(message, "photo", None):
            media = message.photo[-1]
            original_name = f"photo_{message.message_id}.jpg"
            media_kind = "photo"
        elif getattr(message, "video", None):
            media = message.video
            original_name = message.video.file_name or f"video_{message.message_id}.mp4"
            media_kind = "video"
            mime_type = getattr(message.video, "mime_type", None)
        elif getattr(message, "document", None):
            media = message.document
            original_name = message.document.file_name or f"document_{message.message_id}.bin"
            media_kind = "document"
            mime_type = getattr(message.document, "mime_type", None)
        if not media or not original_name or not media_kind:
            return None
        media_dir = self.settings.data_dir / "dm_media"
        media_dir.mkdir(parents=True, exist_ok=True)
        target = self._unique_dm_media_path(original_name)
        tg_file = await media.get_file()
        await tg_file.download_to_drive(custom_path=str(target))
        media_kind = self._detect_dm_preview_media_kind(target, media_kind, mime_type)
        return {
            "media_kind": media_kind,
            "media_path": str(target),
            "file_name": target.name,
            "caption": (message.caption or "").strip(),
        }

    def _unique_dm_media_path(self, file_name: str) -> Path:
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", Path(file_name).name) or f"dm_media_{int(time.time())}"
        target = self.settings.data_dir / "dm_media" / safe_name
        if not target.exists():
            return target
        return target.parent / f"{target.stem}_{int(time.time() * 1000)}{target.suffix}"

    async def handle_admin_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_message:
            return
        if not self._is_admin(update.effective_user.id):
            return

        state = self.user_states.get(update.effective_user.id)
        if not state:
            return

        mode = state.get("mode")
        text = (update.effective_message.text or "").strip()
        if mode == "await_channels":
            channels = self._parse_channels(text)
            if not channels:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 没识别到有效频道，请按一行一个频道重新发送。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            draft["channels"] = channels
            state["mode"] = "select_days"
            await update.effective_message.reply_text(
                self._select_days_text(channels, task_type=draft.get("task_type", "channel")),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_days_keyboard(),
            )
            return

        if mode == "await_group_targets":
            channels = self._parse_group_targets(text)
            if not channels:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 没识别到有效群组，请按一行一个群链接或群用户名重新发送。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            draft["channels"] = channels
            state["mode"] = "select_days"
            await update.effective_message.reply_text(
                self._select_days_text(channels, task_type=draft.get("task_type", "channel")),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_days_keyboard(),
            )
            return

        if mode == "await_dm_targets":
            targets, invalid, duplicates = parse_targets_text(text)
            if not targets:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 没识别到有效用户名 / 手机号，请按一行一个重新发送。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            draft["targets"] = [asdict(item) for item in targets]
            draft["invalid_targets"] = invalid
            draft["duplicate_targets"] = duplicates
            draft["account_page"] = 1
            state["mode"] = "dm_select_accounts"
            await update.effective_message.reply_text(
                self._dm_select_accounts_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_dm_account_selection_keyboard(draft),
            )
            return

        if mode == "await_dm_message":
            if not text:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 请输入要发送的文本内容。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            if (draft.get("content_type") or "text") == "post":
                draft["post_code"] = text
                draft["text"] = text
            else:
                draft["text"] = text
            state["mode"] = "dm_confirm"
            await update.effective_message.reply_text(
                self._dm_confirm_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_dm_confirm_keyboard("dm:wizard:back:input"),
            )
            return

        if mode == "await_dm_greeting":
            draft = state.setdefault("draft", {})
            draft["greeting"] = text
            content_type = draft.get("content_type") or "text"
            state["mode"] = "await_dm_body" if content_type in {"text", "post"} else ("await_dm_media" if content_type == "media" else "await_dm_forward")
            await update.effective_message.reply_text(
                self._dm_body_prompt_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._single_back_keyboard("dm:wizard:back:greeting"),
            )
            return

        if mode == "await_dm_body":
            if not text:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 请输入主消息内容。",
                    parse_mode=ParseMode.HTML,
                )
                return
            draft = state.setdefault("draft", {})
            draft["body"] = text
            if (draft.get("content_type") or "text") == "post":
                draft["post_code"] = text
            state["mode"] = "await_dm_closing"
            await update.effective_message.reply_text(
                self._dm_closing_prompt_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._single_back_keyboard("dm:wizard:back:body"),
            )
            return

        if mode == "await_dm_closing":
            draft = state.setdefault("draft", {})
            draft["closing"] = text
            state["mode"] = "dm_confirm"
            await update.effective_message.reply_text(
                self._dm_confirm_text(draft),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_dm_confirm_keyboard("dm:wizard:back:input"),
            )
            return

        if mode == "await_dm_media":
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_upload_id, '📷')} 当前在等待媒体内容，请直接发送图片 / 视频 / 文件，可附带 caption。",
                parse_mode=ParseMode.HTML,
            )
            return

        if mode == "await_dm_forward":
            await self._handle_dm_forward_input(update, state)
            return

        if mode == "await_custom_days":
            try:
                days = int(text)
            except ValueError:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 请输入整数天数，例如 7。",
                    parse_mode=ParseMode.HTML,
                )
                return
            if days <= 0 or days > 365:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 天数请控制在 1 到 365 之间。",
                    parse_mode=ParseMode.HTML,
                )
                return
            await self._wizard_set_days(None, update.effective_user.id, days, reply_message=update.effective_message)
            return

        if mode == "await_custom_workers":
            draft = state.setdefault("draft", {})
            max_workers = self._max_worker_count(draft)
            try:
                workers = int(text)
            except ValueError:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 请输入整数线程数，例如 {max_workers}。",
                    parse_mode=ParseMode.HTML,
                )
                return
            if workers <= 0 or workers > max_workers:
                await update.effective_message.reply_text(
                    f"{tg_emoji(self.settings.emoji_error_id, '❌')} 线程数请控制在 1 到 {max_workers} 之间。",
                    parse_mode=ParseMode.HTML,
                )
                return
            await self._wizard_set_workers(None, update.effective_user.id, workers, reply_message=update.effective_message)

    async def capture_private_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat or not update.effective_message:
            return

        user = update.effective_user
        chat = update.effective_chat
        message = update.effective_message
        self.db.upsert_user(user, chat_id=chat.id, increment_message=True)
        raw_json = message.to_dict() if self.settings.save_raw_update else None
        self.db.save_message(
            message=message,
            tg_user_id=user.id,
            chat_id=chat.id,
            message_type=self._detect_message_type(message),
            raw_json=raw_json,
        )

        if self._is_admin(user.id):
            state = self.user_states.get(user.id) or {}
            mode = state.get("mode")
            if mode == "await_dm_media" and (message.photo or message.video):
                await self._handle_dm_media_input(update, state)
            elif mode == "await_dm_forward" and (message.text or message.caption):
                await self._handle_dm_forward_input(update, state)
            return

        if self.settings.forward_to_admins:
            await self._fanout_to_admins(update)

        if self.settings.auto_reply_enabled:
            await message.reply_text(
                self._build_auto_reply_text(),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

    # ---------- callbacks / menu rendering ----------
    async def _show_accounts_menu(self, query, page: int = 1) -> None:
        count = self.db.count_accounts()
        stats = self.db.get_account_status_counts()
        restriction_stats = self.dm_repository.get_account_restriction_summary_counts()
        text = (
            f"{tg_emoji(self.settings.emoji_list_id, '👤')} <b>账号管理</b>\n"
            f"当前存活账号：<code>{count}</code>\n"
            f"运行中：<code>{stats['active']}</code> · 检测中：<code>{stats['checking']}</code> · 采集中：<code>{stats['collecting']}</code>\n"
            f"状态：无限制 <code>{restriction_stats['unrestricted']}</code> · 受限 <code>{restriction_stats['limited']}</code> · 冻结 <code>{restriction_stats['frozen']}</code> · 待检测 <code>{restriction_stats['unknown']}</code>\n"
            f"待清理无效：<code>{stats['invalid']}</code>\n\n"
            f"上传 .session 后会立即做一次登录验证；点“检查状态”会额外向 SpamBot 读取私信限制状态。现在也支持按状态导出原始 session/zip。"
        )
        keyboard = [
            [
                premium_button("上传 session", self.settings.emoji_upload_id, callback_data="account:upload"),
                premium_button("账号列表", self.settings.emoji_list_id, callback_data=f"account:list:{page}"),
            ],
            [
                premium_button("一键检查状态", self.settings.emoji_stats_id, callback_data="account:check_all"),
                premium_button("一键清理无效", self.settings.emoji_error_id, callback_data="account:purge_invalid"),
            ],
            [
                premium_button("导出无限制", self.settings.emoji_success_id, callback_data="account:export:unrestricted"),
                premium_button("导出受限", self.settings.emoji_timeout_id, callback_data="account:export:limited"),
            ],
            [
                premium_button("导出全部账号", self.settings.emoji_export_id, callback_data="account:export:all"),
            ],
            [
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
                premium_button("刷新页面", self.settings.emoji_refresh_id, callback_data="menu:accounts"),
            ],
        ]
        await self._safe_edit(query, text, InlineKeyboardMarkup(keyboard))

    async def _show_account_list(self, query, page: int = 1) -> None:
        per_page = 6
        total = self.db.count_accounts()
        rows = self.db.list_accounts(limit=per_page, offset=(page - 1) * per_page)
        total_pages = max(1, ceil(total / per_page))
        lines = [
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>账号列表</b>",
            f"页码：<code>{page}/{total_pages}</code>",
            f"存活账号：<code>{total}</code>",
            "",
        ]
        if not rows:
            lines.append("当前没有存活账号，先上传一个可用的 .session 文件吧。")
        else:
            for row in rows:
                label = row["username"] or row["phone"] or row["display_name"] or row["session_name"]
                lines.append(
                    f"• #{self._account_display_code(row)} {html.escape(str(label), quote=False)} · {status_badge(row['status'])} · {restriction_badge(row['restriction_status'])}"
                )

        keyboard: list[list] = [
            [
                premium_button("一键检查状态", self.settings.emoji_stats_id, callback_data="account:check_all"),
                premium_button("一键清理无效", self.settings.emoji_error_id, callback_data="account:purge_invalid"),
            ],
            [
                premium_button("导出无限制", self.settings.emoji_success_id, callback_data="account:export:unrestricted"),
                premium_button("导出受限", self.settings.emoji_timeout_id, callback_data="account:export:limited"),
            ],
            [
                premium_button("导出全部账号", self.settings.emoji_export_id, callback_data="account:export:all"),
            ],
        ]
        row_buffer = []
        for row in rows:
            label = row["username"] or row["phone"] or row["session_name"]
            row_buffer.append(
                premium_button(f"#{self._account_display_code(row)} {str(label)[:28]}", self.settings.emoji_list_id, callback_data=f"account:view:{row['id']}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        nav = []
        if page > 1:
            nav.append(premium_button("上一页", self.settings.emoji_back_id, callback_data=f"account:list:{page - 1}"))
        if page < total_pages:
            nav.append(premium_button("下一页", self.settings.emoji_next_id, callback_data=f"account:list:{page + 1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([
            premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data=f"account:list:{page}"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _show_account_detail(self, query, account_id: int) -> None:
        account = self.db.get_account(account_id)
        if not account:
            await self._safe_edit(query, self._not_found_text("账号不存在或已删除"), self._single_back_keyboard("account:list:1"))
            return
        await self._safe_edit(query, self._format_account_text(account), self._build_account_detail_keyboard(account_id))

    async def _check_account(self, query, account_id: int) -> None:
        account = self.db.get_account(account_id)
        if not account:
            await self._show_account_detail(query, account_id)
            return
        self.db.update_account_status(account_id, status="checking", last_error=None)
        self.dm_repository.update_account_restriction(account_id, restriction_status="checking", restriction_reason="正在检测")
        await self._safe_edit(query, self._format_account_text(self.db.get_account(account_id)), self._build_account_detail_keyboard(account_id))
        result = await self.dm_account_checker.check_account_status(account)
        account = self.db.get_account(account_id)
        if not account:
            await self._safe_edit(query, self._not_found_text("账号不存在或已删除"), self._single_back_keyboard("account:list:1"))
            return
        issue_text = result.summary
        if result.restriction_status in {"session_invalid", "frozen"}:
            label = account["username"] or account["phone"] or account["display_name"] or account["session_name"]
            bucket = "invalid" if result.restriction_status == "session_invalid" else "frozen"
            backup_note = "已自动导出原始文件"
            try:
                await self._send_account_export(query.message.chat_id, [account], bucket, auto_delete=True)
            except FileNotFoundError:
                self._purge_account_files(account)
                self.db.delete_account(account_id)
                backup_note = "未找到可导出的原始文件，已直接删除账号"
            text = (
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} <b>账号已自动删除</b>\n"
                f"名称：<code>{html.escape(str(label), quote=False)}</code>\n"
                f"原因：<code>{html.escape(issue_text, quote=False)}</code>\n"
                f"备份：<code>{backup_note}</code>"
            )
            await self._safe_edit(query, text, self._single_back_keyboard("account:list:1"))
            return
        await self._show_account_detail(query, account_id)

    async def _check_all_accounts(self, query) -> None:
        rows = self.db.list_all_accounts()
        if not rows:
            await self._safe_edit(query, self._not_found_text("当前没有可检测的账号。"), self._single_back_keyboard("menu:accounts"))
            return

        unrestricted = 0
        limited = 0
        frozen = 0
        unknown = 0
        deleted_broken: list[str] = []
        deleted_banned: list[str] = []
        deleted_frozen: list[str] = []
        removed_invalid_rows: list = []
        removed_frozen_rows: list = []
        kept_other_errors: list[str] = []
        total_checked = len(rows)
        processed = 0
        parallel = min(10, total_checked)

        semaphore = asyncio.Semaphore(parallel)

        async def _verify_one(account_row):
            async with semaphore:
                result = await self.dm_account_checker.check_account_status(account_row)
                refreshed = self.db.get_account(account_row["id"])
                return account_row, refreshed, result

        await self._safe_edit(
            query,
            self._format_check_all_progress_text(
                total=total_checked,
                processed=0,
                current_label="准备开始",
                unrestricted=0,
                limited=0,
                frozen=0,
                unknown=0,
                deleted_broken=0,
                deleted_banned=0,
                kept_other_errors=0,
                parallel=parallel,
            ),
            InlineKeyboardMarkup([
                [
                    premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
                    premium_button("账号列表", self.settings.emoji_list_id, callback_data="account:list:1"),
                ],
            ]),
        )

        tasks = [asyncio.create_task(_verify_one(row)) for row in rows]
        for finished in asyncio.as_completed(tasks):
            original, refreshed, result = await finished
            processed += 1
            account = refreshed or original
            label = account["username"] or account["phone"] or account["display_name"] or account["session_name"]
            issue_text = result.summary
            if result.restriction_status == "unrestricted":
                unrestricted += 1
            elif result.restriction_status in {"temp_mutual", "permanent_mutual", "geo_limited", "spam_limited", "restricted"}:
                limited += 1
            elif result.restriction_status == "frozen":
                frozen += 1
                deleted_frozen.append(str(label))
                if refreshed:
                    removed_frozen_rows.append(refreshed)
                    self.db.delete_account(refreshed["id"])
            elif result.restriction_status == "session_invalid":
                if "损坏" in issue_text:
                    deleted_broken.append(str(label))
                else:
                    deleted_banned.append(str(label))
                if refreshed:
                    removed_invalid_rows.append(refreshed)
                    self.db.delete_account(refreshed["id"])
            else:
                unknown += 1
                kept_other_errors.append(f"{label}｜{issue_text}")

            await self._safe_edit(
                query,
                self._format_check_all_progress_text(
                    total=total_checked,
                    processed=processed,
                    current_label=str(label),
                    unrestricted=unrestricted,
                    limited=limited,
                    frozen=frozen,
                    unknown=unknown,
                    deleted_broken=len(deleted_broken),
                    deleted_banned=len(deleted_banned) + len(deleted_frozen),
                    kept_other_errors=len(kept_other_errors),
                    parallel=parallel,
                ),
                InlineKeyboardMarkup([
                    [
                        premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
                        premium_button("账号列表", self.settings.emoji_list_id, callback_data="account:list:1"),
                    ],
                ]),
            )

        export_warnings: list[str] = []
        if removed_invalid_rows:
            try:
                await self._send_account_export(query.message.chat_id, removed_invalid_rows, "invalid", auto_delete=True)
            except FileNotFoundError:
                for row in removed_invalid_rows:
                    self._purge_account_files(row)
                export_warnings.append("失效/封禁账号没有找到可导出的原始文件")
        if removed_frozen_rows:
            try:
                await self._send_account_export(query.message.chat_id, removed_frozen_rows, "frozen", auto_delete=True)
            except FileNotFoundError:
                for row in removed_frozen_rows:
                    self._purge_account_files(row)
                export_warnings.append("冻结账号没有找到可导出的原始文件")

        total_alive = self.db.count_accounts()
        lines = [
            f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>批量检查状态完成</b>",
            f"总检测账号：<code>{total_checked}</code>",
            f"无限制：<code>{unrestricted}</code>",
            f"受限：<code>{limited}</code>",
            f"冻结：<code>{frozen}</code>",
            f"待人工确认：<code>{unknown}</code>",
            f"自动删除损坏 session：<code>{len(deleted_broken)}</code>",
            f"自动删除封禁/失效：<code>{len(deleted_banned)}</code>",
            f"自动删除冻结：<code>{len(deleted_frozen)}</code>",
            f"当前列表保留：<code>{total_alive}</code>",
        ]
        if total_alive == 0:
            lines.append("\n<b>当前账号列表已清空，没有可用账号。</b>")
        if deleted_broken:
            lines.append("")
            lines.append("<b>已删除：session 已损坏</b>")
            for item in deleted_broken[:8]:
                lines.append(f"• {html.escape(item, quote=False)}")
        if deleted_banned:
            lines.append("")
            lines.append("<b>已删除：封禁 / 失效</b>")
            for item in deleted_banned[:8]:
                lines.append(f"• {html.escape(item, quote=False)}")
        if deleted_frozen:
            lines.append("")
            lines.append("<b>已删除：冻结</b>")
            for item in deleted_frozen[:8]:
                lines.append(f"• {html.escape(item, quote=False)}")
        if kept_other_errors:
            lines.append("")
            lines.append("<b>暂未删除：其他异常</b>")
            for item in kept_other_errors[:8]:
                lines.append(f"• {html.escape(item, quote=False)}")
        if export_warnings:
            lines.append("")
            lines.append("<b>导出提示</b>")
            for item in export_warnings:
                lines.append(f"• {html.escape(item, quote=False)}")
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup([
            [
                premium_button("查看账号列表", self.settings.emoji_list_id, callback_data="account:list:1"),
                premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
            ],
        ]))

    async def _purge_invalid_accounts(self, query) -> None:
        rows = self.db.list_invalid_accounts()
        if not rows:
            text = (
                f"{tg_emoji(self.settings.emoji_success_id, '🆗')} <b>没有可清理的无效账号</b>\n"
                f"当前列表里只剩存活账号。"
            )
            await self._safe_edit(query, text, InlineKeyboardMarkup([
                [
                    premium_button("查看账号列表", self.settings.emoji_list_id, callback_data="account:list:1"),
                    premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
                ],
            ]))
            return

        deleted_labels: list[str] = []
        for row in rows:
            label = row["username"] or row["phone"] or row["display_name"] or row["session_name"]
            deleted_labels.append(str(label))
            self._purge_account_files(row)
            self.db.delete_account(row["id"])

        lines = [
            f"{tg_emoji(self.settings.emoji_error_id, '❌')} <b>无效账号已清理</b>",
            f"已删除数量：<code>{len(deleted_labels)}</code>",
            f"当前保留存活：<code>{self.db.count_accounts()}</code>",
        ]
        if deleted_labels:
            lines.append("")
            lines.append("<b>本次已删除</b>")
            for item in deleted_labels[:10]:
                lines.append(f"• {html.escape(item, quote=False)}")
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup([
            [
                premium_button("查看账号列表", self.settings.emoji_list_id, callback_data="account:list:1"),
                premium_button("返回账号管理", self.settings.emoji_back_id, callback_data="menu:accounts"),
            ],
        ]))

    async def _delete_account(self, query, account_id: int) -> None:
        row = self.db.delete_account(account_id)
        if row:
            self._purge_account_files(row)
        await self._show_account_list(query, page=1)

    async def _show_collect_menu(self, query) -> None:
        text = (
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>采集用户</b>\n"
            f"当前支持：频道采集、群组发言用户采集、失败结果导出。"
        )
        keyboard = [
            [
                premium_button("新建采集任务", self.settings.emoji_idea_id, callback_data="collect:new"),
                premium_button("任务列表", self.settings.emoji_history_id, callback_data="collect:tasks"),
            ],
            [
                premium_button("历史结果", self.settings.emoji_export_id, callback_data="menu:history"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ],
        ]
        await self._safe_edit(query, text, InlineKeyboardMarkup(keyboard))

    async def _show_collect_create_menu(self, query) -> None:
        text = (
            f"{tg_emoji(self.settings.emoji_idea_id, '💡')} <b>新建采集任务</b>\n"
            f"请选择这次要采集的来源类型。"
        )
        keyboard = [
            [
                premium_button("采集频道", self.settings.emoji_progress_id, callback_data="collect:new:channel"),
                premium_button("采集群组", self.settings.emoji_list_id, callback_data="collect:new:group"),
            ],
            [
                premium_button("返回采集用户", self.settings.emoji_back_id, callback_data="menu:collect"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ],
        ]
        await self._safe_edit(query, text, InlineKeyboardMarkup(keyboard))

    async def _show_dm_menu(self, query) -> None:
        text = (
            f"{tg_emoji(self.settings.emoji_start_id, '🎊')} <b>私信任务</b>\n"
            f"当前支持：文本私信、媒体私信、频道帖子转发（通过链接定位原帖）、txt/手输用户名单、单号上限、自动切号、实时成功失败统计。"
        )
        keyboard = [
            [
                premium_button("新建私信任务", self.settings.emoji_idea_id, callback_data="dm:new"),
                premium_button("任务列表", self.settings.emoji_history_id, callback_data="dm:tasks"),
            ],
            [
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
                premium_button("账号管理", self.settings.emoji_list_id, callback_data="menu:accounts"),
            ],
        ]
        await self._safe_edit(query, text, InlineKeyboardMarkup(keyboard))

    async def _start_dm_wizard(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        if not active_accounts:
            await self._safe_edit(
                query,
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前没有可用账号，请先上传并验证 session。",
                self._single_back_keyboard("menu:accounts"),
            )
            return
        self.user_states[user_id] = {
            "mode": "await_dm_targets",
            "draft": {
                "message_mode": "single",
                "content_type": "text",
                "worker_count": 1,
                "account_ids": [],
                "account_page": 1,
                "policy": {
                    "per_account_success_limit": 40,
                    "delay_min": 8,
                    "delay_max": 15,
                    "stage1_delay_seconds": 5,
                    "stage2_delay_seconds": 3,
                    "pin_after_send": False,
                    "pin_delay_seconds": 3,
                    "auto_switch_account": True,
                    "auto_stop_when_accounts_exhausted": True,
                    "typing_simulation": True,
                    "max_retries": 3,
                    "stop_account_after_user_frequent": 30,
                },
            },
        }
        await self._safe_edit(query, self._dm_targets_prompt_text(), self._single_back_keyboard("dm:wizard:cancel"))

    async def _dm_wizard_auto_accounts(self, query, user_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        draft["account_ids"] = [int(row["id"]) for row in self.db.get_active_accounts()]
        self._sync_dm_worker_count(draft)
        await self._render_dm_account_selection(query, draft)

    async def _dm_wizard_change_account_page(self, query, user_id: int, page: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        draft["account_page"] = max(1, page)
        await self._render_dm_account_selection(query, draft)

    async def _dm_wizard_select_current_page(self, query, user_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        selected_ids = set(int(item) for item in (draft.get("account_ids") or []))
        rows, _, _ = self._get_dm_account_page_rows(draft)
        for row in rows:
            selected_ids.add(int(row["id"]))
        draft["account_ids"] = sorted(selected_ids)
        self._sync_dm_worker_count(draft)
        await self._render_dm_account_selection(query, draft)

    async def _dm_wizard_toggle_account(self, query, user_id: int, account_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        selected_ids = list(draft.get("account_ids") or [])
        if account_id in selected_ids:
            selected_ids.remove(account_id)
        else:
            selected_ids.append(account_id)
        draft["account_ids"] = selected_ids
        self._sync_dm_worker_count(draft)
        await self._render_dm_account_selection(query, draft)

    async def _dm_wizard_finish_accounts(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        if not draft.get("account_ids"):
            await query.answer("至少选择一个账号", show_alert=True)
            return
        self._sync_dm_worker_count(draft)
        state["mode"] = "dm_config"
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_back_to_input(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        if draft.get("message_mode") == "three_stage":
            state["mode"] = "await_dm_closing"
            await self._safe_edit(query, self._dm_closing_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:body"))
            return
        content_type = draft.get("content_type") or "text"
        if content_type == "media":
            state["mode"] = "await_dm_media"
        elif content_type == "forward":
            state["mode"] = "await_dm_forward"
        else:
            state["mode"] = "await_dm_message"
        await self._safe_edit(query, self._dm_message_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:config"))

    async def _dm_wizard_toggle_mode(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        draft["message_mode"] = "three_stage" if draft.get("message_mode") == "single" else "single"
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_content_type(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        options = ["text", "post", "media", "forward"]
        current = str(draft.get("content_type") or "text")
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else "text"
        draft["content_type"] = next_value
        for key in ("text", "body", "post_code", "media_kind", "media_path", "media_file_name", "media_caption", "forward_link", "forward_preview", "forward_message_preview", "forward_preview_error"):
            draft.pop(key, None)
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_limit(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        options = [20, 40, 60, 80]
        current = int(policy.get("per_account_success_limit") or 40)
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else 40
        policy["per_account_success_limit"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_delay(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        options = [(5, 10), (8, 15), (15, 30), (30, 45)]
        current = (int(policy.get("delay_min") or 8), int(policy.get("delay_max") or 15))
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else (8, 15)
        policy["delay_min"], policy["delay_max"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_worker_count(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        current = self._sync_dm_worker_count(draft)
        options = [value for value in (1, 2, 3, 5, 10, 20, 30, 40, 50) if value <= self._dm_max_worker_count(draft)]
        if not options:
            options = [1]
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else options[0]
        draft["worker_count"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_stage1_delay(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        options = [0, 3, 5, 8, 10]
        current = int(policy.get("stage1_delay_seconds") or 5)
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else 5
        policy["stage1_delay_seconds"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_stage2_delay(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        options = [0, 3, 5, 8, 10]
        current = int(policy.get("stage2_delay_seconds") or 3)
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else 3
        policy["stage2_delay_seconds"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_toggle_pin(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        policy["pin_after_send"] = not bool(policy.get("pin_after_send", False))
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_cycle_pin_delay(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        options = [0, 3, 5, 8, 10]
        current = int(policy.get("pin_delay_seconds") or 3)
        next_value = options[(options.index(current) + 1) % len(options)] if current in options else 3
        policy["pin_delay_seconds"] = next_value
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_toggle_typing(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        policy["typing_simulation"] = not bool(policy.get("typing_simulation", True))
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_toggle_switch(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        policy = draft.setdefault("policy", {})
        current = bool(policy.get("auto_switch_account", True))
        policy["auto_switch_account"] = not current
        policy["auto_stop_when_accounts_exhausted"] = not current
        await self._safe_edit(query, self._dm_config_text(draft), self._build_dm_config_keyboard(draft))

    async def _dm_wizard_finish_config(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        if draft.get("message_mode") == "three_stage":
            state["mode"] = "await_dm_greeting"
            await self._safe_edit(query, self._dm_message_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:config"))
            return
        content_type = draft.get("content_type") or "text"
        if content_type == "media":
            state["mode"] = "await_dm_media"
        elif content_type == "forward":
            state["mode"] = "await_dm_forward"
        else:
            state["mode"] = "await_dm_message"
        await self._safe_edit(query, self._dm_message_prompt_text(draft), self._single_back_keyboard("dm:wizard:back:config"))

    async def _dm_wizard_start_task(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        raw_targets = draft.get("targets") or []
        mode = draft.get("message_mode") or "single"
        content_type = draft.get("content_type") or "text"
        if content_type == "media":
            has_content = bool(draft.get("media_path"))
        elif content_type == "post":
            has_content = bool((draft.get("post_code") or draft.get("body") or draft.get("text") or "").strip())
        elif content_type == "forward":
            has_content = bool(draft.get("forward_link"))
        else:
            has_content = bool((draft.get("text") or "").strip()) if mode == "single" else bool((draft.get("body") or "").strip())
        if not raw_targets or not draft.get("account_ids") or not has_content:
            await query.answer("任务信息不完整", show_alert=True)
            return

        targets = [ParsedTarget(**item) for item in raw_targets]
        recipient_ids = self.dm_repository.create_or_get_recipients(targets)
        policy = draft.get("policy") or {}
        worker_count = self._sync_dm_worker_count(draft)
        payload = self._build_dm_payload_from_draft(draft)
        task = self.dm_repository.create_dm_task(
            requester_id=user_id,
            account_ids=[int(item) for item in draft.get("account_ids") or []],
            worker_count=worker_count,
            message_mode=mode,
            content_type=content_type,
            payload=payload,
            policy=policy,
        )
        self.dm_repository.attach_task_recipients(int(task["id"]), recipient_ids)
        sent = await self.application.bot.send_message(
            chat_id=query.message.chat_id,
            text=self._format_dm_task_text(int(task["id"])),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._build_dm_task_keyboard(int(task["id"])),
        )
        self.dm_repository.set_dm_task_progress_message(int(task["id"]), sent.chat_id, sent.message_id)
        self._clear_state(user_id)
        runner = asyncio.create_task(self.dm_sender.run_task(int(task["id"])))
        self.dm_task_runners[int(task["id"])] = runner

        def _cleanup_dm_runner(_: asyncio.Task, task_id: int = int(task["id"])) -> None:
            self.dm_task_runners.pop(task_id, None)

        runner.add_done_callback(_cleanup_dm_runner)
        if query.message:
            await query.message.delete()

    def _build_dm_payload_from_draft(self, draft: dict) -> dict:
        mode = str(draft.get("message_mode") or "single")
        content_type = str(draft.get("content_type") or "text")
        payload = {"mode": mode, "content_type": content_type}
        if mode == "three_stage":
            payload["greeting"] = draft.get("greeting") or ""
            payload["closing"] = draft.get("closing") or ""
        if content_type == "media":
            payload.update({
                "media_kind": draft.get("media_kind") or "file",
                "media_path": draft.get("media_path") or "",
                "file_name": draft.get("media_file_name") or "",
                "caption": draft.get("media_caption") or "",
            })
        elif content_type == "post":
            payload["post_code"] = draft.get("post_code") or draft.get("body") or draft.get("text") or ""
        elif content_type == "forward":
            payload.update({
                "forward_link": draft.get("forward_link") or "",
                "forward_preview": draft.get("forward_preview") or "",
                "forward_message_preview": draft.get("forward_message_preview") or "",
                "forward_preview_error": draft.get("forward_preview_error") or "",
            })
        elif mode == "three_stage":
            payload["body"] = draft.get("body") or ""
        else:
            payload["text"] = draft.get("text") or ""
        return payload

    async def _dm_wizard_preview(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        payload = self._build_dm_payload_from_draft(draft)
        chat_id = query.message.chat_id if query.message else None
        if not chat_id:
            return
        try:
            await self._send_dm_preview(chat_id, payload, draft.get("content_type") or "text", draft=draft)
        except Exception as exc:  # noqa: BLE001
            logger.exception("私信预览失败")
            await query.answer(f"预览失败：{self._humanize_dm_error(None, str(exc))[:120]}", show_alert=True)

    async def _send_dm_preview(self, chat_id: int, payload: dict, content_type: str, *, draft: dict | None = None) -> None:
        mode = str(payload.get("mode") or "single")
        if mode == "three_stage" and str(payload.get("greeting") or "").strip():
            await self.application.bot.send_message(chat_id=chat_id, text=str(payload.get("greeting") or ""))
        if content_type == "media":
            media_path = Path(str(payload.get("media_path") or ""))
            if media_path.exists():
                with media_path.open("rb") as fp:
                    caption = str(payload.get("caption") or "") or None
                    media_kind = self._detect_dm_preview_media_kind(media_path, str(payload.get("media_kind") or "document"))
                    if media_kind == "photo":
                        await self.application.bot.send_photo(chat_id=chat_id, photo=fp, caption=caption)
                    elif media_kind == "video":
                        await self.application.bot.send_video(chat_id=chat_id, video=fp, caption=caption)
                    else:
                        await self.application.bot.send_document(
                            chat_id=chat_id,
                            document=fp,
                            filename=media_path.name,
                            caption=caption,
                        )
            else:
                await self.application.bot.send_message(chat_id=chat_id, text="【预览失败】媒体文件不存在")
        elif content_type == "post":
            post_code = str(payload.get("body") or payload.get("post_code") or payload.get("text") or "").strip()
            if post_code:
                await self._send_postbot_preview(chat_id, post_code, draft=draft)
        elif content_type == "forward":
            preview_text = str(payload.get("forward_message_preview") or "").strip() or str(payload.get("forward_preview") or "").strip() or "暂时无法抓到帖子摘要"
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=f"【频道帖子预览】\n{preview_text}",
                disable_web_page_preview=True,
            )
        else:
            main_text = str(payload.get("body") or payload.get("text") or "").strip()
            if main_text:
                await self.application.bot.send_message(chat_id=chat_id, text=main_text)
        if mode == "three_stage" and str(payload.get("closing") or "").strip():
            await self.application.bot.send_message(chat_id=chat_id, text=str(payload.get("closing") or ""))

    async def _build_postbot_preview_text(self, post_code: str, *, draft: dict | None = None) -> str:
        draft = draft or {}
        account_ids = [int(item) for item in (draft.get("account_ids") or []) if str(item).strip()]
        if not account_ids:
            return "【PostBot 预览】\n已保存文案代码。实际发送时会按 PostBot 返回的内联内容发出，不会把代码原样发出去。"

        account = self.db.get_account(account_ids[0])
        if not account:
            return "【PostBot 预览】\n未找到可用于预览的账号。实际发送时会按 PostBot 返回的内联内容发出。"

        client = None
        try:
            client = self.collection_manager._build_client(Path(str(account["session_file"])))
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("预览账号 session 未登录")
            _, inline_result = await fetch_postbot_inline_result(client, post_code)
            return "【PostBot 预览】\n" + describe_postbot_inline_result(inline_result)
        finally:
            if client is not None:
                await client.disconnect()

    async def _send_postbot_preview(self, chat_id: int, post_code: str, *, draft: dict | None = None) -> None:
        draft = draft or {}
        account_ids = [int(item) for item in (draft.get("account_ids") or []) if str(item).strip()]
        if not account_ids:
            preview_text = await self._build_postbot_preview_text(post_code, draft=draft)
            await self.application.bot.send_message(chat_id=chat_id, text=preview_text)
            return

        account = self.db.get_account(account_ids[0])
        if not account:
            preview_text = await self._build_postbot_preview_text(post_code, draft=draft)
            await self.application.bot.send_message(chat_id=chat_id, text=preview_text)
            return

        client = None
        preview_message = None
        try:
            client = self.collection_manager._build_client(Path(str(account["session_file"])))
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("预览账号 session 未登录")

            _, inline_result = await fetch_postbot_inline_result(client, post_code)
            preview_message = await inline_result.click("me")
            preview_message = self._normalize_preview_message(preview_message)
            if preview_message is None:
                raise RuntimeError("PostBot 预览消息生成失败")
            await self._relay_preview_message_to_chat(chat_id, client, preview_message)
        finally:
            if client is not None:
                if preview_message is not None:
                    try:
                        await client.delete_messages("me", [preview_message.id])
                    except Exception:
                        logger.debug("删除 PostBot 预览缓存消息失败", exc_info=True)
                await client.disconnect()

    async def _relay_preview_message_to_chat(self, chat_id: int, client, message) -> None:
        text = (getattr(message, "raw_text", None) or getattr(message, "message", None) or "").strip()
        reply_markup = self._build_postbot_preview_markup(message)
        if getattr(message, "media", None):
            mime_type = None
            media_kind = "document"
            if getattr(message, "photo", None):
                media_kind = "photo"
            elif getattr(message, "video", None):
                media_kind = "video"
                mime_type = getattr(message.video, "mime_type", None)
            elif getattr(message, "document", None):
                mime_type = getattr(message.document, "mime_type", None)

            with tempfile.TemporaryDirectory(prefix="postbot_preview_") as tmp_dir:
                downloaded = await client.download_media(message, file=tmp_dir)
                if downloaded:
                    media_path = Path(str(downloaded))
                    resolved_kind = self._detect_dm_preview_media_kind(media_path, media_kind, mime_type)
                    with media_path.open("rb") as fp:
                        if resolved_kind == "photo":
                            await self.application.bot.send_photo(
                                chat_id=chat_id,
                                photo=fp,
                                caption=text or None,
                                reply_markup=reply_markup,
                            )
                            return
                        if resolved_kind == "video":
                            await self.application.bot.send_video(
                                chat_id=chat_id,
                                video=fp,
                                caption=text or None,
                                reply_markup=reply_markup,
                            )
                            return
                        await self.application.bot.send_document(
                            chat_id=chat_id,
                            document=fp,
                            filename=media_path.name,
                            caption=text or None,
                            reply_markup=reply_markup,
                        )
                        return
        if text:
            await self.application.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            return
        await self.application.bot.send_message(chat_id=chat_id, text="【预览失败】PostBot 没有返回可展示的消息内容")

    @staticmethod
    def _normalize_preview_message(result):
        if isinstance(result, list):
            return result[-1] if result else None
        return result

    @staticmethod
    def _build_postbot_preview_markup(message) -> InlineKeyboardMarkup | None:
        rows = getattr(message, "buttons", None) or []
        keyboard: list[list[InlineKeyboardButton]] = []
        for row in rows:
            button_row: list[InlineKeyboardButton] = []
            for button in row or []:
                text = str(getattr(button, "text", "") or "").strip() or "按钮"
                url = getattr(button, "url", None)
                if url:
                    button_row.append(InlineKeyboardButton(text=text, url=str(url)))
                    continue
                button_row.append(InlineKeyboardButton(text=text, callback_data="preview:noop"))
            if button_row:
                keyboard.append(button_row)
        return InlineKeyboardMarkup(keyboard) if keyboard else None

    @staticmethod
    def _detect_dm_preview_media_kind(path: Path, media_kind: str | None, mime_type: str | None = None) -> str:
        normalized = str(media_kind or "document").lower()
        if normalized in {"photo", "video"}:
            return normalized
        mime = str(mime_type or "").lower()
        suffix = path.suffix.lower()
        if mime.startswith("image/") or suffix in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
            return "photo"
        if mime.startswith("video/") or suffix in {".mp4", ".mov", ".mkv", ".webm"}:
            return "video"
        return "document"

    async def _show_dm_task_list(self, query, page: int = 1) -> None:
        per_page = 6
        total = self.dm_repository.count_dm_tasks()
        total_pages = max(1, ceil(total / per_page))
        page = max(1, min(page, total_pages))
        tasks = self.dm_repository.list_dm_tasks(limit=per_page, offset=(page - 1) * per_page)
        lines = [
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>私信任务列表</b>",
            f"页码：<code>{page}/{total_pages}</code>",
            f"任务总数：<code>{total}</code>",
        ]
        if not tasks:
            lines.append("\n还没有私信任务。")
        else:
            for task in tasks:
                lines.append(
                    f"\n• 私信任务 #{task['id']} · {self._dm_status_badge(task['status'])} · 成功 <code>{task['success_count']}</code> · 失败 <code>{task['failed_count']}</code> / 总 <code>{task['total_targets']}</code>"
                )
        keyboard = []
        row_buffer = []
        for task in tasks:
            row_buffer.append(premium_button(f"查看任务 #{task['id']}", self.settings.emoji_history_id, callback_data=f"dm:view:{task['id']}:{page}"))
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        nav = []
        if page > 1:
            nav.append(premium_button("上一页", self.settings.emoji_back_id, callback_data=f"dm:tasks:{page - 1}"))
        if page < total_pages:
            nav.append(premium_button("下一页", self.settings.emoji_next_id, callback_data=f"dm:tasks:{page + 1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([
            premium_button("一键清空任务", self.settings.emoji_error_id, callback_data="dm:tasks:clear"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data=f"dm:tasks:{page}"),
        ])
        keyboard.append([
            premium_button("返回私信任务", self.settings.emoji_back_id, callback_data="menu:dm"),
            premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _confirm_clear_dm_tasks(self, query) -> None:
        total = self.dm_repository.count_dm_tasks()
        text = (
            f"{tg_emoji(self.settings.emoji_error_id, '❌')} <b>确认清空私信任务</b>\n"
            f"当前列表任务：<code>{total}</code>\n"
            f"说明：会清空 <code>已完成 / 已停止 / 异常</code> 的任务，运行中的任务会保留。"
        )
        keyboard = InlineKeyboardMarkup([
            [
                premium_button("确认清空", self.settings.emoji_error_id, callback_data="dm:tasks:clear:confirm"),
                premium_button("返回列表", self.settings.emoji_back_id, callback_data="dm:tasks:1"),
            ]
        ])
        await self._safe_edit(query, text, keyboard)

    async def _clear_dm_tasks(self, query) -> None:
        cleared = self.dm_repository.clear_dm_finished_tasks()
        await query.answer(f"已清空 {cleared} 个已结束任务", show_alert=False)
        await self._show_dm_task_list(query, page=1)

    async def _show_dm_task_detail(self, query, task_id: int, page: int = 1) -> None:
        await self._safe_edit(query, self._format_dm_task_text(task_id), self._build_dm_task_keyboard(task_id, page=page))

    async def _render_dm_account_selection(self, query, draft: dict) -> None:
        await self._safe_edit(query, self._dm_select_accounts_text(draft), self._build_dm_account_selection_keyboard(draft))

    async def _stop_dm_task(self, query, task_id: int, page: int = 1) -> None:
        self.dm_repository.request_dm_task_stop(task_id)
        runner = self.dm_task_runners.get(task_id)
        if runner and not runner.done():
            runner.cancel()
            try:
                await runner
            except asyncio.CancelledError:
                pass
        self.dm_repository.mark_dm_task_status(task_id, "stopped", last_error="管理员手动停止任务")
        await self._show_dm_task_detail(query, task_id, page=page)

    async def _show_task_list(self, query, page: int = 1) -> None:
        per_page = 6
        total = self.db.count_collect_tasks()
        total_pages = max(1, ceil(total / per_page))
        page = max(1, min(page, total_pages))
        tasks = self.db.list_collect_tasks(limit=per_page, offset=(page - 1) * per_page)
        history_total = self.db.count_collect_tasks(history=True)
        lines = [
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>任务列表</b>",
            f"页码：<code>{page}/{total_pages}</code>",
            f"任务总数：<code>{total}</code> · 历史任务：<code>{history_total}</code>",
        ]
        if not tasks:
            lines.append("\n还没有采集任务。")
        else:
            for task in tasks:
                lines.append(
                    f"\n• 任务 #{self._task_display_code(task)} · {status_badge(task['status'])} · 频道 <code>{task['finished_channels']}/{task['total_channels']}</code> · 去重 <code>{task['unique_hits']}</code>"
                )
        keyboard = []
        row_buffer = []
        for task in tasks:
            row_buffer.append(
                premium_button(f"查看任务 #{self._task_display_code(task)}", self.settings.emoji_history_id, callback_data=f"task:view:{task['id']}:{page}:tasks")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        nav = []
        if page > 1:
            nav.append(premium_button("上一页", self.settings.emoji_back_id, callback_data=f"collect:tasks:{page - 1}"))
        if page < total_pages:
            nav.append(premium_button("下一页", self.settings.emoji_next_id, callback_data=f"collect:tasks:{page + 1}"))
        if nav:
            keyboard.append(nav)
        if history_total > 0:
            keyboard.append([
                premium_button("一键清空任务历史", self.settings.emoji_error_id, callback_data="task:clear_history"),
                premium_button("历史结果", self.settings.emoji_history_id, callback_data="menu:history:1"),
            ])
        keyboard.append([
            premium_button("返回采集用户", self.settings.emoji_back_id, callback_data="menu:collect"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data=f"collect:tasks:{page}"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _show_task_detail(self, query, task_id: int, page: int = 1, source: str = "tasks", force: bool = False) -> None:
        text = self._format_task_text(task_id)
        await self._safe_edit(query, text, self._build_task_keyboard(task_id, page=page, source=source))

    async def _show_history(self, query, page: int = 1) -> None:
        per_page = 6
        total = self.db.count_collect_tasks(history=True)
        total_pages = max(1, ceil(total / per_page))
        page = max(1, min(page, total_pages))
        tasks = self.db.list_history_tasks(limit=per_page, offset=(page - 1) * per_page)
        lines = [
            f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>历史结果</b>",
            f"页码：<code>{page}/{total_pages}</code>",
            f"历史总数：<code>{total}</code>",
        ]
        if not tasks:
            lines.append("\n还没有已完成/已停止的任务。")
        else:
            for task in tasks:
                lines.append(
                    f"\n• 任务 #{self._task_display_code(task)} · {status_badge(task['status'])} · 去重 <code>{task['unique_hits']}</code>"
                )
        keyboard = []
        row_buffer = []
        for task in tasks:
            row_buffer.append(
                premium_button(f"查看任务 #{self._task_display_code(task)}", self.settings.emoji_history_id, callback_data=f"task:view:{task['id']}:{page}:history")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        nav = []
        if page > 1:
            nav.append(premium_button("上一页", self.settings.emoji_back_id, callback_data=f"menu:history:{page - 1}"))
        if page < total_pages:
            nav.append(premium_button("下一页", self.settings.emoji_next_id, callback_data=f"menu:history:{page + 1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([
            premium_button("返回采集用户", self.settings.emoji_back_id, callback_data="menu:collect"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data=f"menu:history:{page}"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _delete_task(self, query, task_id: int, page: int = 1, source: str = "tasks") -> None:
        task = self.db.get_collect_task(task_id)
        if not task:
            if source == "history":
                await self._show_history(query, page=page)
            else:
                await self._show_task_list(query, page=page)
            return
        if task["status"] in {"queued", "running"}:
            self.db.stop_collect_task_now(task_id, reason="管理员删除任务，账号已释放")
            runner = self.task_runners.get(task_id)
            if runner and not runner.done():
                runner.cancel()
                try:
                    await runner
                except asyncio.CancelledError:
                    pass
        deleted = self.db.delete_collect_task(task_id)
        if deleted:
            self._cleanup_task_export_files(deleted)
        total_after = self.db.count_collect_tasks(history=(source == 'history'))
        per_page = 6
        total_pages_after = max(1, ceil(total_after / per_page))
        target_page = max(1, min(page, total_pages_after))
        if source == "history":
            await self._show_history(query, page=target_page)
        else:
            await self._show_task_list(query, page=target_page)

    async def _clear_task_history(self, query) -> None:
        rows = self.db.delete_history_tasks()
        if not rows:
            await self._show_task_list(query, page=1)
            return
        for row in rows:
            self._cleanup_task_export_files(row)
        await self._show_task_list(query, page=1)

    async def _start_collect_wizard(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        if not active_accounts:
            await self._safe_edit(
                query,
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前没有可用账号，请先上传并验证 session。",
                self._single_back_keyboard("menu:accounts"),
            )
            return
        self.user_states[user_id] = {"mode": "await_channels", "draft": {"task_type": "channel"}}
        await self._safe_edit(query, self._channels_prompt_text(), self._single_back_keyboard("wizard:cancel"))

    async def _start_group_collect_wizard(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        if not active_accounts:
            await self._safe_edit(
                query,
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前没有可用账号，请先上传并验证 session。",
                self._single_back_keyboard("menu:accounts"),
            )
            return
        self.user_states[user_id] = {
            "mode": "await_group_targets",
            "draft": {
                "task_type": "group",
                "filters": {
                    "bot_mode": "non_bot_only",
                    "admin_mode": "non_admin_only",
                    "photo_mode": "has_photo_only",
                    "username_mode": "has_username_only",
                    "premium_mode": "premium_only",
                },
            },
        }
        await self._safe_edit(query, self._group_targets_prompt_text(), self._single_back_keyboard("wizard:cancel"))

    async def _wizard_set_days(self, query, user_id: int, days: int, reply_message=None) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        draft["days"] = days
        draft.setdefault("account_ids", [])
        if draft.get("task_type") == "group":
            state["mode"] = "select_group_filters"
            text = self._group_filters_text(draft)
            markup = self._build_group_filters_keyboard(draft)
        else:
            state["mode"] = "select_accounts"
            text = self._select_accounts_text(draft["channels"], days, draft["account_ids"], task_type=draft.get("task_type", "channel"), filters=draft.get("filters"))
            markup = self._build_account_selection_keyboard(draft["account_ids"])
        if query is not None:
            await self._safe_edit(query, text, markup)
        elif reply_message is not None:
            await reply_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

    async def _toggle_group_filter(self, query, user_id: int, key: str) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        filters_map = draft.setdefault("filters", {})
        mode_defaults = {
            "bot_mode": "non_bot_only",
            "admin_mode": "non_admin_only",
            "photo_mode": "has_photo_only",
            "username_mode": "has_username_only",
            "premium_mode": "premium_only",
        }
        if key in mode_defaults:
            current = str(filters_map.get(key) or mode_defaults[key])
            next_value = {
                "non_bot_only": "bot_only",
                "bot_only": "non_bot_only",
                "non_admin_only": "admin_only",
                "admin_only": "non_admin_only",
                "has_photo_only": "no_photo_only",
                "no_photo_only": "has_photo_only",
                "has_username_only": "no_username_only",
                "no_username_only": "has_username_only",
                "premium_only": "non_premium_only",
                "non_premium_only": "premium_only",
            }.get(current, mode_defaults[key])
            filters_map[key] = next_value
        else:
            filters_map[key] = not bool(filters_map.get(key))
        state["mode"] = "select_group_filters"
        await self._safe_edit(query, self._group_filters_text(draft), self._build_group_filters_keyboard(draft))

    async def _wizard_finish_group_filters(self, query, user_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        state["mode"] = "select_accounts"
        await self._safe_edit(
            query,
            self._select_accounts_text(
                draft.get("channels", []),
                int(draft.get("days") or 1),
                draft.get("account_ids") or [],
                task_type=draft.get("task_type", "channel"),
                filters=draft.get("filters"),
            ),
            self._build_account_selection_keyboard(draft.get("account_ids") or []),
        )

    async def _wizard_auto_accounts(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        draft["account_ids"] = [row["id"] for row in active_accounts]
        text = self._select_accounts_text(
            draft.get("channels", []),
            int(draft.get("days") or 1),
            draft["account_ids"],
            task_type=draft.get("task_type", "channel"),
            filters=draft.get("filters"),
        )
        await self._safe_edit(query, text, self._build_account_selection_keyboard(draft["account_ids"]))

    async def _wizard_toggle_account(self, query, user_id: int, account_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        selected = set(draft.get("account_ids") or [])
        if account_id in selected:
            selected.remove(account_id)
        else:
            selected.add(account_id)
        draft["account_ids"] = sorted(selected)
        text = self._select_accounts_text(
            draft.get("channels", []),
            draft.get("days", 1),
            draft["account_ids"],
            task_type=draft.get("task_type", "channel"),
            filters=draft.get("filters"),
        )
        await self._safe_edit(query, text, self._build_account_selection_keyboard(draft["account_ids"]))

    async def _wizard_finish_accounts(self, query, user_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        account_ids = draft.get("account_ids") or []
        if not account_ids:
            await self._safe_edit(
                query,
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 至少选择一个可用账号。",
                self._build_account_selection_keyboard(account_ids),
            )
            return
        state["mode"] = "select_workers"
        await self._safe_edit(query, self._select_workers_text(draft), self._build_workers_keyboard(draft))

    async def _wizard_set_workers(self, query, user_id: int, worker_count: int, reply_message=None) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        max_workers = self._max_worker_count(draft)
        draft["worker_count"] = max(1, min(worker_count, max_workers))
        state["mode"] = "confirm_task"
        text = self._collect_confirm_text(draft)
        markup = self._build_confirm_keyboard()
        if query is not None:
            await self._safe_edit(query, text, markup)
        elif reply_message is not None:
            await reply_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

    async def _wizard_start_task(self, query, user_id: int) -> None:
        state = self.user_states.get(user_id) or {}
        draft = state.get("draft") or {}
        channels = draft.get("channels") or []
        days = int(draft.get("days") or 1)
        account_ids = list(draft.get("account_ids") or [])
        worker_count = int(draft.get("worker_count") or 1)
        if not channels or not account_ids:
            await self._safe_edit(query, self._not_found_text("采集草稿不完整，请重新开始。"), self._single_back_keyboard("collect:new"))
            return

        task = self.db.create_collect_task(
            requester_id=user_id,
            channels=channels,
            days_limit=days,
            account_ids=account_ids,
            worker_count=worker_count,
            task_type=draft.get("task_type", "channel"),
            filters_json=json.dumps(draft.get("filters") or {}, ensure_ascii=False),
        )
        text = self._format_task_text(task["id"])
        markup = self._build_task_keyboard(task["id"])
        await self._safe_edit(query, text, markup)
        if query.message:
            self.db.set_collect_task_progress_message(task["id"], query.message.chat_id, query.message.message_id)
        self._clear_state(user_id)
        runner = asyncio.create_task(self.collection_manager.run_collect_task(task["id"]))
        self.task_runners[task["id"]] = runner
        watcher = asyncio.create_task(self._task_progress_heartbeat(task["id"]))
        self.task_watchers[task["id"]] = watcher

        def _cleanup_task_runtime(_: asyncio.Task, task_id: int = task["id"]) -> None:
            self.task_runners.pop(task_id, None)
            heartbeat = self.task_watchers.pop(task_id, None)
            if heartbeat and not heartbeat.done():
                heartbeat.cancel()

        runner.add_done_callback(_cleanup_task_runtime)

    # ---------- task events ----------
    async def _on_task_progress(self, task_id: int) -> None:
        task = self.db.get_collect_task(task_id)
        if not task:
            return
        now = time.time()
        snapshot = self.progress_snapshots.setdefault(
            task_id,
            {
                "last_scanned": 0,
                "last_hits": 0,
                "last_unique": 0,
                "last_finished": 0,
                "retry_after_until": 0.0,
            },
        )
        retry_after_until = float(snapshot.get("retry_after_until", 0.0) or 0.0)
        if now < retry_after_until:
            return

        scanned = int(task["total_messages_scanned"] or 0)
        hits = int(task["total_hits"] or 0)
        unique_hits = int(task["unique_hits"] or 0)
        finished_channels = int(task["finished_channels"] or 0)

        delta_scanned = scanned - int(snapshot.get("last_scanned", 0) or 0)
        delta_hits = hits - int(snapshot.get("last_hits", 0) or 0)
        delta_unique = unique_hits - int(snapshot.get("last_unique", 0) or 0)
        delta_finished = finished_channels - int(snapshot.get("last_finished", 0) or 0)

        min_interval = 15.0
        if delta_finished > 0:
            min_interval = 2.0
        elif delta_hits > 0 or delta_unique > 0:
            min_interval = 4.0
        elif delta_scanned >= 500:
            min_interval = 5.0
        elif delta_scanned >= 200:
            min_interval = 7.0
        elif delta_scanned >= 50:
            min_interval = 10.0

        last = self.progress_throttle.get(task_id, 0.0)
        if now - last < min_interval:
            return

        pushed = await self._push_task_update(task_id)
        if pushed:
            self.progress_throttle[task_id] = now
            snapshot["last_scanned"] = scanned
            snapshot["last_hits"] = hits
            snapshot["last_unique"] = unique_hits
            snapshot["last_finished"] = finished_channels

    async def _on_task_complete(self, task_id: int) -> None:
        await self._push_task_update(task_id, force=True)
        heartbeat = self.task_watchers.pop(task_id, None)
        if heartbeat and not heartbeat.done():
            heartbeat.cancel()
        self.progress_throttle.pop(task_id, None)
        self.progress_snapshots.pop(task_id, None)
        task = self.db.get_collect_task(task_id)
        if not task:
            return
        await self._send_task_result(task["requester_id"], task_id, announce=True)

    async def _on_dm_task_progress(self, task_id: int, force: bool = False) -> None:
        task = self.dm_repository.get_dm_task(task_id)
        if not task or not task["progress_chat_id"] or not task["progress_message_id"]:
            return
        now = time.time()
        if not force:
            last = float(self.dm_progress_throttle.get(task_id, 0.0) or 0.0)
            if now - last < 2.5:
                return
        self.dm_progress_throttle[task_id] = now
        try:
            await self.application.bot.edit_message_text(
                chat_id=task["progress_chat_id"],
                message_id=task["progress_message_id"],
                text=self._format_dm_task_text(task_id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._build_dm_task_keyboard(task_id),
            )
        except RetryAfter as exc:
            self.dm_progress_throttle[task_id] = time.time() + float(exc.retry_after or 1)
        except BadRequest as exc:
            if "Message is not modified" not in str(exc):
                logger.exception("刷新私信任务进度失败: task_id=%s", task_id)

    async def _on_dm_task_complete(self, task_id: int) -> None:
        await self._on_dm_task_progress(task_id, force=True)
        self.dm_progress_throttle.pop(task_id, None)
        task = self.dm_repository.get_dm_task(task_id)
        if not task:
            return
        await self._send_dm_task_result(task["requester_id"], task_id)

    async def _task_progress_heartbeat(self, task_id: int) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                task = self.db.get_collect_task(task_id)
                if not task or task["status"] not in {"queued", "running"}:
                    return
                await self._push_task_update(task_id)
        except asyncio.CancelledError:
            return

    async def _push_task_update(self, task_id: int, force: bool = False) -> bool:
        task = self.db.get_collect_task(task_id)
        if not task or not task["progress_chat_id"] or not task["progress_message_id"]:
            return False
        try:
            await self.application.bot.edit_message_text(
                chat_id=task["progress_chat_id"],
                message_id=task["progress_message_id"],
                text=self._format_task_text(task_id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._build_task_keyboard(task_id),
            )
            return True
        except RetryAfter as exc:
            retry_seconds = max(3.0, float(getattr(exc, "retry_after", 3) or 3))
            snapshot = self.progress_snapshots.setdefault(task_id, {})
            snapshot["retry_after_until"] = time.time() + retry_seconds + 1
            logger.warning("任务消息刷新过快，延后重试 task=%s retry_after=%s", task_id, retry_seconds)
            return False
        except BadRequest as exc:
            if "Message is not modified" not in str(exc):
                logger.warning("更新任务消息失败 task=%s: %s", task_id, exc)
                return False
            return True

    # ---------- formatting ----------
    def _build_welcome_text(self, user_id: int) -> str:
        lines = [
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>DM Collector Bot</b> <code>v{__version__}</code>",
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} {html.escape(self.settings.welcome_text, quote=False)}",
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} 当前已接入：账号上传 / 多频道采集 / 群组发言采集 / txt 去重导出。",
        ]
        if self._is_admin(user_id):
            lines.append(f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} 你是管理员，可直接用下面按钮进入账号管理和采集用户。")
        return "\n\n".join(lines)

    def _build_stats_text(self) -> str:
        stats = self.db.get_stats()
        return "\n".join(
            [
                f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>当前统计</b> <code>v{__version__}</code>",
                f"用户数：<code>{stats['users']}</code>",
                f"私信总数：<code>{stats['messages']}</code>",
                f"今日私信：<code>{stats['today_messages']}</code>",
                f"账号总数：<code>{stats['accounts']}</code>",
                f"可用账号：<code>{stats['active_accounts']}</code>",
                f"运行任务：<code>{stats['running_tasks']}</code>",
            ]
        )

    def _format_account_text(self, account) -> str:
        title = account["display_name"] or account["username"] or account["phone"] or account["session_name"]
        display_code = self._account_display_code(account)
        lines = [
            f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>账号详情</b>",
            f"编号：<code>#{display_code}</code>",
            f"名称：<code>{html.escape(str(title), quote=False)}</code>",
            f"运行：{status_badge(account['status'])}",
            f"私信状态：{restriction_badge(account['restriction_status'])}",
            f"用户名：<code>{html.escape(str(account['username'] or '-'), quote=False)}</code>",
            f"手机号：<code>{html.escape(str(account['phone'] or '-'), quote=False)}</code>",
            f"User ID：<code>{account['tg_user_id'] or '-'}</code>",
            f"最近检测：<code>{account['last_checked_at'] or '-'}</code>",
        ]
        if account["restriction_checked_at"]:
            lines.append(f"状态检测：<code>{account['restriction_checked_at']}</code>")
        if account["restriction_reason"]:
            lines.append(f"状态结果：<code>{html.escape(str(account['restriction_reason']), quote=False)}</code>")
        if account["last_error"]:
            friendly = self._humanize_account_issue(account["status"], account["last_error"])
            lines.append(f"结果：<code>{html.escape(friendly, quote=False)}</code>")
            if friendly != account["last_error"]:
                lines.append(f"原始错误：<code>{html.escape(str(account['last_error']), quote=False)}</code>")
        return "\n".join(lines)

    def _format_task_text(self, task_id: int) -> str:
        task = self.db.get_collect_task(task_id)
        if not task:
            return self._not_found_text("任务不存在或已被删除。")
        channels = self.db.list_collect_task_channels(task_id)
        visible_channels = [item for item in channels if item["status"] != "completed"]
        display_code = self._task_display_code(task)
        task_type = task["task_type"] or "channel"
        unit = "群组" if task_type == "group" else "频道"
        lines = [
            f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>任务 #{display_code}</b> <code>v{__version__}</code>",
            f"类型：<code>{'群组发言采集' if task_type == 'group' else '频道用户名采集'}</code>",
            f"状态：{status_badge(task['status'])}",
        ]
        runtime_text = self._format_runtime(task["started_at"], task["finished_at"])
        if runtime_text:
            lines.append(f"已运行：<code>{runtime_text}</code>")
        lines.extend([
            f"时间范围：最近 <code>{task['days_limit']}</code> 天",
            f"账号数：<code>{task['account_count']}</code> · 并发：<code>{task['worker_count']}</code>",
            f"{unit}进度：<code>{task['finished_channels']}/{task['total_channels']}</code>",
            f"扫描消息：<code>{task['total_messages_scanned']}</code>",
            f"命中总数：<code>{task['total_hits']}</code>",
            f"去重数量：<code>{task['unique_hits']}</code>",
        ])
        if task_type == "group":
            filters_text = self._format_filter_summary(self._parse_group_filters(task["filters_json"]), empty_label="不过滤")
            lines.append(f"筛选规则：<code>{html.escape(filters_text, quote=False)}</code>")
        if task["last_error"] and task["status"] in {"error", "stopped"}:
            lines.append(f"错误：<code>{html.escape(str(task['last_error']), quote=False)}</code>")
        if task["status"] == "stopped" and int(task["total_messages_scanned"] or 0) > 0:
            lines.append("提示：<code>已保留当前已采集结果，可直接点击“导出结果”</code>")
        if visible_channels:
            lines.append("")
            lines.append(f"<b>{unit}子任务</b>")
            for item in visible_channels[:6]:
                item_runtime = self._format_runtime(item["started_at"], item["finished_at"])
                runtime_suffix = f" · 已跑 <code>{item_runtime}</code>" if item["status"] == "running" and item_runtime else ""
                lines.append(
                    f"• {html.escape(item['channel'], quote=False)} · {status_badge(item['status'])} · 扫描 <code>{item['scanned_messages']}</code> · 去重 <code>{item['unique_hits']}</code>{runtime_suffix}"
                )
        return "\n".join(lines)

    @staticmethod
    def _format_runtime(started_at, finished_at=None) -> str:
        if not started_at:
            return ""
        try:
            start_dt = datetime.strptime(str(started_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return ""
        try:
            end_dt = datetime.strptime(str(finished_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) if finished_at else datetime.now(timezone.utc)
        except Exception:
            end_dt = datetime.now(timezone.utc)
        seconds = max(0, int((end_dt - start_dt).total_seconds()))
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours}时{minutes}分"
        if minutes > 0:
            return f"{minutes}分{secs}秒"
        return f"{secs}秒"

    def _upload_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>上传 session</b>\n"
            f"请直接发送一个 <code>.session</code> 文件，或发送包含 <code>.session + .json</code> 的 <code>.zip</code> 压缩包。\n\n"
            f"收到后会自动：\n"
            f"1. 保存/解压文件\n2. 验证是否已登录\n3. 写入账号列表"
        )

    def _dm_targets_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_list_id, '👤')} <b>新建私信任务</b>\n"
            f"请直接发送用户名单，或上传 <code>.txt</code> 文件。\n\n"
            f"支持格式：\n"
            f"<code>@username</code>\n<code>username</code>\n<code>https://t.me/username</code>\n<code>+1649494646</code>"
        )

    def _dm_select_accounts_text(self, draft: dict) -> str:
        rows, page, total_pages = self._get_dm_account_page_rows(draft)
        targets = draft.get("targets") or []
        invalid = draft.get("invalid_targets") or []
        duplicates = draft.get("duplicate_targets") or []
        lines = [
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>选择私信账号</b>",
            f"目标数：<code>{len(targets)}</code> · 重复行：<code>{len(duplicates)}</code> · 无效行：<code>{len(invalid)}</code>",
            f"已选账号：<code>{len(draft.get('account_ids') or [])}</code>",
            f"页码：<code>{page}/{total_pages}</code> · 本页：<code>{len(rows)}</code>",
            "",
        ]
        for row in rows:
            mark = "已选" if int(row["id"]) in (draft.get("account_ids") or []) else "未选"
            label = row["username"] or row["phone"] or row["session_name"]
            lines.append(
                f"• #{self._account_display_code(row)} {html.escape(str(label), quote=False)} · {mark} · {restriction_badge(row['restriction_status'])}"
            )
        if duplicates:
            sample = "、".join(html.escape(str(item), quote=False) for item in duplicates[:5])
            lines.extend(["", f"重复目标已自动去重：<code>{len(duplicates)}</code>"])
            if sample:
                suffix = " …" if len(duplicates) > 5 else ""
                lines.append(f"示例：<code>{sample}{suffix}</code>")
        return "\n".join(lines)

    def _get_dm_account_page_rows(self, draft: dict) -> tuple[list, int, int]:
        all_rows = self.db.get_active_accounts()
        per_page = 20
        total_pages = max(1, ceil(len(all_rows) / per_page))
        page = max(1, min(int(draft.get("account_page") or 1), total_pages))
        draft["account_page"] = page
        start = (page - 1) * per_page
        end = start + per_page
        return list(all_rows[start:end]), page, total_pages

    def _dm_message_prompt_text(self, draft: dict) -> str:
        content_type = draft.get("content_type") or "text"
        if draft.get("message_mode") == "three_stage":
            return (
                f"{tg_emoji(self.settings.emoji_idea_id, '💡')} <b>输入第 1 段问候语</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"三段式会按顺序发送：第 1 段问候 → 第 2 段主内容 → 第 3 段结束语。"
            )
        if content_type == "post":
            return (
                f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>发送 PostBot 图文代码</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请直接粘贴 PostBot 生成的文案代码，例如：<code>@postbot jsc1mpdn1gw3</code>\n我会按它生成的内联内容发送，不会把代码原样发出去。"
            )
        if content_type == "media":
            return (
                f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>发送媒体内容</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请直接发送图片 / 视频 / 文件，可附带 caption。"
            )
        if content_type == "forward":
            return (
                f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>频道帖子转发</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请直接发送频道帖子链接，例如：<code>https://t.me/channelname/123</code>\n我会用这个链接定位原帖，并由私信账号把原帖内容转发出去。"
            )
        return (
            f"{tg_emoji(self.settings.emoji_idea_id, '💡')} <b>发送文本内容</b>\n"
            f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"请直接发送要私信的文本内容。"
        )

    def _dm_closing_prompt_text(self, draft: dict) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>输入第 3 段结束语</b>\n"
            f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"这段会在主内容发送后按配置延迟补发。"
        )

    def _dm_body_prompt_text(self, draft: dict) -> str:
        content_type = draft.get("content_type") or "text"
        if content_type == "post":
            return (
                f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>发送第 2 段主内容（PostBot 图文代码）</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请直接粘贴 PostBot 生成的文案代码，例如：<code>@postbot jsc1mpdn1gw3</code>。"
            )
        if content_type == "media":
            return (
                f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>发送第 2 段主内容（媒体）</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请直接发送图片 / 视频 / 文件，可附带 caption。"
            )
        if content_type == "forward":
            return (
                f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>发送第 2 段主内容（频道帖子）</b>\n"
                f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
                f"请发送频道帖子链接，例如：<code>https://t.me/channelname/123</code>。"
            )
        return (
            f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>输入第 2 段主消息</b>\n"
            f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"请发送第二段主消息内容。"
        )

    def _dm_config_text(self, draft: dict) -> str:
        worker_count = self._sync_dm_worker_count(draft)
        policy = draft.get("policy") or {}
        content_label = content_type_label(draft.get("content_type"))
        mode_label = message_mode_label(draft.get("message_mode"), content_type=draft.get("content_type"))
        switch_label = "自动切号" if policy.get("auto_switch_account", True) else "单号用完即停"
        typing_label = "开启" if policy.get("typing_simulation", True) else "关闭"
        delay_label = f"{int(policy.get('delay_min', 8))}-{int(policy.get('delay_max', 15))}秒"
        stage1_label = f"{int(policy.get('stage1_delay_seconds', 5))}秒"
        stage2_label = f"{int(policy.get('stage2_delay_seconds', 3))}秒"
        pin_label = "开启" if policy.get("pin_after_send", False) else "关闭"
        pin_delay_label = f"{int(policy.get('pin_delay_seconds', 3))}秒"
        lines = [
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>发送配置</b>",
            f"目标数：<code>{len(draft.get('targets') or [])}</code> · 账号数：<code>{len(draft.get('account_ids') or [])}</code>",
            f"内容类型：<code>{content_label}</code>",
            f"发送模式：<code>{mode_label}</code>",
            f"并发线程：<code>{worker_count}</code>",
            f"单号上限：<code>{int(policy.get('per_account_success_limit', 40))}</code>",
            f"随机间隔：<code>{delay_label}</code>",
            f"打字状态：<code>{typing_label}</code>",
            f"账号策略：<code>{switch_label}</code>",
            f"自动置顶：<code>{pin_label}</code> · 延迟：<code>{pin_delay_label}</code>",
        ]
        if draft.get("message_mode") == "three_stage":
            lines.append(f"三段间隔：<code>第1段后 {stage1_label} / 第2段后 {stage2_label}</code>")
        return "\n".join(lines)

    def _dm_confirm_text(self, draft: dict) -> str:
        worker_count = self._sync_dm_worker_count(draft)
        policy = draft.get("policy") or {}
        content_type = draft.get("content_type") or "text"
        mode_label = message_mode_label(draft.get("message_mode"), content_type=content_type)
        preview_error = str(draft.get("forward_preview_error") or "").strip()
        lines = [
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} <b>确认启动私信任务</b>",
            f"内容类型：<code>{content_type_label(content_type)}</code>",
            f"发送模式：<code>{mode_label}</code>",
            f"目标数：<code>{len(draft.get('targets') or [])}</code>",
            f"账号数：<code>{len(draft.get('account_ids') or [])}</code>",
            f"并发线程：<code>{worker_count}</code>",
            f"单号上限：<code>{policy.get('per_account_success_limit', 40)}</code>",
            f"随机间隔：<code>{policy.get('delay_min', 8)}-{policy.get('delay_max', 15)}秒</code>",
            f"打字状态：<code>{'开启' if policy.get('typing_simulation', True) else '关闭'}</code>",
            f"账号策略：<code>{'自动切号' if policy.get('auto_switch_account', True) else '单号用完即停'}</code>",
            f"自动置顶：<code>{'开启' if policy.get('pin_after_send', False) else '关闭'}</code> · 延迟：<code>{int(policy.get('pin_delay_seconds', 3))}秒</code>",
        ]
        if draft.get("message_mode") == "three_stage":
            lines.append(f"三段间隔：<code>第1段后 {int(policy.get('stage1_delay_seconds', 5))}秒 / 第2段后 {int(policy.get('stage2_delay_seconds', 3))}秒</code>")
        if content_type == "forward" and preview_error:
            lines.append(f"帖子预览状态：<code>抓取失败｜{html.escape(preview_error[:120], quote=False)}</code>")
        lines.extend(["", "需要看真实效果，请点【预览文案】。"])
        return "\n".join(lines)

    def _format_dm_task_text(self, task_id: int) -> str:
        task = self.dm_repository.get_dm_task(task_id)
        if not task:
            return self._not_found_text("私信任务不存在或已被删除。")
        accounts = self.dm_repository.list_dm_task_accounts(task_id)
        current = self.dm_repository.get_dm_task_current_recipient(task_id)
        recent_logs = self.dm_repository.list_dm_recent_logs(task_id, limit=5)
        failure_summary = self.dm_repository.get_dm_task_failure_summary(task_id, limit=6)
        processed = self.dm_repository.get_dm_task_processed_count(task_id)
        pending_count = max(0, int(task['total_targets'] or 0) - int(task['success_count'] or 0) - int(task['failed_count'] or 0) - int(task['skipped_count'] or 0))
        payload = json.loads(str(task["payload_json"] or "{}"))
        policy = json.loads(str(task["policy_json"] or "{}"))
        content_type = str(task["content_type"] or payload.get("content_type") or "text")
        body = payload_preview(payload, content_type=content_type, max_len=240)
        lines = [
            f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>私信任务 #{task['id']}</b> <code>v{__version__}</code>",
            f"状态：{self._dm_status_badge(task['status'])}",
            f"总目标：<code>{task['total_targets']}</code>",
            f"当前进度：<code>{processed}/{task['total_targets']}</code>",
        ]
        if int(task["success_count"] or 0) > 0:
            lines.append(f"成功：<code>{task['success_count']}</code>")
        if int(task["failed_count"] or 0) > 0:
            lines.append(f"失败：<code>{task['failed_count']}</code>")
        if int(task["skipped_count"] or 0) > 0:
            lines.append(f"跳过：<code>{task['skipped_count']}</code>")
        if pending_count > 0:
            lines.append(f"剩余待发送：<code>{pending_count}</code>")
        if int(task["active_accounts"] or 0) > 0:
            lines.append(f"运行账号：<code>{task['active_accounts']}</code>")
        lines.extend([
            f"创建时间：<code>{task['created_at'] or '-'}</code>",
            f"开始时间：<code>{task['started_at'] or '-'}</code>",
            f"并发线程：<code>{task['worker_count'] or 1}</code>",
            f"内容类型：<code>{content_type_label(content_type)}</code>",
            f"发送模式：<code>{message_mode_label(task['message_mode'], content_type=content_type)}</code>",
            f"发送配置：<code>上限 {policy.get('per_account_success_limit', 40)} / 间隔 {policy.get('delay_min', 8)}-{policy.get('delay_max', 15)}秒 / {'打字开' if policy.get('typing_simulation', True) else '打字关'}</code>",
        ])
        if draft_mode := policy.get("pin_after_send", False):
            lines.append(f"自动置顶：<code>{'开启' if draft_mode else '关闭'}</code> · 延迟 <code>{int(policy.get('pin_delay_seconds', 3))}秒</code>")
        if content_type == "forward" and payload.get("forward_preview_error"):
            lines.append(f"帖子预览：<code>抓取失败｜{html.escape(str(payload.get('forward_preview_error'))[:120], quote=False)}</code>")
        if task["last_error"]:
            lines.append(f"最近错误：<code>{html.escape(self._humanize_dm_error(None, str(task['last_error'])), quote=False)}</code>")
        if current:
            account_label = current["account_username"] or current["account_phone"] or current["account_display_name"] or f"#{current['assigned_account_id']}"
            lines.append(f"当前账号：<code>{html.escape(str(account_label), quote=False)}</code>")
            lines.append(f"当前用户：<code>{html.escape(str(current['normalized_input']), quote=False)}</code>")
        lines.append("")
        lines.append(f"内容概览：<code>{body}</code>")
        if accounts:
            lines.append("")
            lines.append("<b>账号统计</b>")
            for row in accounts[:6]:
                label = row["username"] or row["phone"] or row["session_name"]
                reason = row["last_error"] or row["account_last_error"] or row["restriction_reason"]
                metrics: list[str] = []
                if int(row["sent_success_count"] or 0) > 0:
                    metrics.append(f"成功 <code>{row['sent_success_count']}</code>")
                if int(row["sent_fail_count"] or 0) > 0:
                    metrics.append(f"失败 <code>{row['sent_fail_count']}</code>")
                metrics.append(str(self._dm_status_badge(row['status'])))
                line = f"• #{self._account_display_code(row['account_id'])} {html.escape(str(label), quote=False)} · " + " · ".join(metrics)
                if reason and row["status"] in {"error", "stopped"}:
                    line += f" · <code>{html.escape(self._humanize_dm_error(None, str(reason)), quote=False)[:70]}</code>"
                lines.append(line)
        if failure_summary:
            lines.append("")
            lines.append("<b>失败原因统计</b>")
            for row in failure_summary:
                reason = self._humanize_dm_error(None, str(row["reason"] or "-"))
                lines.append(f"• <code>{html.escape(reason, quote=False)[:80]}</code> · <code>{row['total']}</code>")
        if recent_logs:
            lines.append("")
            lines.append("<b>本次私信任务日志（最新 5 条）</b>")
            for row in recent_logs:
                account_label = row["account_username"] or row["account_phone"] or row["account_display_name"] or (row["account_id"] and f"#{row['account_id']}") or "-"
                target = row["normalized_input"] or "-"
                detail = self._format_dm_log_detail(row, policy)
                action_label = dm_log_action_label(row["action"])
                status_label = dm_log_status_label(row["status"])
                lines.append(
                    f"• [{html.escape(str(action_label), quote=False)} / {html.escape(str(status_label), quote=False)}] {html.escape(str(account_label), quote=False)} → {html.escape(str(target), quote=False)} · <code>{html.escape(detail, quote=False)[:80]}</code>"
                )
        return "\n".join(lines)

    def _humanize_dm_error(self, code: str | None, message: str | None) -> str:
        mapping = {
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
        raw = str(message or code or "-").strip()
        if code in mapping:
            return mapping[str(code)] if not raw or raw == code else f"{mapping[str(code)]}｜{raw}"
        lowered = raw.lower()
        phrase_map = [
            ("you can't write in this chat", "这个会话当前不允许发送消息"),
            ("settypingrequest", "当前目标不支持显示正在输入或无法发送消息"),
            ("privacy", "对方隐私限制，无法私信"),
            ("peerflood", "官方判定发送过于频繁"),
            ("floodwait", "官方限速，需要等待后再发"),
            ("forward", "当前目标不允许转发该帖子内容"),
            ("media", "当前聊天不允许发送媒体"),
            ("inline bot", "PostBot 内联结果获取失败"),
            ("entity not found", "用户不存在或无法解析"),
            ("session 未登录", "session 已失效或未登录"),
        ]
        for needle, friendly in phrase_map:
            if needle in lowered:
                return friendly
        return raw or "发送失败"

    def _channels_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_idea_id, '💡')} <b>新建频道采集</b>\n"
            f"你可以：\n"
            f"1. 直接发频道列表（一行一个）\n"
            f"2. 上传一个 <code>.txt</code> 频道文件\n\n"
            f"支持格式：\n"
            f"<code>@channel</code>\n<code>https://t.me/channel</code>\n<code>t.me/channel</code>"
        )

    def _group_targets_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_list_id, '👤')} <b>新建群组发言采集</b>\n"
            f"你可以：\n"
            f"1. 直接发送群组链接/群用户名（一行一个）\n"
            f"2. 上传一个 <code>.txt</code> 群组文件\n\n"
            f"支持格式：\n"
            f"<code>@publicgroup</code>\n<code>https://t.me/publicgroup</code>\n<code>https://t.me/+inviteHash</code>\n<code>https://t.me/joinchat/inviteHash</code>"
        )

    def _select_days_text(self, channels: list[str], *, task_type: str = "channel") -> str:
        preview = "\n".join(f"• {html.escape(channel, quote=False)}" for channel in channels[:6])
        unit = "群组" if task_type == "group" else "频道"
        return (
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>选择采集时间范围</b>\n"
            f"{unit}数：<code>{len(channels)}</code>\n\n{preview}"
        )

    def _format_check_all_progress_text(
        self,
        *,
        total: int,
        processed: int,
        current_label: str,
        unrestricted: int,
        limited: int,
        frozen: int,
        unknown: int,
        deleted_broken: int,
        deleted_banned: int,
        kept_other_errors: int,
        parallel: int,
    ) -> str:
        return "\n".join(
            [
                f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>正在批量检查账号状态</b>",
                f"进度：<code>{processed}/{total}</code> · 并发：<code>{parallel}</code>",
                f"最近完成：<code>{html.escape(current_label[:36], quote=False)}</code>",
                "",
                f"无限制：<code>{unrestricted}</code>",
                f"受限：<code>{limited}</code>",
                f"冻结：<code>{frozen}</code>",
                f"待确认：<code>{unknown}</code>",
                f"已删损坏：<code>{deleted_broken}</code>",
                f"已删封禁/失效/冻结：<code>{deleted_banned}</code>",
                f"其他异常：<code>{kept_other_errors}</code>",
                "",
                "账号较多时会持续刷新这里，包含 SpamBot 检测过程，不是卡住。",
            ]
        )

    def _custom_days_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_waiting_id, '🕜')} <b>自定义天数</b>\n"
            f"请直接发送一个整数天数，例如 <code>7</code>。"
        )

    def _custom_workers_prompt_text(self, draft: dict) -> str:
        max_workers = self._max_worker_count(draft)
        return (
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>自定义并发线程</b>\n"
            f"当前最多可设：<code>{max_workers}</code>\n"
            f"请直接发送一个整数，例如 <code>{max_workers}</code>。"
        )

    def _select_accounts_text(self, channels: list[str], days: int, selected_ids: list[int], *, task_type: str = "channel", filters: dict | None = None) -> str:
        active = self.db.get_active_accounts()
        unit = "群组" if task_type == "group" else "频道"
        lines = [
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>选择采集账号</b>",
            f"{unit}数：<code>{len(channels)}</code> · 时间范围：<code>{days}</code> 天",
            f"已选账号：<code>{len(selected_ids)}</code>",
        ]
        if task_type == "group":
            lines.append(f"筛选规则：<code>{html.escape(self._format_filter_summary(filters), quote=False)}</code>")
            lines.append("单号每轮最多新增 5 个群，剩余群会自动冷却后继续处理。")
        lines.append("")
        for row in active:
            mark = "已选" if row["id"] in selected_ids else "未选"
            label = row["username"] or row["phone"] or row["session_name"]
            lines.append(f"• #{self._account_display_code(row)} {html.escape(str(label), quote=False)} · {mark}")
        return "\n".join(lines)

    def _group_filters_text(self, draft: dict) -> str:
        filters = self._parse_group_filters(draft.get("filters"))
        lines = [
            f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>设置筛选规则</b>",
            f"群组数：<code>{len(draft.get('channels') or [])}</code> · 时间范围：<code>{draft.get('days') or 1}</code> 天",
            "所有项目都改成二态切换，点一下就在两种采集口径之间切换。",
            "",
            f"• 机器人：<code>{self._bot_mode_label(filters.get('bot_mode'))}</code>",
            f"• 管理员：<code>{self._admin_mode_label(filters.get('admin_mode'))}</code>",
            f"• 头像：<code>{self._photo_mode_label(filters.get('photo_mode'))}</code>",
            f"• 用户名：<code>{self._username_mode_label(filters.get('username_mode'))}</code>",
            f"• 会员：<code>{self._premium_mode_label(filters.get('premium_mode'))}</code>",
        ]
        return "\n".join(lines)

    def _select_workers_text(self, draft: dict) -> str:
        max_workers = self._max_worker_count(draft)
        task_type = draft.get("task_type", "channel")
        unit = "群组" if task_type == "group" else "频道"
        tail = "单号遇到第 6 个新群时会自动冷却后继续，不会只跑 5 个就结束。" if task_type == "group" else "并发会按 账号数 / 频道数 / 上限 取最小值。"
        return (
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>设置并发</b>\n"
            f"{unit}数：<code>{len(draft.get('channels') or [])}</code>\n"
            f"账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"当前最多可设：<code>{max_workers}</code>\n"
            f"{tail}"
        )

    def _max_worker_count(self, draft: dict) -> int:
        channels = len(draft.get("channels") or [])
        accounts = len(draft.get("account_ids") or [])
        return max(1, min(channels or 1, accounts or 1, self.settings.max_collect_workers))

    def _collect_confirm_text(self, draft: dict) -> str:
        channels = draft.get("channels") or []
        preview = "\n".join(f"• {html.escape(channel, quote=False)}" for channel in channels[:6])
        task_type = draft.get("task_type", "channel")
        unit = "群组" if task_type == "group" else "频道"
        lines = [
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} <b>确认启动采集</b>",
            f"类型：<code>{'群组发言采集' if task_type == 'group' else '频道用户名采集'}</code>",
            f"{unit}数：<code>{len(channels)}</code>",
            f"时间范围：<code>{draft.get('days')}</code> 天",
            f"账号数：<code>{len(draft.get('account_ids') or [])}</code>",
            f"并发：<code>{draft.get('worker_count')}</code>",
        ]
        if task_type == "group":
            lines.append(f"筛选规则：<code>{html.escape(self._format_filter_summary(draft.get('filters')), quote=False)}</code>")
            lines.append("导出文件：@用户名 / 无用户名 ID / 失败群原因")
        lines.append("")
        lines.append(preview)
        return "\n".join(lines)

    def _not_found_text(self, message: str) -> str:
        return f"{tg_emoji(self.settings.emoji_error_id, '❌')} <b>{html.escape(message, quote=False)}</b>"

    def _build_auto_reply_text(self) -> str:
        return f"{tg_emoji(self.settings.emoji_success_id, '🆗')} {html.escape(self.settings.auto_reply_text, quote=False)}"

    # ---------- keyboards ----------
    def _build_main_menu(self, user_id: int) -> InlineKeyboardMarkup | None:
        if not self._is_admin(user_id):
            return None
        keyboard = [
            [
                premium_button("账号管理", self.settings.emoji_list_id, callback_data="menu:accounts"),
                premium_button("采集用户", self.settings.emoji_progress_id, callback_data="menu:collect"),
            ],
            [
                premium_button("私信任务", self.settings.emoji_start_id, callback_data="menu:dm"),
                premium_button("统计", self.settings.emoji_stats_id, callback_data="menu:stats"),
            ],
            [
                premium_button("历史结果", self.settings.emoji_history_id, callback_data="menu:history"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _build_account_detail_keyboard(self, account_id: int) -> InlineKeyboardMarkup:
        keyboard = [
            [
                premium_button("检测状态", self.settings.emoji_stats_id, callback_data=f"account:check:{account_id}"),
                premium_button("删除账号", self.settings.emoji_error_id, callback_data=f"account:delete:{account_id}"),
            ],
            [
                premium_button("返回账号列表", self.settings.emoji_back_id, callback_data="account:list:1"),
                premium_button("刷新详情", self.settings.emoji_refresh_id, callback_data=f"account:view:{account_id}"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _build_days_keyboard(self) -> InlineKeyboardMarkup:
        keyboard = [
            [
                premium_button("1 天", self.settings.emoji_waiting_id, callback_data="wizard:days:1"),
                premium_button("3 天", self.settings.emoji_progress_id, callback_data="wizard:days:3"),
            ],
            [
                premium_button("7 天", self.settings.emoji_history_id, callback_data="wizard:days:7"),
                premium_button("15 天", self.settings.emoji_stats_id, callback_data="wizard:days:15"),
            ],
            [
                premium_button("自定义", self.settings.emoji_idea_id, callback_data="wizard:days_custom"),
                premium_button("取消", self.settings.emoji_error_id, callback_data="wizard:cancel"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _build_group_filters_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        filters = self._parse_group_filters(draft.get("filters"))
        keyboard = [
            [
                premium_button(f"机器人：{self._bot_mode_label(filters.get('bot_mode'))}", self.settings.emoji_error_id, callback_data="wizard:gflt:toggle:bot_mode"),
                premium_button(f"管理员：{self._admin_mode_label(filters.get('admin_mode'))}", self.settings.emoji_stats_id, callback_data="wizard:gflt:toggle:admin_mode"),
            ],
            [
                premium_button(f"头像：{self._photo_mode_label(filters.get('photo_mode'))}", self.settings.emoji_upload_id, callback_data="wizard:gflt:toggle:photo_mode"),
                premium_button(f"用户名：{self._username_mode_label(filters.get('username_mode'))}", self.settings.emoji_inbox_id, callback_data="wizard:gflt:toggle:username_mode"),
            ],
            [
                premium_button(f"会员：{self._premium_mode_label(filters.get('premium_mode'))}", self.settings.emoji_all_id, callback_data="wizard:gflt:toggle:premium_mode"),
            ],
            [
                premium_button("完成设置", self.settings.emoji_ok_id, callback_data="wizard:gflt:done"),
                premium_button("取消", self.settings.emoji_back_id, callback_data="wizard:cancel"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _build_account_selection_keyboard(self, selected_ids: list[int]) -> InlineKeyboardMarkup:
        keyboard = []
        row_buffer = []
        for row in self.db.get_active_accounts():
            is_selected = row["id"] in selected_ids
            icon = self.settings.emoji_ok_id if is_selected else self.settings.emoji_error_id
            title = row["username"] or row["phone"] or row["session_name"]
            row_buffer.append(
                premium_button(f"#{self._account_display_code(row)} {str(title)[:28]}", icon, callback_data=f"wizard:acc:toggle:{row['id']}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        keyboard.append([
            premium_button("使用全部可用账号", self.settings.emoji_all_id, callback_data="wizard:acc:auto"),
            premium_button("完成选择", self.settings.emoji_ok_id, callback_data="wizard:acc:done"),
        ])
        keyboard.append([
            premium_button("取消", self.settings.emoji_error_id, callback_data="wizard:cancel"),
            premium_button("重新开始", self.settings.emoji_idea_id, callback_data="collect:new"),
        ])
        return InlineKeyboardMarkup(keyboard)

    def _build_dm_account_selection_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        selected_ids = [int(item) for item in (draft.get("account_ids") or [])]
        rows, page, total_pages = self._get_dm_account_page_rows(draft)
        keyboard = []
        row_buffer = []
        for row in rows:
            is_selected = int(row["id"]) in selected_ids
            icon = self.settings.emoji_ok_id if is_selected else self.settings.emoji_error_id
            title = row["username"] or row["phone"] or row["session_name"]
            row_buffer.append(
                premium_button(f"#{self._account_display_code(row)} {str(title)[:28]}", icon, callback_data=f"dm:wizard:acc:toggle:{row['id']}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        keyboard.append([
            premium_button("全选本页", self.settings.emoji_all_id, callback_data="dm:wizard:acc:page_all"),
            premium_button("完成选择", self.settings.emoji_ok_id, callback_data="dm:wizard:acc:done"),
        ])
        nav = []
        if page > 1:
            nav.append(premium_button("上一页", self.settings.emoji_back_id, callback_data=f"dm:wizard:acc:page:{page - 1}"))
        if page < total_pages:
            nav.append(premium_button("下一页", self.settings.emoji_next_id, callback_data=f"dm:wizard:acc:page:{page + 1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([
            premium_button("全选全部", self.settings.emoji_success_id, callback_data="dm:wizard:acc:auto"),
            premium_button("返回上一步", self.settings.emoji_back_id, callback_data="dm:wizard:back:targets"),
        ])
        return InlineKeyboardMarkup(keyboard)

    def _build_dm_config_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        worker_count = self._sync_dm_worker_count(draft)
        policy = draft.get("policy") or {}
        content_type = draft.get("content_type") or "text"
        mode_label = message_mode_label(draft.get("message_mode"), content_type=content_type)
        delay_label = f"{int(policy.get('delay_min', 8))}-{int(policy.get('delay_max', 15))}秒"
        keyboard = [
            [
                premium_button(f"内容：{content_type_label(content_type)}", self.settings.emoji_list_id, callback_data="dm:wizard:content:cycle"),
                premium_button(f"模式：{mode_label}", self.settings.emoji_idea_id, callback_data="dm:wizard:mode:toggle"),
            ],
            [
                premium_button(f"上限：{int(policy.get('per_account_success_limit', 40))}", self.settings.emoji_progress_id, callback_data="dm:wizard:limit:cycle"),
                premium_button(f"并发：{worker_count}", self.settings.emoji_stats_id, callback_data="dm:wizard:worker:cycle"),
            ],
            [
                premium_button(f"间隔：{delay_label}", self.settings.emoji_timeout_id, callback_data="dm:wizard:delay:cycle"),
                premium_button(f"打字：{'开' if policy.get('typing_simulation', True) else '关'}", self.settings.emoji_upload_id, callback_data="dm:wizard:typing:toggle"),
            ],
            [
                premium_button(f"切号：{'开' if policy.get('auto_switch_account', True) else '关'}", self.settings.emoji_all_id, callback_data="dm:wizard:switch:toggle"),
                premium_button(f"置顶：{'开' if policy.get('pin_after_send', False) else '关'}", self.settings.emoji_history_id, callback_data="dm:wizard:pin:toggle"),
                premium_button(f"置顶延迟：{int(policy.get('pin_delay_seconds', 3))}秒", self.settings.emoji_waiting_id, callback_data="dm:wizard:pin_delay:cycle"),
            ],
        ]
        if draft.get("message_mode") == "three_stage":
            keyboard.append([
                premium_button(f"第1段后：{int(policy.get('stage1_delay_seconds', 5))}秒", self.settings.emoji_stats_id, callback_data="dm:wizard:stage1:cycle"),
                premium_button(f"第2段后：{int(policy.get('stage2_delay_seconds', 3))}秒", self.settings.emoji_welcome_id, callback_data="dm:wizard:stage2:cycle"),
            ])
        keyboard.extend([
            [premium_button("继续输入文案", self.settings.emoji_start_id, callback_data="dm:wizard:cfg:done")],
            [
                premium_button("重新选账号", self.settings.emoji_back_id, callback_data="dm:wizard:back_accounts"),
                premium_button("取消", self.settings.emoji_error_id, callback_data="dm:wizard:cancel"),
            ],
        ])
        return InlineKeyboardMarkup(keyboard)

    def _build_workers_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        max_workers = self._max_worker_count(draft)
        presets = [1, 2, 3, 5, 10, 20, 30, 50]
        available = [value for value in presets if value <= max_workers]
        if max_workers not in available:
            available.append(max_workers)
        available = sorted(set(available))
        icon_pool = [
            self.settings.emoji_progress_id,
            self.settings.emoji_waiting_id,
            self.settings.emoji_history_id,
            self.settings.emoji_stats_id,
            self.settings.emoji_upload_id,
            self.settings.emoji_list_id,
            self.settings.emoji_all_id,
            self.settings.emoji_start_id,
        ]

        keyboard = []
        row_buffer = []
        for index, value in enumerate(available):
            row_buffer.append(
                premium_button(f"{value} 线程", icon_pool[index % len(icon_pool)], callback_data=f"wizard:wrk:{value}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        keyboard.append([
            premium_button("自定义线程", self.settings.emoji_idea_id, callback_data="wizard:wrk_custom"),
            premium_button("取消", self.settings.emoji_error_id, callback_data="wizard:cancel"),
        ])
        return InlineKeyboardMarkup(keyboard)

    def _build_confirm_keyboard(self) -> InlineKeyboardMarkup:
        keyboard = [
            [
                premium_button("开始采集", self.settings.emoji_start_id, callback_data="wizard:start"),
                premium_button("取消", self.settings.emoji_error_id, callback_data="wizard:cancel"),
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    def _build_task_keyboard(self, task_id: int, *, page: int = 1, source: str = "tasks") -> InlineKeyboardMarkup:
        task = self.db.get_collect_task(task_id)
        back_callback = f"menu:history:{page}" if source == "history" else f"collect:tasks:{page}"
        back_label = "返回历史结果" if source == "history" else "返回任务列表"
        keyboard = [
            [
                premium_button("刷新任务", self.settings.emoji_refresh_id, callback_data=f"task:refresh:{task_id}:{page}:{source}"),
                premium_button("导出结果", self.settings.emoji_export_id, callback_data=f"task:export:{task_id}"),
            ],
            [
                premium_button("删除任务", self.settings.emoji_error_id, callback_data=f"task:delete:{task_id}:{page}:{source}"),
                premium_button(back_label, self.settings.emoji_back_id, callback_data=back_callback),
            ],
        ]
        if task and task["status"] in {"queued", "running"}:
            keyboard.insert(1, [
                premium_button("停止任务", self.settings.emoji_timeout_id, callback_data=f"task:stop:{task_id}:{page}:{source}"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ])
        else:
            keyboard.append([
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
                premium_button(back_label, self.settings.emoji_list_id, callback_data=back_callback),
            ])
        return InlineKeyboardMarkup(keyboard)

    def _build_dm_confirm_keyboard(self, back_callback: str = "dm:wizard:back:input") -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                premium_button("预览文案", self.settings.emoji_welcome_id, callback_data="dm:wizard:preview"),
                premium_button("开始私信", self.settings.emoji_start_id, callback_data="dm:wizard:start"),
            ],
            [
                premium_button("返回上一步", self.settings.emoji_back_id, callback_data=back_callback),
                premium_button("取消", self.settings.emoji_error_id, callback_data="dm:wizard:cancel"),
            ],
        ])

    def _build_dm_task_keyboard(self, task_id: int, *, page: int = 1) -> InlineKeyboardMarkup:
        task = self.dm_repository.get_dm_task(task_id)
        keyboard: list[list] = []
        if task:
            pending_count = max(0, int(task['total_targets'] or 0) - int(task['success_count'] or 0) - int(task['failed_count'] or 0) - int(task['skipped_count'] or 0))
            stats_row = []
            if int(task['success_count'] or 0) > 0:
                stats_row.append(premium_button(f"成功 {task['success_count']}", self.settings.emoji_success_id, callback_data=f"dm:refresh:{task_id}:{page}"))
            if int(task['failed_count'] or 0) > 0:
                stats_row.append(premium_button(f"失败 {task['failed_count']}", self.settings.emoji_error_id, callback_data=f"dm:refresh:{task_id}:{page}"))
            if stats_row:
                keyboard.append(stats_row[:2])
            if pending_count > 0:
                keyboard.append([premium_button(f"待发送 {pending_count}", self.settings.emoji_waiting_id, callback_data=f"dm:refresh:{task_id}:{page}")])
        if task and task["status"] in {"queued", "running"}:
            keyboard.append([premium_button("停止任务", self.settings.emoji_timeout_id, callback_data=f"dm:stop:{task_id}:{page}")])
        keyboard.extend([
            [
                premium_button("导出结果", self.settings.emoji_export_id, callback_data=f"dm:export:{task_id}"),
                premium_button("刷新任务", self.settings.emoji_refresh_id, callback_data=f"dm:refresh:{task_id}:{page}"),
            ],
            [
                premium_button("返回任务列表", self.settings.emoji_back_id, callback_data=f"dm:tasks:{page}"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ],
        ])
        return InlineKeyboardMarkup(keyboard)

    def _single_back_keyboard(self, callback_data: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[premium_button("返回", self.settings.emoji_back_id, callback_data=callback_data)]])

    def _account_display_map(self) -> dict[int, int]:
        visible_rows = [
            row for row in self.db.list_all_accounts()
            if row["status"] in {"active", "checking", "collecting"}
        ]
        visible_rows.sort(key=lambda row: int(row["id"]))
        return {int(row["id"]): index for index, row in enumerate(visible_rows, start=1)}

    def _account_display_code(self, account_or_id) -> int:
        account_id = int(account_or_id["id"] if hasattr(account_or_id, "keys") else account_or_id)
        return self._account_display_map().get(account_id, account_id)

    def _task_display_map(self) -> dict[int, int]:
        rows = self.db.list_collect_tasks(limit=1000000, offset=0, history=True)
        rows.sort(key=lambda row: int(row["id"]))
        return {int(row["id"]): index for index, row in enumerate(rows, start=1)}

    def _task_display_code(self, task_or_id) -> int:
        task_id = int(task_or_id["id"] if hasattr(task_or_id, "keys") else task_or_id)
        return self._task_display_map().get(task_id, task_id)

    def _dm_max_worker_count(self, draft: dict) -> int:
        account_count = len(draft.get("account_ids") or [])
        return max(1, min(account_count or 1, 50))

    def _sync_dm_worker_count(self, draft: dict) -> int:
        max_workers = self._dm_max_worker_count(draft)
        default_workers = min(3, max_workers)
        try:
            current = int(draft.get("worker_count") or default_workers)
        except (TypeError, ValueError):
            current = default_workers
        current = max(1, min(current, max_workers))
        draft["worker_count"] = current
        return current

    def _dm_status_badge(self, status: str | None) -> str:
        normalized = str(status or "queued")
        if normalized == "running":
            return f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>私信中</b>"
        return status_badge(normalized)

    def _is_dm_frequency_log(self, detail: str) -> bool:
        return any(keyword in detail for keyword in ("频繁", "限速", "双向", "冻结"))

    def _format_dm_log_detail(self, row, policy: dict) -> str:
        detail = self._humanize_dm_error(None, str(row["message"] or row["raw_error"] or "-"))
        if "[" in detail and detail.endswith("]"):
            return detail
        if not self._is_dm_frequency_log(detail):
            return detail
        limit = int(policy.get("per_account_success_limit") or 0)
        if limit <= 0:
            return detail
        current = int(row["account_sent_success_count"] or 0)
        return f"{detail}[{current}/{limit}]"

    # ---------- helpers ----------
    def _account_export_bucket_label(self, bucket: str) -> str:
        return {
            "all": "全部账号",
            "unrestricted": "无限制",
            "limited": "受限",
            "invalid": "失效/封禁",
            "frozen": "冻结",
        }.get(bucket, bucket)

    def _filter_account_rows_for_export(self, bucket: str) -> list:
        rows = self.db.list_all_accounts()
        limited_statuses = {"temp_mutual", "permanent_mutual", "geo_limited", "spam_limited", "restricted"}
        matched = []
        for row in rows:
            restriction = str(row["restriction_status"] or "unknown")
            runtime_status = str(row["status"] or "active")
            if bucket == "all":
                matched.append(row)
            elif bucket == "unrestricted" and restriction == "unrestricted":
                matched.append(row)
            elif bucket == "limited" and restriction in limited_statuses:
                matched.append(row)
            elif bucket == "frozen" and restriction == "frozen":
                matched.append(row)
            elif bucket == "invalid" and (restriction == "session_invalid" or runtime_status in {"unauthorized", "error"}):
                matched.append(row)
        return matched

    def _build_accounts_export_zip(self, rows: list, bucket: str) -> tuple[Path, int]:
        self.settings.export_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = self.settings.export_dir / f"accounts_{bucket}_{stamp}.zip"
        added_files = 0
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for row in rows:
                session_path = Path(str(row["session_file"] or ""))
                if not session_path.exists():
                    continue
                folder_name = f"{self._account_display_code(row)}_{session_path.stem}"
                archive.write(session_path, arcname=f"{folder_name}/{session_path.name}")
                added_files += 1
                sidecar = session_path.with_suffix(".json")
                if sidecar.exists():
                    archive.write(sidecar, arcname=f"{folder_name}/{sidecar.name}")
                    added_files += 1
        return zip_path, added_files

    async def _send_account_export(self, chat_id: int, rows: list, bucket: str, *, auto_delete: bool) -> None:
        if not rows:
            return
        zip_path, added_files = self._build_accounts_export_zip(rows, bucket)
        if added_files <= 0:
            raise FileNotFoundError("没有找到可打包的 session 或 json 原文件")
        await self.application.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        with zip_path.open("rb") as fp:
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=fp,
                filename=zip_path.name,
                caption=(
                    f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>账号导出｜{self._account_export_bucket_label(bucket)}</b>\n"
                    f"数量：<code>{len(rows)}</code>"
                    + ("\n说明：<code>导出后已自动删除这些账号</code>" if auto_delete else "")
                ),
                parse_mode=ParseMode.HTML,
            )
        if auto_delete:
            for row in rows:
                self._purge_account_files(row)
                self.db.delete_account(int(row["id"]))

    async def _export_accounts_by_bucket(self, query, chat_id: int, bucket: str) -> None:
        rows = self._filter_account_rows_for_export(bucket)
        if not rows:
            await query.answer(f"没有可导出的{self._account_export_bucket_label(bucket)}账号", show_alert=True)
            return
        try:
            await self._send_account_export(chat_id, rows, bucket, auto_delete=True)
        except FileNotFoundError as exc:
            await query.answer(str(exc), show_alert=True)
            return
        await self._show_accounts_menu(query, page=1)
        await query.answer("导出完成，顶部统计已刷新", show_alert=False)

    async def _send_dm_exports(self, chat_id: int) -> None:
        await self.application.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        paths = self.db.export_csv(self.settings.export_dir)
        with paths.users_csv.open("rb") as users_fp:
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=users_fp,
                filename=paths.users_csv.name,
                caption=f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>用户导出</b>",
                parse_mode=ParseMode.HTML,
            )
        with paths.messages_csv.open("rb") as messages_fp:
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=messages_fp,
                filename=paths.messages_csv.name,
                caption=f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>消息导出</b>",
                parse_mode=ParseMode.HTML,
            )

    async def _send_dm_task_result(self, chat_id: int, task_id: int) -> None:
        task = self.dm_repository.get_dm_task(task_id)
        if not task:
            return
        paths = self.dm_repository.export_task_results(task_id, self.settings.export_dir)
        pending_count = max(0, int(task['total_targets'] or 0) - int(task['success_count'] or 0) - int(task['failed_count'] or 0) - int(task['skipped_count'] or 0))
        captions = [
            (paths.success_txt, int(task['success_count'] or 0), f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>私信成功名单</b>\n成功：<code>{task['success_count']}</code>"),
            (paths.failed_txt, int(task['failed_count'] or 0), f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>私信失败名单</b>\n失败：<code>{task['failed_count']}</code>"),
            (paths.report_csv, max(1, int(task['success_count'] or 0) + int(task['failed_count'] or 0) + int(task['skipped_count'] or 0)), f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>本次私信任务日志 / 统计视图</b>"),
            (paths.pending_txt, pending_count, f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>私信剩余待发送</b>\n待发送：<code>{pending_count}</code>"),
        ]
        sent_any = False
        await self.application.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        for path, count, caption in captions:
            if count <= 0 or not path.exists():
                continue
            sent_any = True
            with path.open("rb") as fp:
                await self.application.bot.send_document(
                    chat_id=chat_id,
                    document=fp,
                    filename=path.name,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
        if not sent_any:
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前这个私信任务还没有可导出的结果。",
                parse_mode=ParseMode.HTML,
            )

    async def _send_task_result(self, chat_id: int, task_id: int, announce: bool = False) -> None:
        task = self.db.get_collect_task(task_id)
        if not task:
            return
        display_code = self._task_display_code(task)
        caption_prefix = "采集结果已生成" if announce else f"任务 #{display_code} 结果导出"
        if (task["task_type"] or "channel") == "group":
            outputs = self.db.export_group_task_files(task_id, self.settings.export_dir)
            captions = {
                "usernames": f"{caption_prefix} · 用户名",
                "ids": f"任务 #{display_code} 结果导出 · ID",
                "failed": f"任务 #{display_code} 结果导出 · 失败群",
            }
            labels = {
                "usernames": "用户名数量",
                "ids": "ID 数量",
                "failed": "失败群数量",
            }
            sent_any = False
            for key in ["usernames", "ids", "failed"]:
                info = outputs[key]
                path = info.get("path")
                count = int(info.get("count") or 0)
                if count <= 0 or not path or not Path(path).exists():
                    continue
                sent_any = True
                with Path(path).open("rb") as fp:
                    await self.application.bot.send_document(
                        chat_id=chat_id,
                        document=fp,
                        filename=Path(path).name,
                        caption=(
                            f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>{captions[key]}</b>\n"
                            f"{labels[key]}：<code>{count}</code>"
                        ),
                        parse_mode=ParseMode.HTML,
                    )
            if not sent_any:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前没有可导出的用户名、ID 或失败群结果。",
                    parse_mode=ParseMode.HTML,
                )
            return

        path = task["result_file_path"]
        if not path or not Path(path).exists():
            path = str(self.db.export_task_usernames_txt(task_id, self.settings.export_dir))
        with Path(path).open("rb") as fp:
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=fp,
                filename=Path(path).name,
                caption=(
                    f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>{caption_prefix}</b>\n"
                    f"去重数量：<code>{task['unique_hits']}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )

    def _cleanup_task_export_files(self, task) -> None:
        path = task["result_file_path"]
        if path:
            try:
                Path(str(path)).unlink(missing_ok=True)
            except Exception:
                logger.exception("删除任务导出文件失败: %s", path)
        if hasattr(task, "keys") and task["task_type"] == "group":
            for suffix in ("_ids.txt", "_failed_groups.txt"):
                extra = self.settings.export_dir / f"task_{int(task['id'])}{suffix}"
                try:
                    extra.unlink(missing_ok=True)
                except Exception:
                    logger.exception("删除任务导出文件失败: %s", extra)

    async def _fanout_to_admins(self, update: Update) -> None:
        user = update.effective_user
        chat = update.effective_chat
        message = update.effective_message
        if not user or not chat or not message:
            return

        safe_name = html.escape(user.full_name or str(user.id), quote=False)
        safe_type = html.escape(self._detect_message_type(message), quote=False)
        lines = [
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>收到新私信</b>",
            f"用户 ID：<code>{user.id}</code>",
            f"姓名：{safe_name}",
            f"Chat ID：<code>{chat.id}</code>",
            f"消息类型：<code>{safe_type}</code>",
        ]
        if user.username:
            lines.insert(2, f"用户名：@{html.escape(user.username, quote=False)}")
        prefix = "\n".join(lines)

        for admin_id in self.settings.admin_ids:
            try:
                await self.application.bot.send_message(
                    chat_id=admin_id,
                    text=prefix,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await self.application.bot.copy_message(
                    chat_id=admin_id,
                    from_chat_id=chat.id,
                    message_id=message.message_id,
                )
            except Exception:  # noqa: BLE001
                logger.exception("转发私信到管理员失败: admin_id=%s user_id=%s", admin_id, user.id)

    async def _ensure_admin(self, update: Update) -> bool:
        user = update.effective_user
        if user and self._is_admin(user.id):
            return True
        denied_text = (
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>无权限</b>\n"
            f"这个功能目前只开放给管理员。"
        )
        if update.callback_query:
            await update.callback_query.answer("无权限", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text(denied_text, parse_mode=ParseMode.HTML)
        return False

    async def _reply_or_edit(self, update: Update, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
        if update.callback_query:
            await self._safe_edit(update.callback_query, text, markup)
        elif update.effective_message:
            await update.effective_message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=markup,
            )

    async def _safe_edit(self, query, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
        try:
            await query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
        except BadRequest as exc:
            if "Message is not modified" in str(exc):
                return
            raise

    async def _save_uploaded_session_files(self, document) -> list[Path]:
        original_name = Path(document.file_name or f"upload_{int(time.time())}")
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", original_name.name)
        temp_target = self.settings.session_dir / f"upload_{int(time.time())}_{safe_name}"
        tg_file = await document.get_file()
        await tg_file.download_to_drive(custom_path=str(temp_target))

        if temp_target.suffix.lower() == ".session":
            final_session = self._unique_session_path(temp_target.name)
            temp_target.replace(final_session)
            return [final_session]

        session_files: list[Path] = []
        archive_sessions: list[tuple[str, bytes]] = []
        extracted_json: dict[str, bytes] = {}
        try:
            with zipfile.ZipFile(temp_target) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    entry_name = Path(info.filename).name
                    if not entry_name:
                        continue
                    suffix = Path(entry_name).suffix.lower()
                    if suffix == ".json":
                        extracted_json[Path(entry_name).stem.lower()] = zf.read(info)
                    elif suffix == ".session":
                        archive_sessions.append((entry_name, zf.read(info)))

                for entry_name, content in archive_sessions:
                    final_session = self._unique_session_path(entry_name)
                    final_session.write_bytes(content)
                    sidecar = extracted_json.get(Path(entry_name).stem.lower())
                    if sidecar is not None:
                        final_session.with_suffix(".json").write_bytes(sidecar)
                    session_files.append(final_session)
            return session_files
        finally:
            temp_target.unlink(missing_ok=True)

    def _unique_session_path(self, file_name: str) -> Path:
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", Path(file_name).name)
        if not safe_name.lower().endswith(".session"):
            safe_name = f"{Path(safe_name).stem}.session"
        target = self.settings.session_dir / safe_name
        if not target.exists():
            return target
        return self.settings.session_dir / f"{target.stem}_{int(time.time() * 1000)}.session"

    def _parse_channels(self, text: str) -> list[str]:
        result = []
        seen = set()
        for raw_line in (text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("https://t.me/"):
                line = "@" + line.removeprefix("https://t.me/").split("/", 1)[0]
            elif line.startswith("http://t.me/"):
                line = "@" + line.removeprefix("http://t.me/").split("/", 1)[0]
            elif line.startswith("t.me/"):
                line = "@" + line.removeprefix("t.me/").split("/", 1)[0]
            if not line.startswith("@"):
                continue
            normalized = "@" + re.sub(r"[^A-Za-z0-9_]", "", line[1:])
            if len(normalized) < 6:
                continue
            if normalized.lower() in seen:
                continue
            seen.add(normalized.lower())
            result.append(normalized)
        return result

    def _parse_group_targets(self, text: str) -> list[str]:
        result = []
        seen = set()
        for raw_line in (text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            invite_match = re.search(r"(?:https?://)?t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)", line)
            if invite_match:
                normalized = f"https://t.me/+{invite_match.group(1)}"
            else:
                candidate = line
                if candidate.startswith("https://t.me/"):
                    candidate = "@" + candidate.removeprefix("https://t.me/").split("/", 1)[0]
                elif candidate.startswith("http://t.me/"):
                    candidate = "@" + candidate.removeprefix("http://t.me/").split("/", 1)[0]
                elif candidate.startswith("t.me/"):
                    candidate = "@" + candidate.removeprefix("t.me/").split("/", 1)[0]
                elif not candidate.startswith("@") and re.fullmatch(r"[A-Za-z0-9_]{5,64}", candidate):
                    candidate = f"@{candidate}"
                if not candidate.startswith("@"):
                    continue
                normalized = "@" + re.sub(r"[^A-Za-z0-9_]", "", candidate[1:])
                if len(normalized) < 6:
                    continue
            if normalized.lower() in seen:
                continue
            seen.add(normalized.lower())
            result.append(normalized)
        return result

    def _parse_group_filters(self, raw) -> dict[str, str]:
        defaults = {
            "bot_mode": "non_bot_only",
            "admin_mode": "non_admin_only",
            "photo_mode": "has_photo_only",
            "username_mode": "has_username_only",
            "premium_mode": "premium_only",
        }
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None
        if isinstance(raw, dict):
            bot_mode = str(raw.get("bot_mode") or "")
            admin_mode = str(raw.get("admin_mode") or "")
            photo_mode = str(raw.get("photo_mode") or "")
            username_mode = str(raw.get("username_mode") or "")
            premium_mode = str(raw.get("premium_mode") or "premium_only")

            if bot_mode in {"non_bot_only", "bot_only"}:
                defaults["bot_mode"] = bot_mode
            elif raw.get("exclude_bots") is True:
                defaults["bot_mode"] = "non_bot_only"

            if admin_mode in {"non_admin_only", "admin_only"}:
                defaults["admin_mode"] = admin_mode
            elif raw.get("exclude_admins") is True:
                defaults["admin_mode"] = "non_admin_only"

            if photo_mode in {"has_photo_only", "no_photo_only"}:
                defaults["photo_mode"] = photo_mode
            elif raw.get("exclude_no_photo") is True:
                defaults["photo_mode"] = "has_photo_only"

            if username_mode in {"has_username_only", "no_username_only"}:
                defaults["username_mode"] = username_mode
            elif raw.get("exclude_no_username") is True:
                defaults["username_mode"] = "has_username_only"

            if premium_mode in {"premium_only", "non_premium_only"}:
                defaults["premium_mode"] = premium_mode
        return defaults

    def _format_filter_summary(self, raw_filters, *, empty_label: str = "全部保留") -> str:
        filters = self._parse_group_filters(raw_filters)
        labels = [
            self._bot_mode_label(filters.get("bot_mode")),
            self._admin_mode_label(filters.get("admin_mode")),
            self._photo_mode_label(filters.get("photo_mode")),
            self._username_mode_label(filters.get("username_mode")),
            self._premium_mode_label(filters.get("premium_mode")),
        ]
        return "、".join(labels) if labels else empty_label

    @staticmethod
    def _bot_mode_label(value: str | None) -> str:
        if value == "bot_only":
            return "仅机器人"
        return "非机器人"

    @staticmethod
    def _admin_mode_label(value: str | None) -> str:
        if value == "admin_only":
            return "仅管理员"
        return "非管理员"

    @staticmethod
    def _photo_mode_label(value: str | None) -> str:
        if value == "no_photo_only":
            return "无头像"
        return "仅有头像"

    @staticmethod
    def _username_mode_label(value: str | None) -> str:
        if value == "no_username_only":
            return "无用户名（导出 ID）"
        return "仅有用户名"

    @staticmethod
    def _premium_mode_label(value: str | None) -> str:
        if value == "premium_only":
            return "仅会员"
        return "非会员"

    def _humanize_account_issue(self, status: str, last_error: str | None) -> str:
        raw = (last_error or "").strip()
        text = raw.lower()
        if status == "unauthorized" or any(key in text for key in ["user_deactivated", "banned", "revoked", "auth key duplicated", "phone_number_banned"]):
            return "session 已失效或已封禁"
        if any(key in text for key in ["malformed", "not valid sqlite", "file is not a database", "缺少 sessions 表", "没有可用的登录记录", "缺少 telethon 必要字段", "已损坏或不是有效 sqlite"]):
            return "session 已损坏"
        if "floodwait" in text:
            return "触发 Telegram 限流，稍后再试"
        return raw or "账号不可用"

    def _should_auto_purge_account(self, status: str, last_error: str | None) -> bool:
        friendly = self._humanize_account_issue(status, last_error)
        return friendly in {"session 已失效或已封禁", "session 已损坏"}

    def _purge_session_artifacts(self, session_file: Path) -> None:
        session_file.unlink(missing_ok=True)
        session_file.with_suffix(".json").unlink(missing_ok=True)
        session_file.with_name(f"{session_file.stem}.compat.session").unlink(missing_ok=True)

    def _purge_account_files(self, account) -> None:
        try:
            self._purge_session_artifacts(Path(account["session_file"]))
        except Exception:  # noqa: BLE001
            logger.exception("删除账号 session 文件失败: %s", account["session_file"])

    def _clear_state(self, user_id: int) -> None:
        self.user_states.pop(user_id, None)

    def _is_admin(self, user_id: int) -> bool:
        return user_id in self.settings.admin_ids

    @staticmethod
    def _detect_message_type(message) -> str:
        if message.text:
            return "text"
        if message.photo:
            return "photo"
        if message.video:
            return "video"
        if message.document:
            return "document"
        if message.voice:
            return "voice"
        if message.audio:
            return "audio"
        if message.sticker:
            return "sticker"
        if message.contact:
            return "contact"
        if message.location:
            return "location"
        return "other"

    def run(self) -> None:
        logger.info("DM Collector Bot 启动中，版本：%s，数据库：%s", __version__, self.settings.db_path)
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

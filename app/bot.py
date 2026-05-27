from __future__ import annotations

import asyncio
import html
import re
import time
import zipfile
from math import ceil
from pathlib import Path

import logging
from telegram import InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest
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
from .emoji import premium_button, status_badge, tg_emoji
from .version import __version__

logger = logging.getLogger(__name__)


class DmCollectorBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = Database(settings.db_path)
        self.collection_manager = CollectionManager(
            settings,
            self.db,
            on_progress=self._on_task_progress,
            on_complete=self._on_task_complete,
        )
        self.application = Application.builder().token(settings.bot_token).build()
        self.user_states: dict[int, dict] = {}
        self.task_runners: dict[int, asyncio.Task] = {}
        self.progress_throttle: dict[int, float] = {}
        self.application.bot_data["settings"] = settings
        self.application.bot_data["db"] = self.db
        recovered = self.db.recover_interrupted_tasks(reason="机器人重启，已停止上次未完成任务并释放账号")
        if recovered:
            logger.info("启动时已回收中断采集任务: %s", recovered)
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
        if data == "menu:history":
            await self._show_history(query)
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
        if data.startswith("account:check:"):
            account_id = int(data.split(":")[-1])
            await self._check_account(query, account_id)
            return
        if data.startswith("account:delete:"):
            account_id = int(data.split(":")[-1])
            await self._delete_account(query, account_id)
            return
        if data == "collect:new":
            await self._start_collect_wizard(query, update.effective_user.id)
            return
        if data == "collect:tasks":
            await self._show_task_list(query)
            return
        if data.startswith("task:view:"):
            task_id = int(data.split(":")[-1])
            await self._show_task_detail(query, task_id, force=True)
            return
        if data.startswith("task:refresh:"):
            task_id = int(data.split(":")[-1])
            await self._show_task_detail(query, task_id, force=True)
            return
        if data.startswith("task:stop:"):
            task_id = int(data.split(":")[-1])
            self.db.stop_collect_task_now(task_id, reason="管理员手动停止任务，账号已释放")
            runner = self.task_runners.get(task_id)
            if runner and not runner.done():
                runner.cancel()
                try:
                    await runner
                except asyncio.CancelledError:
                    pass
            await self._show_task_detail(query, task_id, force=True)
            return
        if data.startswith("task:export:"):
            task_id = int(data.split(":")[-1])
            await self._send_task_result(update.effective_chat.id, task_id)
            return
        if data == "wizard:cancel":
            self._clear_state(update.effective_user.id)
            await self._safe_edit(query, self._build_welcome_text(update.effective_user.id), self._build_main_menu(update.effective_user.id))
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
        if file_name.endswith(".txt"):
            await self._handle_channel_txt_upload(update, document, state)
            return
        if not (file_name.endswith(".session") or file_name.endswith(".zip")):
            return

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
                lines.append(f"• #{account['id']} {html.escape(str(label), quote=False)} · {status_badge(account['status'])}")
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

    async def _handle_channel_txt_upload(self, update: Update, document, state: dict) -> None:
        if state.get("mode") != "await_channels":
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_idea_id, '💡')} 频道 txt 只在 <b>新建采集任务</b> 时使用。先点“新建采集任务”，再上传 txt。",
                parse_mode=ParseMode.HTML,
            )
            return

        await self.application.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
        tg_file = await document.get_file()
        raw_bytes = await tg_file.download_as_bytearray()
        try:
            text = bytes(raw_bytes).decode("utf-8")
        except UnicodeDecodeError:
            text = bytes(raw_bytes).decode("utf-8-sig", errors="ignore")
        channels = self._parse_channels(text)
        if not channels:
            await update.effective_message.reply_text(
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} txt 里没识别到有效频道，请检查内容后重传。",
                parse_mode=ParseMode.HTML,
            )
            return
        draft = state.setdefault("draft", {})
        draft["channels"] = channels
        state["mode"] = "select_days"
        await update.effective_message.reply_text(
            self._select_days_text(channels),
            parse_mode=ParseMode.HTML,
            reply_markup=self._build_days_keyboard(),
        )

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
                self._select_days_text(channels),
                parse_mode=ParseMode.HTML,
                reply_markup=self._build_days_keyboard(),
            )
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
        text = (
            f"{tg_emoji(self.settings.emoji_list_id, '👤')} <b>账号管理</b>\n"
            f"当前存活账号：<code>{count}</code>\n"
            f"可用：<code>{stats['active']}</code> · 检测中：<code>{stats['checking']}</code> · 采集中：<code>{stats['collecting']}</code>\n"
            f"待清理无效：<code>{stats['invalid']}</code>\n\n"
            f"上传 .session 后会立即做一次登录验证；损坏 / 封禁 / 失效账号会自动清掉。"
        )
        keyboard = [
            [
                premium_button("上传 session", self.settings.emoji_upload_id, callback_data="account:upload"),
                premium_button("账号列表", self.settings.emoji_list_id, callback_data=f"account:list:{page}"),
            ],
            [
                premium_button("一键检测全部", self.settings.emoji_stats_id, callback_data="account:check_all"),
                premium_button("一键清理无效", self.settings.emoji_error_id, callback_data="account:purge_invalid"),
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
                lines.append(f"• #{row['id']} {html.escape(str(label), quote=False)} · {status_badge(row['status'])}")

        keyboard: list[list] = [
            [
                premium_button("一键检测全部", self.settings.emoji_stats_id, callback_data="account:check_all"),
                premium_button("一键清理无效", self.settings.emoji_error_id, callback_data="account:purge_invalid"),
            ]
        ]
        row_buffer = []
        for row in rows:
            label = row["username"] or row["phone"] or row["session_name"]
            row_buffer.append(
                premium_button(f"#{row['id']} {str(label)[:28]}", self.settings.emoji_list_id, callback_data=f"account:view:{row['id']}")
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
        await self._safe_edit(query, self._format_account_text(self.db.get_account(account_id)), self._build_account_detail_keyboard(account_id))
        result = await self.collection_manager.verify_account(account)
        account = self.db.get_account(account_id)
        if not account:
            await self._safe_edit(query, self._not_found_text("账号不存在或已删除"), self._single_back_keyboard("account:list:1"))
            return
        issue_text = self._humanize_account_issue(result.status, result.last_error)
        if self._should_auto_purge_account(result.status, result.last_error):
            label = account["username"] or account["phone"] or account["display_name"] or account["session_name"]
            self._purge_account_files(account)
            self.db.delete_account(account_id)
            text = (
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} <b>账号已自动删除</b>\n"
                f"名称：<code>{html.escape(str(label), quote=False)}</code>\n"
                f"原因：<code>{html.escape(issue_text, quote=False)}</code>"
            )
            await self._safe_edit(query, text, self._single_back_keyboard("account:list:1"))
            return
        await self._show_account_detail(query, account_id)

    async def _check_all_accounts(self, query) -> None:
        rows = self.db.list_all_accounts()
        if not rows:
            await self._safe_edit(query, self._not_found_text("当前没有可检测的账号。"), self._single_back_keyboard("menu:accounts"))
            return

        kept_active = 0
        deleted_broken: list[str] = []
        deleted_banned: list[str] = []
        kept_other_errors: list[str] = []
        total_checked = len(rows)
        processed = 0
        parallel = min(10, total_checked)

        semaphore = asyncio.Semaphore(parallel)

        async def _verify_one(account_row):
            async with semaphore:
                result = await self.collection_manager.verify_account(account_row)
                refreshed = self.db.get_account(account_row["id"])
                return account_row, refreshed, result

        await self._safe_edit(
            query,
            self._format_check_all_progress_text(
                total=total_checked,
                processed=0,
                current_label="准备开始",
                kept_active=0,
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
            issue_text = self._humanize_account_issue(result.status, result.last_error)
            if result.status == "active":
                kept_active += 1
            elif self._should_auto_purge_account(result.status, result.last_error):
                if "损坏" in issue_text:
                    deleted_broken.append(str(label))
                else:
                    deleted_banned.append(str(label))
                if refreshed:
                    self._purge_account_files(refreshed)
                    self.db.delete_account(refreshed["id"])
            else:
                kept_other_errors.append(f"{label}｜{issue_text}")

            await self._safe_edit(
                query,
                self._format_check_all_progress_text(
                    total=total_checked,
                    processed=processed,
                    current_label=str(label),
                    kept_active=kept_active,
                    deleted_broken=len(deleted_broken),
                    deleted_banned=len(deleted_banned),
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

        total_alive = self.db.count_accounts()
        lines = [
            f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>批量检测完成</b>",
            f"总检测账号：<code>{total_checked}</code>",
            f"保留可用：<code>{kept_active}</code>",
            f"自动删除损坏 session：<code>{len(deleted_broken)}</code>",
            f"自动删除封禁/失效：<code>{len(deleted_banned)}</code>",
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
        if kept_other_errors:
            lines.append("")
            lines.append("<b>暂未删除：其他异常</b>")
            for item in kept_other_errors[:8]:
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
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>采集中心</b>\n"
            f"第一版支持：多频道、可选几天前消息、多账号并发、去重导出 txt。"
        )
        keyboard = [
            [
                premium_button("新建采集任务", self.settings.emoji_idea_id, callback_data="collect:new"),
                premium_button("任务列表", self.settings.emoji_progress_id, callback_data="collect:tasks"),
            ],
            [
                premium_button("历史结果", self.settings.emoji_history_id, callback_data="menu:history"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ],
        ]
        await self._safe_edit(query, text, InlineKeyboardMarkup(keyboard))

    async def _show_task_list(self, query) -> None:
        tasks = self.db.list_collect_tasks(limit=8)
        lines = [f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>任务列表</b>"]
        if not tasks:
            lines.append("\n还没有采集任务。")
        else:
            for task in tasks:
                lines.append(
                    f"\n• 任务 #{task['id']} · {status_badge(task['status'])} · 频道 <code>{task['finished_channels']}/{task['total_channels']}</code> · 去重 <code>{task['unique_hits']}</code>"
                )
        keyboard = []
        row_buffer = []
        for task in tasks:
            row_buffer.append(
                premium_button(f"查看任务 #{task['id']}", self.settings.emoji_history_id, callback_data=f"task:view:{task['id']}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        keyboard.append([
            premium_button("返回采集中心", self.settings.emoji_back_id, callback_data="menu:collect"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data="collect:tasks"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _show_task_detail(self, query, task_id: int, force: bool = False) -> None:
        text = self._format_task_text(task_id)
        await self._safe_edit(query, text, self._build_task_keyboard(task_id))

    async def _show_history(self, query) -> None:
        tasks = self.db.list_collect_tasks(limit=8, history=True)
        tasks = [task for task in tasks if task["status"] in {"completed", "stopped", "error"}]
        lines = [f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>历史结果</b>"]
        if not tasks:
            lines.append("\n还没有已完成/已停止的任务。")
        else:
            for task in tasks[:8]:
                lines.append(
                    f"\n• 任务 #{task['id']} · {status_badge(task['status'])} · 去重 <code>{task['unique_hits']}</code>"
                )
        keyboard = []
        row_buffer = []
        for task in tasks[:8]:
            row_buffer.append(
                premium_button(f"导出任务 #{task['id']}", self.settings.emoji_export_id, callback_data=f"task:export:{task['id']}")
            )
            if len(row_buffer) == 2:
                keyboard.append(row_buffer)
                row_buffer = []
        if row_buffer:
            keyboard.append(row_buffer)
        keyboard.append([
            premium_button("返回采集中心", self.settings.emoji_back_id, callback_data="menu:collect"),
            premium_button("刷新列表", self.settings.emoji_refresh_id, callback_data="menu:history"),
        ])
        await self._safe_edit(query, "\n".join(lines), InlineKeyboardMarkup(keyboard))

    async def _start_collect_wizard(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        if not active_accounts:
            await self._safe_edit(
                query,
                f"{tg_emoji(self.settings.emoji_error_id, '❌')} 当前没有可用账号，请先上传并验证 session。",
                self._single_back_keyboard("menu:accounts"),
            )
            return
        self.user_states[user_id] = {"mode": "await_channels", "draft": {}}
        await self._safe_edit(query, self._channels_prompt_text(), self._single_back_keyboard("wizard:cancel"))

    async def _wizard_set_days(self, query, user_id: int, days: int, reply_message=None) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        draft["days"] = days
        state["mode"] = "select_accounts"
        active_accounts = self.db.get_active_accounts()
        draft.setdefault("account_ids", [row["id"] for row in active_accounts])
        text = self._select_accounts_text(draft["channels"], days, draft["account_ids"])
        markup = self._build_account_selection_keyboard(draft["account_ids"])
        if query is not None:
            await self._safe_edit(query, text, markup)
        elif reply_message is not None:
            await reply_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

    async def _wizard_auto_accounts(self, query, user_id: int) -> None:
        active_accounts = self.db.get_active_accounts()
        state = self.user_states.setdefault(user_id, {"draft": {}})
        state.setdefault("draft", {})["account_ids"] = [row["id"] for row in active_accounts]
        await self._wizard_finish_accounts(query, user_id)

    async def _wizard_toggle_account(self, query, user_id: int, account_id: int) -> None:
        state = self.user_states.setdefault(user_id, {"draft": {}})
        draft = state.setdefault("draft", {})
        selected = set(draft.get("account_ids") or [])
        if account_id in selected:
            selected.remove(account_id)
        else:
            selected.add(account_id)
        draft["account_ids"] = sorted(selected)
        text = self._select_accounts_text(draft.get("channels", []), draft.get("days", 1), draft["account_ids"])
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
        )
        text = self._format_task_text(task["id"])
        markup = self._build_task_keyboard(task["id"])
        await self._safe_edit(query, text, markup)
        if query.message:
            self.db.set_collect_task_progress_message(task["id"], query.message.chat_id, query.message.message_id)
        self._clear_state(user_id)
        runner = asyncio.create_task(self.collection_manager.run_collect_task(task["id"]))
        self.task_runners[task["id"]] = runner
        runner.add_done_callback(lambda _: self.task_runners.pop(task["id"], None))

    # ---------- task events ----------
    async def _on_task_progress(self, task_id: int) -> None:
        now = time.time()
        last = self.progress_throttle.get(task_id, 0)
        if now - last < 8:
            return
        self.progress_throttle[task_id] = now
        await self._push_task_update(task_id)

    async def _on_task_complete(self, task_id: int) -> None:
        await self._push_task_update(task_id, force=True)
        task = self.db.get_collect_task(task_id)
        if not task:
            return
        await self._send_task_result(task["requester_id"], task_id, announce=True)

    async def _push_task_update(self, task_id: int, force: bool = False) -> None:
        task = self.db.get_collect_task(task_id)
        if not task or not task["progress_chat_id"] or not task["progress_message_id"]:
            return
        try:
            await self.application.bot.edit_message_text(
                chat_id=task["progress_chat_id"],
                message_id=task["progress_message_id"],
                text=self._format_task_text(task_id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._build_task_keyboard(task_id),
            )
        except BadRequest as exc:
            if "Message is not modified" not in str(exc):
                logger.warning("更新任务消息失败 task=%s: %s", task_id, exc)

    # ---------- formatting ----------
    def _build_welcome_text(self, user_id: int) -> str:
        lines = [
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>DM Collector Bot</b> <code>v{__version__}</code>",
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} {html.escape(self.settings.welcome_text, quote=False)}",
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} 第一版已接入：账号上传 / 状态检测 / 多频道采集 / txt 去重导出。",
        ]
        if self._is_admin(user_id):
            lines.append(f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} 你是管理员，可直接用下面按钮进入账号管理和采集中心。")
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
        lines = [
            f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>账号详情</b>",
            f"编号：<code>{account['id']}</code>",
            f"名称：<code>{html.escape(str(title), quote=False)}</code>",
            f"状态：{status_badge(account['status'])}",
            f"用户名：<code>{html.escape(str(account['username'] or '-'), quote=False)}</code>",
            f"手机号：<code>{html.escape(str(account['phone'] or '-'), quote=False)}</code>",
            f"User ID：<code>{account['tg_user_id'] or '-'}</code>",
            f"最近检测：<code>{account['last_checked_at'] or '-'}</code>",
        ]
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
        lines = [
            f"{tg_emoji(self.settings.emoji_history_id, '📝')} <b>任务 #{task_id}</b> <code>v{__version__}</code>",
            f"状态：{status_badge(task['status'])}",
            f"时间范围：最近 <code>{task['days_limit']}</code> 天",
            f"账号数：<code>{task['account_count']}</code> · 并发：<code>{task['worker_count']}</code>",
            f"频道进度：<code>{task['finished_channels']}/{task['total_channels']}</code>",
            f"扫描消息：<code>{task['total_messages_scanned']}</code>",
            f"命中总数：<code>{task['total_hits']}</code>",
            f"去重数量：<code>{task['unique_hits']}</code>",
        ]
        if task["last_error"] and task["status"] in {"error", "stopped"}:
            lines.append(f"错误：<code>{html.escape(str(task['last_error']), quote=False)}</code>")
        if visible_channels:
            lines.append("")
            lines.append("<b>频道子任务</b>")
            for item in visible_channels[:6]:
                lines.append(
                    f"• {html.escape(item['channel'], quote=False)} · {status_badge(item['status'])} · 扫描 <code>{item['scanned_messages']}</code> · 去重 <code>{item['unique_hits']}</code>"
                )
        return "\n".join(lines)

    def _upload_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_upload_id, '📷')} <b>上传 session</b>\n"
            f"请直接发送一个 <code>.session</code> 文件，或发送包含 <code>.session + .json</code> 的 <code>.zip</code> 压缩包。\n\n"
            f"收到后会自动：\n"
            f"1. 保存/解压文件\n2. 验证是否已登录\n3. 写入账号列表"
        )

    def _channels_prompt_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_idea_id, '💡')} <b>新建采集任务</b>\n"
            f"你可以：\n"
            f"1. 直接发频道列表（一行一个）\n"
            f"2. 上传一个 <code>.txt</code> 频道文件\n\n"
            f"支持格式：\n"
            f"<code>@channel</code>\n<code>https://t.me/channel</code>\n<code>t.me/channel</code>"
        )

    def _select_days_text(self, channels: list[str]) -> str:
        preview = "\n".join(f"• {html.escape(channel, quote=False)}" for channel in channels[:6])
        return (
            f"{tg_emoji(self.settings.emoji_waiting_id, '🕜')} <b>选择采集时间范围</b>\n"
            f"频道数：<code>{len(channels)}</code>\n\n{preview}"
        )

    def _format_check_all_progress_text(
        self,
        *,
        total: int,
        processed: int,
        current_label: str,
        kept_active: int,
        deleted_broken: int,
        deleted_banned: int,
        kept_other_errors: int,
        parallel: int,
    ) -> str:
        return "\n".join(
            [
                f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>正在批量检测账号</b>",
                f"进度：<code>{processed}/{total}</code> · 并发：<code>{parallel}</code>",
                f"最近完成：<code>{html.escape(current_label[:36], quote=False)}</code>",
                "",
                f"已确认可用：<code>{kept_active}</code>",
                f"已删损坏：<code>{deleted_broken}</code>",
                f"已删封禁/失效：<code>{deleted_banned}</code>",
                f"其他异常：<code>{kept_other_errors}</code>",
                "",
                "账号较多时会持续刷新这里，不是卡住。",
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

    def _select_accounts_text(self, channels: list[str], days: int, selected_ids: list[int]) -> str:
        active = self.db.get_active_accounts()
        lines = [
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} <b>选择采集账号</b>",
            f"频道数：<code>{len(channels)}</code> · 时间范围：<code>{days}</code> 天",
            f"已选账号：<code>{len(selected_ids)}</code>",
            "",
        ]
        for row in active:
            mark = "已选" if row["id"] in selected_ids else "未选"
            label = row["username"] or row["phone"] or row["session_name"]
            lines.append(f"• #{row['id']} {html.escape(str(label), quote=False)} · {mark}")
        return "\n".join(lines)

    def _select_workers_text(self, draft: dict) -> str:
        max_workers = self._max_worker_count(draft)
        return (
            f"{tg_emoji(self.settings.emoji_progress_id, '🎚️')} <b>设置并发</b>\n"
            f"频道数：<code>{len(draft.get('channels') or [])}</code>\n"
            f"账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"当前最多可设：<code>{max_workers}</code>\n"
            f"并发会按 账号数 / 频道数 / 上限 取最小值。"
        )

    def _max_worker_count(self, draft: dict) -> int:
        channels = len(draft.get("channels") or [])
        accounts = len(draft.get("account_ids") or [])
        return max(1, min(channels or 1, accounts or 1, self.settings.max_collect_workers))

    def _collect_confirm_text(self, draft: dict) -> str:
        channels = draft.get("channels") or []
        preview = "\n".join(f"• {html.escape(channel, quote=False)}" for channel in channels[:6])
        return (
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} <b>确认启动采集</b>\n"
            f"频道数：<code>{len(channels)}</code>\n"
            f"时间范围：<code>{draft.get('days')}</code> 天\n"
            f"账号数：<code>{len(draft.get('account_ids') or [])}</code>\n"
            f"并发：<code>{draft.get('worker_count')}</code>\n\n"
            f"{preview}"
        )

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
                premium_button("采集中心", self.settings.emoji_progress_id, callback_data="menu:collect"),
            ],
            [
                premium_button("统计", self.settings.emoji_stats_id, callback_data="menu:stats"),
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
                premium_button("3 天", self.settings.emoji_waiting_id, callback_data="wizard:days:3"),
            ],
            [
                premium_button("7 天", self.settings.emoji_waiting_id, callback_data="wizard:days:7"),
                premium_button("15 天", self.settings.emoji_waiting_id, callback_data="wizard:days:15"),
            ],
            [
                premium_button("自定义", self.settings.emoji_idea_id, callback_data="wizard:days_custom"),
                premium_button("取消", self.settings.emoji_error_id, callback_data="wizard:cancel"),
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
                premium_button(f"#{row['id']} {str(title)[:28]}", icon, callback_data=f"wizard:acc:toggle:{row['id']}")
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

    def _build_workers_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        max_workers = self._max_worker_count(draft)
        presets = [1, 2, 3, 5, 10, 20, 30, 50]
        available = [value for value in presets if value <= max_workers]
        if max_workers not in available:
            available.append(max_workers)
        available = sorted(set(available))

        keyboard = []
        row_buffer = []
        for value in available:
            row_buffer.append(
                premium_button(f"{value} 线程", self.settings.emoji_progress_id, callback_data=f"wizard:wrk:{value}")
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

    def _build_task_keyboard(self, task_id: int) -> InlineKeyboardMarkup:
        task = self.db.get_collect_task(task_id)
        keyboard = [
            [
                premium_button("刷新任务", self.settings.emoji_refresh_id, callback_data=f"task:refresh:{task_id}"),
                premium_button("导出结果", self.settings.emoji_export_id, callback_data=f"task:export:{task_id}"),
            ],
        ]
        if task and task["status"] in {"queued", "running"}:
            keyboard.append([
                premium_button("停止任务", self.settings.emoji_timeout_id, callback_data=f"task:stop:{task_id}"),
                premium_button("返回任务列表", self.settings.emoji_back_id, callback_data="collect:tasks"),
            ])
        else:
            keyboard.append([
                premium_button("返回任务列表", self.settings.emoji_back_id, callback_data="collect:tasks"),
                premium_button("返回首页", self.settings.emoji_home_id, callback_data="menu:main"),
            ])
        return InlineKeyboardMarkup(keyboard)

    def _single_back_keyboard(self, callback_data: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[premium_button("返回", self.settings.emoji_back_id, callback_data=callback_data)]])

    # ---------- helpers ----------
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

    async def _send_task_result(self, chat_id: int, task_id: int, announce: bool = False) -> None:
        task = self.db.get_collect_task(task_id)
        if not task:
            return
        path = task["result_file_path"]
        if not path or not Path(path).exists():
            path = str(self.db.export_task_usernames_txt(task_id, self.settings.export_dir))
        caption_prefix = "采集结果已生成" if announce else f"任务 #{task_id} 结果导出"
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

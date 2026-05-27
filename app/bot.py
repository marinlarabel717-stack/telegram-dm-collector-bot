from __future__ import annotations

import html
import logging

from telegram import InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .config import Settings
from .database import Database
from .emoji import premium_button, tg_emoji

logger = logging.getLogger(__name__)


class DmCollectorBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = Database(settings.db_path)
        self.application = Application.builder().token(settings.bot_token).build()
        self.application.bot_data["settings"] = settings
        self.application.bot_data["db"] = self.db
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("stats", self.stats))
        self.application.add_handler(CommandHandler("export", self.export_data))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_callback, pattern=r"^admin:(stats|export)$"))
        self.application.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, self.capture_private_message)
        )

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat or not update.message:
            return

        self.db.upsert_user(
            update.effective_user,
            chat_id=update.effective_chat.id,
            increment_start=True,
        )
        await update.message.reply_text(
            self._build_welcome_text(update.effective_user.id),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._build_admin_keyboard(update.effective_user.id),
        )

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await self._show_stats(update)

    async def export_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        await self._send_export_files(update)

    async def handle_admin_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not update.effective_user:
            return
        if not await self._ensure_admin(update):
            return

        await query.answer()
        action = (query.data or "").split(":", 1)[-1]
        if action == "stats":
            await self._show_stats(update)
        elif action == "export":
            await self._send_export_files(update)

    async def capture_private_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat or not update.message:
            return

        user = update.effective_user
        chat = update.effective_chat
        message = update.message

        self.db.upsert_user(user, chat_id=chat.id, increment_message=True)
        raw_json = message.to_dict() if self.settings.save_raw_update else None
        self.db.save_message(
            message=message,
            tg_user_id=user.id,
            chat_id=chat.id,
            message_type=self._detect_message_type(message),
            raw_json=raw_json,
        )

        if self.settings.forward_to_admins:
            await self._fanout_to_admins(update)

        if self.settings.auto_reply_enabled:
            await message.reply_text(
                self._build_auto_reply_text(),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

    async def _show_stats(self, update: Update) -> None:
        stats = self.db.get_stats()
        text = self._build_stats_text(stats)
        keyboard = self._build_admin_keyboard(update.effective_user.id if update.effective_user else 0)

        if update.callback_query and update.callback_query.message:
            await update.callback_query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
            return

        if update.effective_message:
            await update.effective_message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )

    async def _send_export_files(self, update: Update) -> None:
        if not update.effective_chat:
            return

        await self.application.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action=ChatAction.UPLOAD_DOCUMENT,
        )
        export_dir = self.settings.db_path.parent / "exports"
        paths = self.db.export_csv(export_dir)

        with paths.users_csv.open("rb") as users_fp:
            await self.application.bot.send_document(
                chat_id=update.effective_chat.id,
                document=users_fp,
                filename=paths.users_csv.name,
                caption=self._build_export_caption("用户导出已生成"),
                parse_mode=ParseMode.HTML,
            )
        with paths.messages_csv.open("rb") as messages_fp:
            await self.application.bot.send_document(
                chat_id=update.effective_chat.id,
                document=messages_fp,
                filename=paths.messages_csv.name,
                caption=self._build_export_caption("消息导出已生成"),
                parse_mode=ParseMode.HTML,
            )

    async def _fanout_to_admins(self, update: Update) -> None:
        if not update.effective_user or not update.effective_chat or not update.effective_message:
            return

        user = update.effective_user
        chat = update.effective_chat
        message = update.effective_message

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
                await message.get_bot().send_message(
                    chat_id=admin_id,
                    text=prefix,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=self._build_admin_keyboard(admin_id),
                )
                await message.get_bot().copy_message(
                    chat_id=admin_id,
                    from_chat_id=chat.id,
                    message_id=message.message_id,
                )
            except Exception:
                logger.exception("转发私信到管理员失败: admin_id=%s user_id=%s", admin_id, user.id)

    def _build_welcome_text(self, user_id: int) -> str:
        lines = [
            f"{tg_emoji(self.settings.emoji_welcome_id, '🌠')} <b>DM Collector Bot</b>",
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} {html.escape(self.settings.welcome_text, quote=False)}",
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} 当前已启用 <b>PTB 20+</b> 链路。",
        ]
        if self._is_admin(user_id):
            lines.append(f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} 你是管理员，可直接用下方按钮查看统计或导出数据。")
        return "\n\n".join(lines)

    def _build_auto_reply_text(self) -> str:
        return (
            f"{tg_emoji(self.settings.emoji_success_id, '🆗')} "
            f"{html.escape(self.settings.auto_reply_text, quote=False)}"
        )

    def _build_stats_text(self, stats: dict[str, int]) -> str:
        return "\n".join(
            [
                f"{tg_emoji(self.settings.emoji_stats_id, '🧠')} <b>当前统计</b>",
                f"用户数：<code>{stats['users']}</code>",
                f"私信总数：<code>{stats['messages']}</code>",
                f"今日私信：<code>{stats['today_messages']}</code>",
            ]
        )

    def _build_export_caption(self, title: str) -> str:
        return f"{tg_emoji(self.settings.emoji_export_id, '🖥')} <b>{html.escape(title, quote=False)}</b>"

    def _build_admin_keyboard(self, user_id: int) -> InlineKeyboardMarkup | None:
        if not self._is_admin(user_id):
            return None
        keyboard = [[
            premium_button("查看统计", self.settings.emoji_stats_id, callback_data="admin:stats"),
            premium_button("导出数据", self.settings.emoji_export_id, callback_data="admin:export"),
        ]]
        return InlineKeyboardMarkup(keyboard)

    async def _ensure_admin(self, update: Update) -> bool:
        user = update.effective_user
        if user and self._is_admin(user.id):
            return True

        denied_text = (
            f"{tg_emoji(self.settings.emoji_inbox_id, '🔵')} "
            f"<b>无权限</b>\n这个命令目前只开放给管理员。"
        )
        if update.callback_query:
            await update.callback_query.answer("无权限", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text(denied_text, parse_mode=ParseMode.HTML)
        return False

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

    def _is_admin(self, user_id: int) -> bool:
        return user_id in self.settings.admin_ids

    def run(self) -> None:
        logger.info("DM Collector Bot 启动中，数据库：%s", self.settings.db_path)
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

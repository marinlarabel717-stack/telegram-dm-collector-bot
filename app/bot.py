from __future__ import annotations

import logging

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from .config import Settings
from .database import Database

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
        await update.message.reply_text(self.settings.welcome_text)

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.message:
            return
        if not self._is_admin(update.effective_user.id):
            return

        stats = self.db.get_stats()
        await update.message.reply_text(
            "\n".join(
                [
                    "📊 当前统计",
                    f"用户数：{stats['users']}",
                    f"私信总数：{stats['messages']}",
                    f"今日私信：{stats['today_messages']}",
                ]
            )
        )

    async def export_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat or not update.message:
            return
        if not self._is_admin(update.effective_user.id):
            return

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
        export_dir = self.settings.db_path.parent / "exports"
        paths = self.db.export_csv(export_dir)

        with paths.users_csv.open("rb") as users_fp:
            await update.message.reply_document(document=users_fp, filename=paths.users_csv.name, caption="用户导出")
        with paths.messages_csv.open("rb") as messages_fp:
            await update.message.reply_document(document=messages_fp, filename=paths.messages_csv.name, caption="消息导出")

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
            await message.reply_text(self.settings.auto_reply_text)

    async def _fanout_to_admins(self, update: Update) -> None:
        if not update.effective_user or not update.effective_chat or not update.effective_message:
            return

        user = update.effective_user
        chat = update.effective_chat
        message = update.effective_message
        lines = [
            "📥 新私信",
            f"用户ID：{user.id}",
            f"姓名：{user.full_name}",
            f"Chat ID：{chat.id}",
            f"类型：{self._detect_message_type(message)}",
        ]
        if user.username:
            lines.insert(2, f"用户名：@{user.username}")
        prefix = "\n".join(lines)

        for admin_id in self.settings.admin_ids:
            try:
                await message.get_bot().send_message(chat_id=admin_id, text=prefix)
                await message.get_bot().copy_message(
                    chat_id=admin_id,
                    from_chat_id=chat.id,
                    message_id=message.message_id,
                )
            except Exception:
                logger.exception("转发私信到管理员失败: admin_id=%s user_id=%s", admin_id, user.id)

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

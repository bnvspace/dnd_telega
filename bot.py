import asyncio
import html
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from telegram import Update
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from dotenv import load_dotenv

LIST_NOT_CREATED_TEXT: Final[str] = (
    "Общая повозка еще не создана.\n"
    "Используйте /init, чтобы создать и закрепить основное сообщение."
)
HEADER_EMOJI_ID: Final[str] = "5226656353744862682"
DEFAULT_LOOT_IMAGE_NAME: Final[str] = "телега.png"
MAIN_CAPTION_LIMIT: Final[int] = 1024


@dataclass
class ChatState:
    chat_id: int
    message_id: int | None = None
    items: list[str] = field(default_factory=list)


class Store:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_lists (
                    chat_id INTEGER PRIMARY KEY,
                    message_id INTEGER,
                    items_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def get_chat_state(self, chat_id: int) -> ChatState:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT chat_id, message_id, items_json FROM chat_lists WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        if row is None:
            return ChatState(chat_id=chat_id)

        items_raw = row["items_json"] or "[]"
        try:
            items = json.loads(items_raw)
            if not isinstance(items, list):
                items = []
        except json.JSONDecodeError:
            items = []
        normalized_items = [str(item).strip() for item in items if str(item).strip()]

        return ChatState(
            chat_id=row["chat_id"],
            message_id=row["message_id"],
            items=normalized_items,
        )

    def save_chat_state(self, state: ChatState) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        payload = json.dumps(state.items, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO chat_lists (chat_id, message_id, items_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    message_id = excluded.message_id,
                    items_json = excluded.items_json,
                    updated_at = excluded.updated_at
                """,
                (state.chat_id, state.message_id, payload, now, now),
            )
            conn.commit()


class ChatLockManager:
    def __init__(self) -> None:
        self._locks: dict[int, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    async def get_lock(self, chat_id: int) -> asyncio.Lock:
        async with self._guard:
            lock = self._locks.get(chat_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[chat_id] = lock
            return lock


class MainMessageUnavailable(Exception):
    """Raised when main list message can no longer be edited."""


class MainMessageNeedsRecreate(Exception):
    """Raised when main message exists but must be recreated as a photo."""


def resolve_loot_image_path() -> str:
    configured = os.getenv("LOOT_IMAGE_PATH", "").strip()
    if configured:
        return configured
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_LOOT_IMAGE_NAME)


def render_list(items: list[str]) -> str:
    lines = [f'<tg-emoji emoji-id="{HEADER_EMOJI_ID}">🛒</tg-emoji> <b>ОБЩАЯ ПОВОЗКА</b>', ""]
    if not items:
        lines.append("Список пуст.")
    else:
        for idx, item in enumerate(items, start=1):
            candidate = f"{idx}. {html.escape(item)}"
            preview = "\n".join([*lines, candidate])
            if len(preview) > MAIN_CAPTION_LIMIT:
                hidden = len(items) - idx + 1
                lines.append(f"... и еще {hidden} поз.")
                break
            lines.append(candidate)
    caption = "\n".join(lines)
    if len(caption) <= MAIN_CAPTION_LIMIT:
        return caption
    return caption[: MAIN_CAPTION_LIMIT - 3] + "..."


def get_store(context: ContextTypes.DEFAULT_TYPE) -> Store:
    return context.application.bot_data["store"]


def get_lock_manager(context: ContextTypes.DEFAULT_TYPE) -> ChatLockManager:
    return context.application.bot_data["locks"]


def is_group_chat(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.type in {"group", "supergroup"}


async def edit_main_message(
    context: ContextTypes.DEFAULT_TYPE,
    state: ChatState,
) -> None:
    if state.message_id is None:
        raise MainMessageUnavailable("message_id is empty")

    caption = render_list(state.items)
    try:
        await context.bot.edit_message_caption(
            chat_id=state.chat_id,
            message_id=state.message_id,
            caption=caption,
            parse_mode="HTML",
        )
    except BadRequest as exc:
        msg = str(exc).lower()
        if "message is not modified" in msg:
            return
        if (
            "there is no caption in the message to edit" in msg
            or "message is not a media message" in msg
            or "can't edit message caption" in msg
        ):
            raise MainMessageNeedsRecreate(str(exc)) from exc
        if "message to edit not found" in msg or "message can't be edited" in msg:
            raise MainMessageUnavailable(str(exc)) from exc
        raise
    except Forbidden as exc:
        raise MainMessageUnavailable(str(exc)) from exc


async def create_main_message_with_photo(
    context: ContextTypes.DEFAULT_TYPE,
    state: ChatState,
) -> None:
    image_path = resolve_loot_image_path()
    if not os.path.isfile(image_path):
        raise FileNotFoundError(image_path)

    caption = render_list(state.items)
    with open(image_path, "rb") as image_file:
        msg = await context.bot.send_photo(
            chat_id=state.chat_id,
            photo=image_file,
            caption=caption,
            parse_mode="HTML",
        )
    state.message_id = msg.message_id


async def refresh_main_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    store: Store,
    state: ChatState,
) -> bool:
    try:
        await edit_main_message(context, state)
        return True
    except MainMessageNeedsRecreate:
        old_message_id = state.message_id
        try:
            await create_main_message_with_photo(context, state)
        except FileNotFoundError:
            if update.effective_message is not None:
                await update.effective_message.reply_text(
                    "Не найден файл изображения повозки. Положите `телега.png` рядом с bot.py."
                )
            return False
        store.save_chat_state(state)
        if old_message_id and old_message_id != state.message_id:
            try:
                await context.bot.delete_message(
                    chat_id=state.chat_id,
                    message_id=old_message_id,
                )
            except BadRequest:
                pass
            except Forbidden:
                pass
        return True
    except MainMessageUnavailable:
        state.message_id = None
        store.save_chat_state(state)
        if update.effective_message is not None:
            await update.effective_message.reply_text(LIST_NOT_CREATED_TEXT)
        return False


async def ensure_group(update: Update) -> bool:
    if is_group_chat(update):
        return True
    if update.effective_message:
        await update.effective_message.reply_text(
            "Этот бот рассчитан на групповой чат. Добавьте его в группу и выдайте право писать сообщения."
        )
    return False


def parse_quick_action(text: str) -> tuple[str, str | None] | None:
    raw = text.strip()
    if not raw:
        return None

    low = raw.casefold()
    if raw.startswith("+"):
        value = raw[1:].strip()
        if value:
            return "add", value
        return None
    if raw.startswith("-"):
        value = raw[1:].strip()
        if value:
            return "del", value
        return None
    if low in {"clear", "очистить", "wipe"}:
        return "clear", None
    if low in {"show", "list", "список", "?"}:
        return "show", None
    return None


async def help_command(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None:
        return
    await update.effective_message.reply_text(
        "Команды:\n"
        "/init - создать или восстановить общее сообщение\n"
        "/add <текст> - положить в повозку\n"
        "/del <номер|текст> - достать из повозки\n"
        "/clear - полностью очистить список\n"
        "/show - принудительно обновить отображение\n"
        "\n"
        "Алиасы:\n"
        "/put, /loot = /add\n"
        "/take, /drop, /rm = /del\n"
        "/inv, /list = /show\n"
        "\n"
        "Быстрый режим (ответом на закрепленный список):\n"
        "+ зелье лечения\n"
        "- 3\n"
        "clear"
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message is None or update.effective_chat is None:
        return

    if update.effective_chat.type == "private":
        await update.effective_message.reply_text(
            "Инструкция по запуску общей повозки:\n"
            "1. Добавьте бота в нужную группу.\n"
            "2. Дайте боту право отправлять сообщения.\n"
            "3. В группе выполните /init.\n"
            "4. Закрепите созданное сообщение.\n"
            "\n"
            "Как пользоваться в группе:\n"
            "- /add <предмет>\n"
            "- /del <номер|текст>\n"
            "- /clear\n"
            "- /show\n"
            "\n"
            "Быстрый режим (reply на закреп):\n"
            "+ веревка 15м\n"
            "- 2\n"
            "clear"
        )
        return

    await help_command(update, context)


async def init_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)

        if state.message_id is not None:
            try:
                if not await refresh_main_message(update, context, store, state):
                    return
                await update.effective_message.reply_text(
                    "Повозка уже существует и была обновлена."
                )
                return
            except MainMessageUnavailable:
                state.message_id = None

        try:
            await create_main_message_with_photo(context, state)
        except FileNotFoundError:
            await update.effective_message.reply_text(
                "Не найден файл изображения повозки. Положите `телега.png` рядом с bot.py."
            )
            return
        store.save_chat_state(state)

    await update.effective_message.reply_text(
        "Повозка создана. Закрепите это сообщение для постоянного использования."
    )


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return

    item_text = " ".join(context.args).strip()
    if not item_text:
        await update.effective_message.reply_text("Использование: /add <текст>")
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)
        if state.message_id is None:
            await update.effective_message.reply_text(LIST_NOT_CREATED_TEXT)
            return

        state.items.append(item_text)
        store.save_chat_state(state)
        if not await refresh_main_message(update, context, store, state):
            return

    # Silent success: main message is updated without extra chat noise.


def pop_item(items: list[str], selector: str) -> str | None:
    token = selector.strip()
    if token.isdigit():
        idx = int(token) - 1
        if 0 <= idx < len(items):
            return items.pop(idx)
        return None

    lowered = token.casefold()
    for idx, value in enumerate(items):
        if value.casefold() == lowered:
            return items.pop(idx)
    return None


async def del_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return

    selector = " ".join(context.args).strip()
    if not selector:
        await update.effective_message.reply_text("Использование: /del <номер|текст>")
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)
        if state.message_id is None:
            await update.effective_message.reply_text(LIST_NOT_CREATED_TEXT)
            return

        removed = pop_item(state.items, selector)
        if removed is None:
            await update.effective_message.reply_text(
                "Ничего не найдено для удаления. Проверьте номер или точный текст."
            )
            return

        store.save_chat_state(state)
        if not await refresh_main_message(update, context, store, state):
            return

    # Silent success: main message is updated without extra chat noise.


async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)
        if state.message_id is None:
            await update.effective_message.reply_text(LIST_NOT_CREATED_TEXT)
            return

        state.items.clear()
        store.save_chat_state(state)
        if not await refresh_main_message(update, context, store, state):
            return

    await update.effective_message.reply_text("Повозка очищена.")


async def show_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)
        if state.message_id is None:
            await update.effective_message.reply_text(LIST_NOT_CREATED_TEXT)
            return
        if not await refresh_main_message(update, context, store, state):
            return

    await update.effective_message.reply_text("Основное сообщение обновлено.")


async def quick_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return
    if not await ensure_group(update):
        return
    if update.effective_user and update.effective_user.is_bot:
        return

    incoming_text = (update.effective_message.text or "").strip()
    action = parse_quick_action(incoming_text)
    if action is None:
        return

    chat_id = update.effective_chat.id
    store = get_store(context)
    locks = get_lock_manager(context)
    lock = await locks.get_lock(chat_id)

    async with lock:
        state = store.get_chat_state(chat_id)
        if state.message_id is None:
            return

        replied = update.effective_message.reply_to_message
        if replied is None or replied.message_id != state.message_id:
            return

        mode, payload = action
        if mode == "add" and payload:
            state.items.append(payload)
            store.save_chat_state(state)
            if not await refresh_main_message(update, context, store, state):
                return
            return

        if mode == "del" and payload:
            removed = pop_item(state.items, payload)
            if removed is None:
                await update.effective_message.reply_text(
                    "Не нашел такой позиции в повозке."
                )
                return
            store.save_chat_state(state)
            if not await refresh_main_message(update, context, store, state):
                return
            return

        if mode == "clear":
            state.items.clear()
            store.save_chat_state(state)
            if not await refresh_main_message(update, context, store, state):
                return
            await update.effective_message.reply_text("Повозка очищена.")
            return

        if mode == "show":
            if not await refresh_main_message(update, context, store, state):
                return
            await update.effective_message.reply_text("Список обновлен.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Unhandled error while processing update: %s", update, exc_info=context.error)


def build_application(token: str, db_path: str) -> Application:
    app = ApplicationBuilder().token(token).build()
    app.bot_data["store"] = Store(db_path)
    app.bot_data["locks"] = ChatLockManager()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("init", init_command))
    app.add_handler(CommandHandler(["add", "put", "loot"], add_command))
    app.add_handler(CommandHandler(["del", "take", "drop", "rm"], del_command))
    app.add_handler(CommandHandler(["clear", "wipe"], clear_command))
    app.add_handler(CommandHandler(["show", "list", "inv"], show_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, quick_action_handler))
    app.add_error_handler(on_error)

    return app


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        level=logging.INFO,
    )
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    db_path = os.getenv("STATE_DB_PATH", "bot_state.sqlite3").strip()
    application = build_application(token=token, db_path=db_path)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

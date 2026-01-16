from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramForbiddenError
from aiogram.types import CallbackQuery, Message


class DmRequiredMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self._cache: Dict[int, bool] = {}

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        message: Message | None = None
        if isinstance(event, CallbackQuery):
            message = event.message
        elif isinstance(event, Message):
            message = event
        if not message or not message.chat:
            return await handler(event, data)
        if message.chat.type == "private":
            return await handler(event, data)
        user = getattr(event, "from_user", None)
        if not user:
            return None
        allowed = self._cache.get(int(user.id))
        if allowed is None:
            try:
                await event.bot.send_chat_action(int(user.id), ChatAction.TYPING)
            except TelegramForbiddenError:
                self._cache[int(user.id)] = False
                await _notify_dm_required(event, message)
                return None
            self._cache[int(user.id)] = True
            return await handler(event, data)
        if not allowed:
            await _notify_dm_required(event, message)
            return None
        return await handler(event, data)


async def _notify_dm_required(event: Any, message: Message) -> None:
    text = "Открой личку с ботом и начни переписку, иначе кнопки не работают."
    if isinstance(event, CallbackQuery):
        try:
            await event.answer(text, show_alert=True)
        except Exception:
            pass
    else:
        try:
            await message.answer(text)
        except Exception:
            pass

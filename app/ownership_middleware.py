from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery

from app.ownership import get_owner
from config import ADMIN_BROADCAST_USER_ID


class OwnershipMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        message = getattr(event, "message", None)
        user = getattr(event, "from_user", None)
        payload = getattr(event, "data", "") or ""
        if user and int(user.id) == int(ADMIN_BROADCAST_USER_ID):
            return await handler(event, data)
        if isinstance(payload, str) and payload.startswith("db_"):
            return await handler(event, data)
        if message and user:
            owner_id = get_owner(message.chat.id, message.message_id)
            if owner_id is not None and owner_id != user.id:
                await event.answer("Это не твоя кнопка.", show_alert=False)
                return None
        return await handler(event, data)

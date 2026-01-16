from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware

from app.repo import delete_broadcast_chat, upsert_broadcast_chat


class ChatRegistryMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        await self._track_chat(event, data)
        return await handler(event, data)

    async def _track_chat(self, event: Any, data: Dict[str, Any]) -> None:
        chat = getattr(event, "chat", None)
        if not chat:
            return
        chat_type = str(getattr(chat, "type", "") or "")
        if not chat_type or chat_type == "private":
            return
        db_pool = data.get("db_pool")
        if not db_pool:
            return
        chat_id = int(getattr(chat, "id", 0) or 0)
        if not chat_id:
            return
        new_member = getattr(event, "new_chat_member", None)
        if new_member is not None:
            status = str(getattr(new_member, "status", "") or "")
            if status in {"left", "kicked"}:
                await delete_broadcast_chat(db_pool, chat_id)
                return
        from_user = getattr(event, "from_user", None)
        added_by = int(from_user.id) if from_user else None
        title = str(getattr(chat, "title", "") or "")
        username = str(getattr(chat, "username", "") or "")
        await upsert_broadcast_chat(
            db_pool,
            chat_id=chat_id,
            chat_type=chat_type,
            title=title,
            username=username,
            added_by=added_by,
        )

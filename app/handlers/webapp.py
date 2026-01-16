from __future__ import annotations

import json

from aiogram import Router, F
from aiogram.types import Message

from app.handlers.donate import _send_stars_menu
from app.repo import get_or_create_user, get_user

router = Router()


@router.message(F.web_app_data)
async def webapp_data_handler(message: Message, db_pool, rate_limiter) -> None:
    payload_raw = message.web_app_data.data if message.web_app_data else ""
    if not payload_raw:
        return
    if not message.from_user:
        return
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        return
    if payload.get("action") != "open_stars":
        return
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        user = await get_or_create_user(
            db_pool,
            message.from_user.id,
            message.from_user.full_name or "",
            message.from_user.username or "",
        )
    await _send_stars_menu(
        message,
        db_pool,
        user,
        rate_limiter=rate_limiter,
        prefer_edit=False,
        owner_id=message.from_user.id,
    )

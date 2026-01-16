from __future__ import annotations

import asyncio

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

from app.repo import fetch_all_users
from app.ratelimit import RateLimiter
from config import ADMIN_BROADCAST_USER_ID

router = Router()


@router.message(Command("text"))
async def broadcast_text_command(
    message: Message,
    db_pool,
    rate_limiter: RateLimiter,
) -> None:
    if not message.from_user:
        return
    if message.from_user.id != int(ADMIN_BROADCAST_USER_ID):
        return
    raw_text = message.text or ""
    text = raw_text.partition(" ")[2].strip()
    if not text:
        await message.answer("Используй: /text <сообщение>")
        return
    users = await fetch_all_users(db_pool)
    if not users:
        await message.answer("Нет юзеров в базе.")
        return

    sent = 0
    failed = 0
    for user in users:
        uid = int(user.get("user_id", 0))
        if uid <= 0:
            continue
        if rate_limiter:
            await rate_limiter.acquire(uid)
        try:
            await message.bot.send_message(chat_id=uid, text=text)
            sent += 1
        except TelegramRetryAfter as exc:
            if rate_limiter:
                await rate_limiter.register_retry_after(exc.retry_after)
            await asyncio.sleep(max(0.1, float(exc.retry_after)))
            failed += 1
        except TelegramForbiddenError:
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.03)

    await message.answer(f"Готово. Отправлено: {sent}, ошибок: {failed}.")

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.giveaway import announce_giveaway, ensure_giveaway, giveaway_phase
from app.repo import (
    adjust_user_balance,
    adjust_user_free_rolls,
    adjust_user_stars,
    fetch_all_users,
    fetch_showcase_active_cards_grouped,
    update_last_reminder_bulk,
    update_user_fields,
)
from app.ratelimit import RateLimiter
from app.utils import now_local
from app.showcase import summarize_showcase_effects
from config import (
    GIVEAWAY_TICK_SEC,
    PUBLIC_BOT_USERNAME,
    REMINDER_INTERVAL_SEC,
    REMINDER_TICK_SEC,
    VIP_INFINITE_DAYS,
)


async def reminder_loop(bot: Bot, db_pool, rate_limiter: RateLimiter) -> None:
    interval_sec = REMINDER_INTERVAL_SEC
    tick_sec = REMINDER_TICK_SEC
    username = (PUBLIC_BOT_USERNAME or "").lstrip("@")
    startgroup_url = f"https://t.me/{username}?startgroup=true" if username else None
    text = "\n".join(
        [
            "Добавьте бота в свой чат!",
            "",
            "Кнопка ниже откроет меню добавления в чат.",
        ]
    )
    reply_markup = None
    if startgroup_url:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Добавить в чат", url=startgroup_url)]
            ]
        )

    while True:
        users = await fetch_all_users(db_pool)
        now = datetime.now(timezone.utc)
        touch_ids: List[int] = []
        for user in users:
            uid = int(user.get("user_id", 0))
            if uid <= 0:
                continue
            last = user.get("last_reminder_at")
            if isinstance(last, datetime):
                if (now - last).total_seconds() < interval_sec:
                    continue
            if rate_limiter:
                await rate_limiter.acquire(uid)
            try:
                await bot.send_message(
                    chat_id=uid,
                    text=text,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                touch_ids.append(uid)
            except TelegramForbiddenError:
                touch_ids.append(uid)
            except TelegramRetryAfter as exc:
                if rate_limiter:
                    await rate_limiter.register_retry_after(exc.retry_after)
                await asyncio.sleep(max(0.1, float(exc.retry_after)))
            except Exception:
                pass
            await asyncio.sleep(0.03)
        if touch_ids:
            await update_last_reminder_bulk(db_pool, touch_ids, now)
        await asyncio.sleep(tick_sec)


async def giveaway_loop(
    bot: Bot,
    db_pool,
    card_map,
) -> None:
    tick_sec = GIVEAWAY_TICK_SEC
    while True:
        try:
            giveaway = await ensure_giveaway(db_pool)
            if giveaway:
                phase = giveaway_phase(now_local())
                if phase == "announce" and giveaway.get("status") != "announced":
                    await announce_giveaway(db_pool, bot, giveaway, card_map)
        except Exception:
            pass
        await asyncio.sleep(tick_sec)


async def showcase_bonus_loop(db_pool) -> None:
    while True:
        now = now_local()
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        await asyncio.sleep(max(1.0, (next_midnight - now).total_seconds()))
        today = now_local().date()
        grouped = await fetch_showcase_active_cards_grouped(db_pool)
        if not grouped:
            continue
        users = await fetch_all_users(db_pool)
        user_map = {int(user.get("user_id", 0)): user for user in users}
        for user_id, cards in grouped.items():
            user = user_map.get(user_id)
            if not user:
                continue
            if user.get("showcase_daily_date") == today:
                continue
            effects = summarize_showcase_effects(cards)
            balance_daily = int(effects.get("balance_daily") or 0)
            free_rolls = int(effects.get("free_rolls_daily") or 0)
            kazik_spins = int(effects.get("kazik_spins_daily") or 0)
            stars_daily = int(effects.get("stars_daily") or 0)
            vip_infinite = int(effects.get("vip_infinite") or 0)
            if not any([balance_daily, free_rolls, kazik_spins, stars_daily, vip_infinite]):
                continue
            if balance_daily:
                await adjust_user_balance(db_pool, user_id, balance_daily)
            if free_rolls:
                await adjust_user_free_rolls(db_pool, user_id, free_rolls)
            if stars_daily:
                await adjust_user_stars(db_pool, user_id, stars_daily)
            updates = {"showcase_daily_date": today}
            if kazik_spins:
                bonus = int(user.get("kazik_bonus_spins", 0) or 0) + kazik_spins
                updates["kazik_bonus_spins"] = max(0, bonus)
            if vip_infinite:
                updates["vip_until"] = now_local() + timedelta(days=VIP_INFINITE_DAYS)
                updates["vip"] = True
            await update_user_fields(db_pool, user_id, updates)


async def run_background_tasks(
    bot: Bot,
    db_pool,
    cards_by_rarity,
    card_map,
    rate_limiter: RateLimiter,
) -> None:
    asyncio.create_task(reminder_loop(bot, db_pool, rate_limiter))
    asyncio.create_task(giveaway_loop(bot, db_pool, card_map))
    asyncio.create_task(showcase_bonus_loop(db_pool))

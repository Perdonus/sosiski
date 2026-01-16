from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.images import build_leaderboard_image
from app.logic import compute_leaderboard
from app.messages import send_or_edit_media
from app.repo import fetch_all_users, fetch_inventory_map
from app.telegram_utils import fetch_user_avatar
from config import TOP_LIMIT

router = Router()


@router.message(Command("top"))
async def top_command(
    message: Message,
    db_pool,
    card_map: Dict[str, object],
    rate_limiter,
    *,
    owner_id: Optional[int] = None,
) -> None:
    if owner_id is None and message.from_user:
        owner_id = message.from_user.id
    users = await fetch_all_users(db_pool)
    inventory_map = await fetch_inventory_map(db_pool)
    leaderboard, total_users = compute_leaderboard(
        users, inventory_map, card_map, TOP_LIMIT
    )
    donors = sorted(
        users,
        key=lambda user: int(user.get("stars_donated", 0) or 0),
        reverse=True,
    )[:TOP_LIMIT]
    user_map = {int(user.get("user_id", 0)): user for user in users}

    tasks: List[asyncio.Task] = []
    for uid, _, _, _ in leaderboard:
        entry = user_map.get(uid, {})
        tasks.append(
            fetch_user_avatar(
                message.bot,
                uid,
                cached_file_id=entry.get("avatar_file_id"),
                db_pool=db_pool,
            )
        )
    donor_tasks: List[asyncio.Task] = []
    for donor in donors:
        donor_id = int(donor.get("user_id", 0))
        donor_tasks.append(
            fetch_user_avatar(
                message.bot,
                donor_id,
                cached_file_id=donor.get("avatar_file_id"),
                db_pool=db_pool,
            )
        )
    avatars = await asyncio.gather(*tasks, return_exceptions=False) if tasks else []
    donor_avatars = (
        await asyncio.gather(*donor_tasks, return_exceptions=False)
        if donor_tasks
        else []
    )

    leaderboard_entries = [
        (
            name,
            total,
            avatar_bytes,
            vip,
            bool(user_map.get(uid, {}).get("is_admin")),
        )
        for (uid, name, total, vip), avatar_bytes in zip(leaderboard, avatars)
    ]
    donor_entries = [
        (
            str(donor.get("username") or "Без имени").strip() or "Без имени",
            int(donor.get("stars_donated", 0) or 0),
            avatar_bytes,
            False,
            bool(donor.get("is_admin")),
        )
        for donor, avatar_bytes in zip(donors, donor_avatars)
    ]
    leaderboard_image = build_leaderboard_image(
        leaderboard_entries, donor_entries, total_users
    )
    back_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="menu")]]
    )
    await send_or_edit_media(
        message,
        leaderboard_image,
        "Топы",
        back_keyboard,
        prefer_edit=False,
        rate_limiter=rate_limiter,
        owner_id=owner_id,
    )


@router.callback_query(F.data == "cmd|top")
async def top_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, object],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await top_command(
        query.message,
        db_pool=db_pool,
        card_map=card_map,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()

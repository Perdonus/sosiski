from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
from aiogram.types import BufferedInputFile, CallbackQuery, Message
from asyncpg import Pool

from app.images import build_profile_image, get_cached_menu_image
from app.keyboards import (
    build_donate_menu_keyboard,
    build_kazik_open_dm_keyboard,
    build_kazik_webapp_keyboard,
    build_main_menu_keyboard,
    build_roll_menu_keyboard,
    build_sausages_menu_keyboard,
)
from app.logic import compute_rank, get_cooldown_seconds, inventory_value, is_vip, total_wealth
from app.messages import send_or_edit_media
from app.handlers.donate import _send_stars_menu as send_stars_menu
from app.repo import (
    fetch_all_users,
    fetch_inventory_map,
    get_or_create_user,
    get_user,
    update_user_fields,
)
from app.telegram_utils import fetch_user_avatar
from app.utils import (
    format_duration,
    format_vip_remaining,
    get_user_label,
    greeting_by_time,
    parse_referrer_id,
)

router = Router()


async def send_main_menu(
    message: Message,
    *,
    db_pool: Pool,
    card_map: Dict[str, object],
    rate_limiter,
    prefer_edit: bool = False,
    tg_user=None,
) -> None:
    if tg_user is None:
        tg_user = message.from_user
    if not tg_user:
        return
    user = await get_or_create_user(
        db_pool, tg_user.id, tg_user.full_name or "", tg_user.username or ""
    )
    users = await fetch_all_users(db_pool)
    inventory_map = await fetch_inventory_map(db_pool)
    totals = []
    for entry in users:
        uid = int(entry.get("user_id", 0))
        items = inventory_map.get(uid, [])
        total_val = inventory_value(items, card_map)
        totals.append((uid, total_wealth(entry, total_val)))
    rank, total_users = compute_rank(totals, tg_user.id)
    user_items = inventory_map.get(tg_user.id, [])
    total_value = inventory_value(user_items, card_map)
    balance = int(user.get("balance", 0) or 0)
    stars = int(user.get("stars", 0) or 0)
    vip = is_vip(user)
    is_admin = bool(user.get("is_admin"))

    avatar_bytes = await fetch_user_avatar(
        message.bot,
        tg_user.id,
        cached_file_id=user.get("avatar_file_id"),
        db_pool=db_pool,
    )
    profile_image = build_profile_image(
        tg_user.full_name or "",
        rank,
        total_users,
        total_value,
        balance,
        stars,
        vip,
        is_admin,
        avatar_bytes,
    )
    user_label = get_user_label(tg_user)
    now = datetime.now(timezone.utc)
    vip_until = user.get("vip_until")
    vip_left = 0
    if isinstance(vip_until, datetime) and vip_until > now:
        vip_left = int((vip_until - now).total_seconds())
    caption_lines = [f"{greeting_by_time()}, {user_label}!"]
    caption_lines.append(f"Общая цена сосисок: {total_value}")
    caption_lines.append(f"Звёзд на балансе: {stars}⭐")
    if vip_left:
        caption_lines.append(f"VIP осталось: {format_vip_remaining(vip_left)}")
    else:
        caption_lines.append("VIP: нет")
    caption = "\n".join(caption_lines)
    photo = BufferedInputFile(profile_image.getvalue(), filename="profile.jpg")
    await send_or_edit_media(
        message,
        photo,
        caption,
        build_main_menu_keyboard(),
        prefer_edit,
        rate_limiter=rate_limiter,
        owner_id=tg_user.id,
    )


@router.message(CommandStart())
async def start_command(
    message: Message,
    command: CommandObject,
    db_pool: Pool,
    card_map: Dict[str, object],
    rate_limiter,
) -> None:
    tg_user = message.from_user
    if not tg_user:
        return
    user = await get_user(db_pool, tg_user.id)
    is_new = not user
    if is_new:
        user = await get_or_create_user(
            db_pool, tg_user.id, tg_user.full_name or "", tg_user.username or ""
        )

    payload = (command.args or "").strip()
    if payload.lower() in {"pay", "stars", "donate"} and message.chat.type == "private":
        if not user:
            user = await get_or_create_user(
                db_pool, tg_user.id, tg_user.full_name or "", tg_user.username or ""
            )
        await send_stars_menu(
            message,
            db_pool,
            user,
            rate_limiter=rate_limiter,
            prefer_edit=False,
            owner_id=tg_user.id,
        )
        return
    if message.chat.type == "private" and is_new and payload:
        referrer_id = parse_referrer_id(payload)
        if referrer_id and referrer_id != str(tg_user.id):
            ref_user = await get_user(db_pool, int(referrer_id))
            if ref_user:
                bonus = int(user.get("kazik_bonus_spins", 0) or 0) + 1
                await update_user_fields(
                    db_pool,
                    tg_user.id,
                    {"referred_by": int(referrer_id), "kazik_bonus_spins": bonus},
                )
                ref_bonus = int(ref_user.get("kazik_bonus_spins", 0) or 0) + 1
                await update_user_fields(
                    db_pool,
                    int(referrer_id),
                    {"kazik_bonus_spins": ref_bonus},
                )
                try:
                    await message.bot.send_message(
                        int(referrer_id),
                        f"По твоей реферальной ссылке зашёл {get_user_label(tg_user)}. +1 фри спин в Казике.",
                    )
                except Exception:
                    pass

    await send_main_menu(
        message, db_pool=db_pool, card_map=card_map, rate_limiter=rate_limiter
    )


@router.callback_query(F.data == "menu")
async def menu_callback(
    query: CallbackQuery,
    db_pool: Pool,
    card_map: Dict[str, object],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await send_main_menu(
        query.message,
        db_pool=db_pool,
        card_map=card_map,
        rate_limiter=rate_limiter,
        prefer_edit=True,
        tg_user=query.from_user,
    )
    await query.answer()


@router.callback_query(F.data == "cmd|games")
async def games_callback(query: CallbackQuery) -> None:
    if not query.message:
        return
    if query.message.chat.type != "private":
        await query.message.answer(
            "Игры в mini apps.",
            reply_markup=build_kazik_open_dm_keyboard(),
        )
    else:
        await query.message.answer(
            "Игры в mini apps.",
            reply_markup=build_kazik_webapp_keyboard(),
        )
    await query.answer()


@router.callback_query(F.data == "cmd|upgrade_web")
async def upgrade_web_callback(query: CallbackQuery) -> None:
    if not query.message:
        return
    if query.message.chat.type != "private":
        await query.message.answer(
            "Игры в mini apps.",
            reply_markup=build_kazik_open_dm_keyboard(),
        )
    else:
        await query.message.answer(
            "Игры в mini apps.",
            reply_markup=build_kazik_webapp_keyboard(),
        )
    await query.answer()


@router.callback_query(F.data == "roll_menu")
async def roll_menu_callback(query: CallbackQuery, db_pool: Pool, rate_limiter) -> None:
    message = query.message
    tg_user = query.from_user
    if not message or not tg_user:
        return
    user = await get_user(db_pool, tg_user.id)
    cooldown = get_cooldown_seconds(user)
    last_roll = user.get("last_roll_at")
    roll_left = 0
    if isinstance(last_roll, datetime):
        diff = datetime.now(timezone.utc) - last_roll
        roll_left = max(0, cooldown - int(diff.total_seconds()))
    roll_line = (
        f"До след. крутки: {format_duration(roll_left)}"
        if roll_left > 0
        else "До след. крутки: доступно"
    )
    caption_lines = [roll_line]
    menu_path = get_cached_menu_image("roll", "Крутки", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "\n".join(caption_lines),
            build_roll_menu_keyboard(),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=tg_user.id,
        )
    await query.answer()


@router.callback_query(F.data == "sausages_menu")
async def sausages_menu_callback(query: CallbackQuery, rate_limiter) -> None:
    message = query.message
    if not message:
        return
    menu_path = get_cached_menu_image("sausages", "Сосиски", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "",
            build_sausages_menu_keyboard(),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=query.from_user.id,
        )
    await query.answer()


@router.callback_query(F.data == "donate_menu")
async def donate_menu_callback(query: CallbackQuery, db_pool: Pool, rate_limiter) -> None:
    message = query.message
    tg_user = query.from_user
    if not message or not tg_user:
        return
    user = await get_user(db_pool, tg_user.id)
    stars = int(user.get("stars", 0) or 0)
    now = datetime.now(timezone.utc)
    vip_until = user.get("vip_until")
    if isinstance(vip_until, datetime) and vip_until > now:
        left = int((vip_until - now).total_seconds())
        status = f"Осталось: {format_vip_remaining(left)}"
    else:
        status = "VIP: нет"
    caption = "\n".join(
        [
            status,
            f"Звёзд на балансе: {stars}⭐",
        ]
    )
    menu_path = get_cached_menu_image("donate", "Донат", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_donate_menu_keyboard(),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=tg_user.id,
        )
    await query.answer()

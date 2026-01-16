from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Dict, List, Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

from app.giveaway import get_giveaway_schedule, schedule_giveaway, set_giveaway_schedule
from app.images import get_cached_menu_image, get_card_media_path
from app.keyboards import (
    build_giveaway_card_nav_keyboard,
    build_giveaway_date_keyboard,
    build_giveaway_delete_keyboard,
    build_giveaway_place_type_keyboard,
    build_giveaway_places_keyboard,
    build_giveaway_vip_duration_keyboard,
    build_rarity_keyboard,
)
from app.messages import send_or_edit_media
from app.ownership import remember_owner
from app.repo import (
    delete_broadcast_chat,
    fetch_all_users,
    fetch_broadcast_chats,
    get_exclusive_stock,
    get_kv,
    set_kv,
    sync_exclusive_stock,
    update_exclusive_reserved,
)
from app.utils import format_short_amount, now_local
from cards import Card, card_currency, card_display_name, filter_existing_cards
from config import ADMIN_BROADCAST_USER_ID, EXCLUSIVE_STOCK_LIMIT, RARITY_NAMES, RARITY_ORDER

router = Router()


class GiveawayCreateState(StatesGroup):
    choosing_date = State()
    entering_date = State()
    main_menu = State()
    entering_amount = State()


def _is_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id == int(ADMIN_BROADCAST_USER_ID))


def _extract_command_arg(message: Message) -> Optional[str]:
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip()


def _parse_date_token(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    value = token.strip().lower()
    today = now_local().date()
    if value in {"today", "сегодня"}:
        return today.isoformat()
    if value in {"tomorrow", "завтра"}:
        return (today + timedelta(days=1)).isoformat()
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _normalize_prizes(value: object) -> Dict[str, Dict[str, object]]:
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, Dict[str, object]] = {}
    for raw_key, raw_prize in value.items():
        try:
            place = int(raw_key)
        except (TypeError, ValueError):
            continue
        if not isinstance(raw_prize, dict):
            continue
        normalized[str(place)] = dict(raw_prize)
    return normalized


def _clone_prizes(value: object) -> Dict[str, Dict[str, object]]:
    return {
        place: dict(prize)
        for place, prize in _normalize_prizes(value).items()
        if isinstance(prize, dict)
    }


def _diff_exclusive_counts(
    new_counts: Dict[str, int],
    old_counts: Dict[str, int],
) -> Dict[str, int]:
    delta: Dict[str, int] = {}
    for file_name in set(new_counts.keys()) | set(old_counts.keys()):
        diff = int(new_counts.get(file_name, 0)) - int(old_counts.get(file_name, 0))
        if diff:
            delta[file_name] = diff
    return delta


def _place_badge(place: int) -> str:
    return {1: "🥇", 2: "🥈", 3: "🥉"}.get(place, "🎁")


def _format_prize_short(prize: Optional[Dict[str, object]], card_map: Dict[str, Card]) -> str:
    if not prize:
        return "не выбрано"
    prize_type = str(prize.get("type", ""))
    if prize_type == "balance":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount}р"
    if prize_type == "free_rolls":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount} фри"
    if prize_type == "vip":
        days = int(prize.get("days", 0) or 0)
        return f"VIP {days}д"
    if prize_type == "card":
        file_name = str(prize.get("file", ""))
        card = card_map.get(file_name) if file_name else None
        rarity = prize.get("rarity") or (card.rarity if card else "")
        rarity_label = RARITY_NAMES.get(str(rarity), rarity) if rarity else ""
        label = card_display_name(card) if card else file_name or "сосиску"
        price_text = ""
        if card and card.price is not None:
            price_text = format_short_amount(card.price, card_currency(card))
        if rarity_label and price_text:
            return f"{label} ({rarity_label}, {price_text})"
        if rarity_label:
            return f"{label} ({rarity_label})"
        return label
    return "приз"


def _format_prize_announce(
    prize: Optional[Dict[str, object]],
    card_map: Dict[str, Card],
) -> str:
    if not prize:
        return "приз"
    prize_type = str(prize.get("type", ""))
    if prize_type == "balance":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount}р на баланс"
    if prize_type == "free_rolls":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount} фри спинов"
    if prize_type == "vip":
        days = int(prize.get("days", 0) or 0)
        return f"VIP на {days} дней"
    if prize_type == "card":
        file_name = str(prize.get("file", ""))
        card = card_map.get(file_name) if file_name else None
        rarity = prize.get("rarity") or (card.rarity if card else "")
        rarity_label = RARITY_NAMES.get(str(rarity), rarity) if rarity else ""
        label = card_display_name(card) if card else file_name or "сосиску"
        price_text = ""
        if card and card.price is not None:
            price_text = format_short_amount(card.price, card_currency(card))
        if rarity_label and price_text:
            return f"{label} ({rarity_label}, {price_text})"
        if rarity_label:
            return f"{label} ({rarity_label})"
        return label
    return "приз"


def _build_announce_caption(
    date_key: str,
    prizes: Dict[str, Dict[str, object]],
    card_map: Dict[str, Card],
) -> str:
    lines = [f"🎉 Розыгрыш {date_key}", "", "Призы:"]
    places = sorted(
        (int(key) for key in prizes.keys() if str(key).isdigit()), key=int
    )
    for place in places:
        prize = prizes.get(str(place))
        label = _format_prize_announce(prize, card_map)
        lines.append(f"{_place_badge(place)} {place} место — {label}")
    lines.extend(["", "Чтобы участвовать: /rozigrish в личке с ботом."])
    return "\n".join(lines)


def _build_main_caption(
    date_key: str,
    prizes: Dict[str, Dict[str, object]],
    card_map: Dict[str, Card],
) -> str:
    lines = [f"Розыгрыш на {date_key}", "Настрой призы для мест 1–10:", ""]
    for place in range(1, 11):
        prize = prizes.get(str(place))
        label = _format_prize_short(prize, card_map)
        lines.append(f"{place} место: {label}")
    return "\n".join(lines)


def _collect_exclusive_prizes(prizes: Dict[str, Dict[str, object]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for prize in prizes.values():
        if str(prize.get("type", "")) != "card":
            continue
        rarity = str(prize.get("rarity", "") or "")
        if rarity != "exclusive":
            continue
        file_name = str(prize.get("file", "")).strip()
        if not file_name:
            continue
        counts[file_name] = counts.get(file_name, 0) + 1
    return counts


async def _send_menu(
    message: Message,
    caption: str,
    reply_markup,
    *,
    prefer_edit: bool,
    owner_id: int,
) -> None:
    menu_path = get_cached_menu_image("giveaway_admin", "Розыгрыш", "Создание")
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            reply_markup,
            prefer_edit=prefer_edit,
            rate_limiter=None,
            owner_id=owner_id,
        )


async def _send_main_menu(
    message: Message,
    state: FSMContext,
    *,
    card_map: Dict[str, Card],
    prefer_edit: bool,
    owner_id: int,
) -> None:
    data = await state.get_data()
    date_key = data.get("date") or now_local().date().isoformat()
    prizes = data.get("prizes") or {}
    caption = _build_main_caption(str(date_key), prizes, card_map)
    selected_places = [int(place) for place in prizes.keys() if str(place).isdigit()]
    await _send_menu(
        message,
        caption,
        build_giveaway_places_keyboard(selected_places),
        prefer_edit=prefer_edit,
        owner_id=owner_id,
    )
    await state.set_state(GiveawayCreateState.main_menu)


@router.message(Command("giveaway_new", "giveaway_create"))
async def giveaway_new_command(
    message: Message,
    state: FSMContext,
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    await state.clear()
    await state.set_state(GiveawayCreateState.choosing_date)
    await state.update_data(prizes={}, date=None)
    await _send_menu(
        message,
        "Создание розыгрыша.\nВыбери дату:",
        build_giveaway_date_keyboard(),
        prefer_edit=False,
        owner_id=message.from_user.id,
    )


@router.message(Command("giveaway_edit"))
async def giveaway_edit_command(
    message: Message,
    state: FSMContext,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    await state.clear()
    raw_arg = _extract_command_arg(message)
    date_key = _parse_date_token(raw_arg)
    if raw_arg and not date_key:
        await message.answer("Дата должна быть в формате YYYY-MM-DD.")
        return
    active = await get_kv(db_pool, "giveaway") or {}
    if active:
        active["prizes"] = _normalize_prizes(active.get("prizes"))
    schedule_items = await get_giveaway_schedule(db_pool)
    target = None
    item: Optional[Dict[str, object]] = None
    if date_key:
        if str(active.get("date", "")) == date_key:
            target = "active"
            item = active
        else:
            item = next(
                (entry for entry in schedule_items if entry.get("date") == date_key),
                None,
            )
            if item:
                target = "schedule"
    else:
        if active and active.get("date") and active.get("status") != "announced":
            target = "active"
            item = active
        else:
            today_key = now_local().date().isoformat()
            upcoming = [
                entry
                for entry in schedule_items
                if str(entry.get("date", "")) >= str(today_key)
            ]
            upcoming.sort(key=lambda entry: str(entry.get("date", "")))
            if upcoming:
                target = "schedule"
                item = upcoming[0]
            elif schedule_items:
                schedule_items.sort(key=lambda entry: str(entry.get("date", "")))
                target = "schedule"
                item = schedule_items[0]
    if not item:
        await message.answer("Розыгрыш для редактирования не найден. Используй /giveaway_list.")
        return
    if target == "active" and item.get("status") == "announced":
        await message.answer("Этот розыгрыш уже объявлен, редактирование недоступно.")
        return
    date_key = str(item.get("date", "")).strip()
    if not date_key:
        await message.answer("Не удалось определить дату розыгрыша.")
        return
    prizes = _clone_prizes(item.get("prizes"))
    await state.update_data(
        date=date_key,
        prizes=prizes,
        edit_target=target,
        original_prizes=_clone_prizes(prizes),
    )
    await _send_main_menu(
        message,
        state,
        card_map=card_map,
        prefer_edit=False,
        owner_id=message.from_user.id,
    )


@router.message(Command("giveaway_delete", "giveaway_cancel"))
async def giveaway_delete_command(
    message: Message,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    raw_arg = _extract_command_arg(message)
    date_key = _parse_date_token(raw_arg)
    if raw_arg and not date_key:
        await message.answer("Дата должна быть в формате YYYY-MM-DD.")
        return
    active = await get_kv(db_pool, "giveaway") or {}
    if active:
        active["prizes"] = _normalize_prizes(active.get("prizes"))
    schedule_items = await get_giveaway_schedule(db_pool)
    target = None
    item: Optional[Dict[str, object]] = None
    if date_key:
        if str(active.get("date", "")) == date_key:
            target = "active"
            item = active
        else:
            item = next(
                (entry for entry in schedule_items if entry.get("date") == date_key),
                None,
            )
            if item:
                target = "schedule"
    else:
        if active and active.get("date"):
            target = "active"
            item = active
        else:
            today_key = now_local().date().isoformat()
            upcoming = [
                entry
                for entry in schedule_items
                if str(entry.get("date", "")) >= str(today_key)
            ]
            upcoming.sort(key=lambda entry: str(entry.get("date", "")))
            if upcoming:
                target = "schedule"
                item = upcoming[0]
            elif schedule_items:
                schedule_items.sort(key=lambda entry: str(entry.get("date", "")))
                target = "schedule"
                item = schedule_items[0]
    if not item or not target:
        await message.answer("Нет розыгрышей для удаления.")
        return
    date_key = str(item.get("date", "")).strip()
    if not date_key:
        await message.answer("Не удалось определить дату розыгрыша.")
        return
    prizes = _normalize_prizes(item.get("prizes"))
    first_prize = _format_prize_short(prizes.get("1"), card_map)
    caption_lines = [
        f"Удалить розыгрыш на {date_key}?",
        f"1 место: {first_prize}",
        "Это действие необратимо.",
    ]
    sent = await message.answer(
        "\n".join(caption_lines),
        reply_markup=build_giveaway_delete_keyboard(date_key, target),
    )
    if message.from_user:
        remember_owner(sent.chat.id, sent.message_id, message.from_user.id)


@router.message(Command("giveaway_say"))
async def giveaway_say_command(
    message: Message,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    raw_arg = _extract_command_arg(message)
    date_key = _parse_date_token(raw_arg)
    if raw_arg and not date_key:
        await message.answer("Дата должна быть в формате YYYY-MM-DD.")
        return
    active = await get_kv(db_pool, "giveaway") or {}
    if active:
        active["prizes"] = _normalize_prizes(active.get("prizes"))
    schedule_items = await get_giveaway_schedule(db_pool)
    item: Optional[Dict[str, object]] = None
    if date_key:
        if str(active.get("date", "")) == date_key:
            item = active
        else:
            item = next(
                (entry for entry in schedule_items if entry.get("date") == date_key),
                None,
            )
    else:
        if active and active.get("date") and active.get("status") != "announced":
            item = active
        else:
            today_key = now_local().date().isoformat()
            upcoming = [
                entry
                for entry in schedule_items
                if str(entry.get("date", "")) >= str(today_key)
            ]
            upcoming.sort(key=lambda entry: str(entry.get("date", "")))
            if upcoming:
                item = upcoming[0]
            elif schedule_items:
                schedule_items.sort(key=lambda entry: str(entry.get("date", "")))
                item = schedule_items[0]
    if not item:
        await message.answer("Розыгрыш не найден.")
        return
    date_key = str(item.get("date", "")).strip()
    if not date_key:
        await message.answer("Не удалось определить дату розыгрыша.")
        return
    prizes = _normalize_prizes(item.get("prizes"))
    if not prizes:
        await message.answer("В этом розыгрыше нет призов.")
        return
    caption = _build_announce_caption(date_key, prizes, card_map)
    menu_path = get_cached_menu_image(f"giveaway_say_{date_key}", "Розыгрыш", date_key)
    users = await fetch_all_users(db_pool)
    chats = await fetch_broadcast_chats(
        db_pool, types=["channel", "supergroup", "group"]
    )
    if not users and not chats:
        await message.answer("Нет получателей для рассылки.")
        return

    sent_users = 0
    failed_users = 0
    for user in users:
        uid = int(user.get("user_id", 0))
        if uid <= 0:
            continue
        if rate_limiter:
            await rate_limiter.acquire(uid)
        try:
            await message.bot.send_photo(
                chat_id=uid,
                photo=FSInputFile(str(menu_path)),
                caption=caption,
            )
            sent_users += 1
        except TelegramRetryAfter as exc:
            if rate_limiter:
                await rate_limiter.register_retry_after(exc.retry_after)
            await asyncio.sleep(max(0.1, float(exc.retry_after)))
            failed_users += 1
        except TelegramForbiddenError:
            failed_users += 1
        except Exception:
            failed_users += 1
        await asyncio.sleep(0.03)

    sent_chats = 0
    failed_chats = 0
    for chat in chats:
        chat_id = int(chat.get("chat_id", 0))
        if chat_id == 0:
            continue
        if rate_limiter:
            await rate_limiter.acquire(chat_id)
        try:
            await message.bot.send_photo(
                chat_id=chat_id,
                photo=FSInputFile(str(menu_path)),
                caption=caption,
            )
            sent_chats += 1
        except TelegramRetryAfter as exc:
            if rate_limiter:
                await rate_limiter.register_retry_after(exc.retry_after)
            await asyncio.sleep(max(0.1, float(exc.retry_after)))
            failed_chats += 1
        except TelegramForbiddenError:
            await delete_broadcast_chat(db_pool, chat_id)
            failed_chats += 1
        except Exception:
            failed_chats += 1
        await asyncio.sleep(0.03)

    await message.answer(
        "Готово.\n"
        f"Лички: {sent_users} ок, {failed_users} ошибок.\n"
        f"Каналы: {sent_chats} ок, {failed_chats} ошибок."
    )


@router.callback_query(F.data == "gw_cancel")
async def giveaway_cancel_callback(query: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    await state.clear()
    if query.message:
        await query.message.answer("Операция отменена.")
    await query.answer()


@router.callback_query(F.data == "gw_delete_cancel")
async def giveaway_delete_cancel_callback(query: CallbackQuery) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    await query.answer("Удаление отменено.")


@router.callback_query(F.data.startswith("gw_delete|"))
async def giveaway_delete_confirm_callback(
    query: CallbackQuery,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    try:
        _, date_key, target = query.data.split("|", 2)
    except ValueError:
        await query.answer("Неверная команда.", show_alert=True)
        return
    if target not in {"schedule", "active"}:
        await query.answer("Неверная команда.", show_alert=True)
        return
    if target == "active":
        active = await get_kv(db_pool, "giveaway") or {}
        if str(active.get("date", "")) != str(date_key):
            await query.answer("Розыгрыш не найден.", show_alert=True)
            return
        prizes = _normalize_prizes(active.get("prizes"))
        counts = _collect_exclusive_prizes(prizes)
        if counts:
            updates = {file_name: -count for file_name, count in counts.items()}
            await update_exclusive_reserved(db_pool, updates)
            await sync_exclusive_stock(db_pool, counts.keys(), EXCLUSIVE_STOCK_LIMIT)
        await set_kv(db_pool, "giveaway", {})
        await query.message.answer(f"Розыгрыш на {date_key} удален.")
        await query.answer()
        return

    schedule_items = await get_giveaway_schedule(db_pool)
    item = next(
        (entry for entry in schedule_items if entry.get("date") == str(date_key)),
        None,
    )
    if not item:
        await query.answer("Розыгрыш не найден.", show_alert=True)
        return
    prizes = _normalize_prizes(item.get("prizes"))
    counts = _collect_exclusive_prizes(prizes)
    if counts:
        updates = {file_name: -count for file_name, count in counts.items()}
        await update_exclusive_reserved(db_pool, updates)
        await sync_exclusive_stock(db_pool, counts.keys(), EXCLUSIVE_STOCK_LIMIT)
    schedule_items = [
        entry for entry in schedule_items if entry.get("date") != str(date_key)
    ]
    await set_giveaway_schedule(db_pool, schedule_items)
    await query.message.answer(f"Розыгрыш на {date_key} удален.")
    await query.answer()


@router.callback_query(F.data.startswith("gw_date|"))
async def giveaway_date_callback(
    query: CallbackQuery,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, mode = query.data.split("|", 1)
    today = now_local().date()
    if mode == "today":
        date_key = today.isoformat()
    elif mode == "tomorrow":
        date_key = (today + timedelta(days=1)).isoformat()
    else:
        await state.set_state(GiveawayCreateState.entering_date)
        await query.message.answer("Введи дату в формате YYYY-MM-DD.")
        await query.answer()
        return
    await state.update_data(date=date_key)
    await _send_main_menu(
        query.message,
        state,
        card_map=card_map,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.message(GiveawayCreateState.entering_date)
async def giveaway_date_input(
    message: Message,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    raw = (message.text or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        await message.answer("Дата должна быть в формате YYYY-MM-DD.")
        return
    if parsed < now_local().date():
        await message.answer("Нельзя создать розыгрыш в прошлом.")
        return
    await state.update_data(date=parsed.isoformat())
    await _send_main_menu(
        message,
        state,
        card_map=card_map,
        prefer_edit=False,
        owner_id=message.from_user.id,
    )


@router.callback_query(F.data == "gw_back_main")
async def giveaway_back_main_callback(
    query: CallbackQuery,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    await _send_main_menu(
        query.message,
        state,
        card_map=card_map,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("gw_place|"))
async def giveaway_place_callback(
    query: CallbackQuery,
    state: FSMContext,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw = query.data.split("|", 1)
    try:
        place = int(place_raw)
    except ValueError:
        return
    await _send_menu(
        query.message,
        f"Место {place}: выбери тип приза.",
        build_giveaway_place_type_keyboard(place),
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("gw_type|"))
async def giveaway_type_callback(
    query: CallbackQuery,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw, prize_type = query.data.split("|", 2)
    try:
        place = int(place_raw)
    except ValueError:
        return
    if prize_type == "balance":
        await state.update_data(pending_place=place, pending_type="balance")
        await state.set_state(GiveawayCreateState.entering_amount)
        await query.message.answer("Введи сумму для баланса (целое число).")
        await query.answer()
        return
    if prize_type == "free":
        await state.update_data(pending_place=place, pending_type="free_rolls")
        await state.set_state(GiveawayCreateState.entering_amount)
        await query.message.answer("Введи количество фри спинов (целое число).")
        await query.answer()
        return
    if prize_type == "vip":
        await _send_menu(
            query.message,
            f"Место {place}: выбери длительность VIP.",
            build_giveaway_vip_duration_keyboard(place),
            prefer_edit=True,
            owner_id=query.from_user.id,
        )
        await query.answer()
        return
    if prize_type == "card":
        await _send_menu(
            query.message,
            f"Место {place}: выбери редкость.",
            build_rarity_keyboard(
                f"gw_rarity|{place}",
                include_menu=True,
                rarities=list(RARITY_ORDER),
                back_callback=f"gw_place|{place}",
            ),
            prefer_edit=True,
            owner_id=query.from_user.id,
        )
        await query.answer()
        return


@router.callback_query(F.data.startswith("gw_vip|"))
async def giveaway_vip_callback(
    query: CallbackQuery,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw, days_raw = query.data.split("|", 2)
    try:
        place = int(place_raw)
        days = int(days_raw)
    except ValueError:
        return
    data = await state.get_data()
    prizes = dict(data.get("prizes") or {})
    prizes[str(place)] = {"type": "vip", "days": days}
    await state.update_data(prizes=prizes)
    await _send_main_menu(
        query.message,
        state,
        card_map=card_map,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.message(GiveawayCreateState.entering_amount)
async def giveaway_amount_input(
    message: Message,
    state: FSMContext,
    card_map: Dict[str, Card],
) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    data = await state.get_data()
    place = data.get("pending_place")
    prize_type = data.get("pending_type")
    if not place or not prize_type:
        await message.answer("Сначала выбери место и тип приза.")
        return
    raw = (message.text or "").strip()
    try:
        amount = int(raw)
    except ValueError:
        await message.answer("Нужно целое число.")
        return
    if amount <= 0:
        await message.answer("Количество должно быть больше нуля.")
        return
    prizes = dict(data.get("prizes") or {})
    prizes[str(place)] = {"type": prize_type, "amount": amount}
    await state.update_data(prizes=prizes, pending_place=None, pending_type=None)
    await _send_main_menu(
        message,
        state,
        card_map=card_map,
        prefer_edit=False,
        owner_id=message.from_user.id,
    )


@router.callback_query(F.data.startswith("gw_rarity_menu|"))
async def giveaway_rarity_menu_callback(
    query: CallbackQuery,
    state: FSMContext,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw = query.data.split("|", 1)
    try:
        place = int(place_raw)
    except ValueError:
        return
    await _send_menu(
        query.message,
        f"Место {place}: выбери редкость.",
        build_rarity_keyboard(
            f"gw_rarity|{place}",
            include_menu=True,
            rarities=list(RARITY_ORDER),
            back_callback=f"gw_place|{place}",
        ),
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


def _get_cards_for_rarity(
    cards_by_rarity: Dict[str, List[Card]],
    rarity: str,
) -> List[Card]:
    available = filter_existing_cards(cards_by_rarity)
    return available.get(rarity, []) or []


async def _show_card_preview(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    place: int,
    rarity: str,
    index: int,
) -> None:
    cards = _get_cards_for_rarity(cards_by_rarity, rarity)
    if not cards:
        await query.message.answer("Нет карточек этой редкости.")
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    path = get_card_media_path(card)
    if not path.exists():
        await query.message.answer("Файл карточки не найден.")
        return
    rarity_label = RARITY_NAMES.get(card.rarity, card.rarity)
    price_label = (
        format_short_amount(card.price, card_currency(card))
        if card.price is not None
        else "не задана"
    )
    caption_lines = [
        f"Место {place}",
        f"{card_display_name(card)}",
        f"Редкость: {rarity_label}",
        f"Цена: {price_label}",
    ]
    if card.rarity == "exclusive":
        await sync_exclusive_stock(db_pool, [card.file], EXCLUSIVE_STOCK_LIMIT)
        stock = await get_exclusive_stock(db_pool, card.file)
        if stock:
            remaining, total = stock
            caption_lines.append(f"Эксклюзив: осталось {remaining}/{total}")
    with path.open("rb") as media:
        await send_or_edit_media(
            query.message,
            media,
            "\n".join(caption_lines),
            build_giveaway_card_nav_keyboard(place, rarity, index, len(cards)),
            prefer_edit=True,
            rate_limiter=None,
            owner_id=query.from_user.id,
        )


@router.callback_query(F.data.startswith("gw_rarity|"))
async def giveaway_rarity_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw, rarity = query.data.split("|", 2)
    try:
        place = int(place_raw)
    except ValueError:
        return
    await _show_card_preview(query, db_pool, cards_by_rarity, place, rarity, 0)
    await query.answer()


@router.callback_query(F.data.startswith("gw_card_nav|"))
async def giveaway_card_nav_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw, rarity, index_raw = query.data.split("|", 3)
    try:
        place = int(place_raw)
        index = int(index_raw)
    except ValueError:
        return
    await _show_card_preview(query, db_pool, cards_by_rarity, place, rarity, index)
    await query.answer()


@router.callback_query(F.data.startswith("gw_card_pick|"))
async def giveaway_card_pick_callback(
    query: CallbackQuery,
    state: FSMContext,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, place_raw, rarity, index_raw = query.data.split("|", 3)
    try:
        place = int(place_raw)
        index = int(index_raw)
    except ValueError:
        return
    cards = _get_cards_for_rarity(cards_by_rarity, rarity)
    if not cards:
        await query.answer("Нет карточек этой редкости.", show_alert=True)
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    if card.rarity == "exclusive":
        await sync_exclusive_stock(db_pool, [card.file], EXCLUSIVE_STOCK_LIMIT)
        stock = await get_exclusive_stock(db_pool, card.file)
        remaining = stock[0] if stock else 0
        if remaining <= 0:
            await query.answer("Эксклюзив закончился.", show_alert=True)
            return
    data = await state.get_data()
    prizes = dict(data.get("prizes") or {})
    prizes[str(place)] = {"type": "card", "file": card.file, "rarity": card.rarity}
    await state.update_data(prizes=prizes)
    await _send_main_menu(
        query.message,
        state,
        card_map=card_map,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "gw_done")
async def giveaway_done_callback(
    query: CallbackQuery,
    state: FSMContext,
    card_map: Dict[str, Card],
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    data = await state.get_data()
    date_key = data.get("date")
    prizes = _clone_prizes(data.get("prizes"))
    edit_target = data.get("edit_target")
    original_prizes = _clone_prizes(data.get("original_prizes"))
    if not date_key:
        await query.answer("Сначала выбери дату.", show_alert=True)
        return
    try:
        parsed_date = date.fromisoformat(str(date_key))
    except ValueError:
        await query.answer("Неверный формат даты.", show_alert=True)
        return
    if parsed_date < now_local().date():
        await query.answer("Нельзя создать розыгрыш в прошлом.", show_alert=True)
        return
    missing = [str(place) for place in range(1, 11) if str(place) not in prizes]
    if missing:
        await query.answer(
            f"Не выбраны призы: {', '.join(missing)}",
            show_alert=True,
        )
        return
    schedule_items = await get_giveaway_schedule(db_pool)
    active = await get_kv(db_pool, "giveaway") or {}
    active_date = str(active.get("date", "")) if active else ""
    if edit_target:
        if edit_target == "schedule":
            if active_date == str(date_key):
                await query.answer(
                    "Этот розыгрыш уже активен. Редактируй активный.",
                    show_alert=True,
                )
                return
            if not any(item.get("date") == str(date_key) for item in schedule_items):
                await query.answer("Запланированный розыгрыш не найден.", show_alert=True)
                return
        elif edit_target == "active":
            if active_date != str(date_key):
                await query.answer("Активный розыгрыш не найден.", show_alert=True)
                return
            if active.get("status") == "announced":
                await query.answer(
                    "Розыгрыш уже объявлен, редактирование недоступно.",
                    show_alert=True,
                )
                return
        else:
            await query.answer("Неверный режим редактирования.", show_alert=True)
            return
    else:
        if any(item.get("date") == str(date_key) for item in schedule_items):
            await query.answer("На эту дату уже есть розыгрыш.", show_alert=True)
            return
        if active_date == str(date_key):
            await query.answer("На эту дату уже идет розыгрыш.", show_alert=True)
            return

    if edit_target:
        old_counts = _collect_exclusive_prizes(original_prizes)
        new_counts = _collect_exclusive_prizes(prizes)
        delta = _diff_exclusive_counts(new_counts, old_counts)
        if delta:
            await sync_exclusive_stock(db_pool, delta.keys(), EXCLUSIVE_STOCK_LIMIT)
            lacking = []
            for file_name, diff in delta.items():
                if diff <= 0:
                    continue
                stock = await get_exclusive_stock(db_pool, file_name)
                remaining = stock[0] if stock else 0
                if remaining < diff:
                    lacking.append(
                        f"{file_name} (нужно {diff}, доступно {remaining})"
                    )
            if lacking:
                await query.answer("Эксклюзивов не хватает.", show_alert=True)
                await query.message.answer("\n".join(lacking))
                return
            await update_exclusive_reserved(db_pool, delta)
            await sync_exclusive_stock(db_pool, delta.keys(), EXCLUSIVE_STOCK_LIMIT)
    else:
        exclusive_counts = _collect_exclusive_prizes(prizes)
        if exclusive_counts:
            await sync_exclusive_stock(
                db_pool, exclusive_counts.keys(), EXCLUSIVE_STOCK_LIMIT
            )
            lacking = []
            for file_name, count in exclusive_counts.items():
                stock = await get_exclusive_stock(db_pool, file_name)
                remaining = stock[0] if stock else 0
                if remaining < count:
                    lacking.append(f"{file_name} (нужно {count}, доступно {remaining})")
            if lacking:
                await query.answer("Эксклюзивов не хватает.", show_alert=True)
                await query.message.answer("\n".join(lacking))
                return
            await update_exclusive_reserved(db_pool, exclusive_counts)
            await sync_exclusive_stock(
                db_pool, exclusive_counts.keys(), EXCLUSIVE_STOCK_LIMIT
            )

    if edit_target == "schedule":
        updated = False
        for item in schedule_items:
            if item.get("date") == str(date_key):
                item["prizes"] = _normalize_prizes(prizes)
                updated = True
                break
        if not updated:
            await query.answer("Запланированный розыгрыш не найден.", show_alert=True)
            return
        await set_giveaway_schedule(db_pool, schedule_items)
        result_text = f"Розыгрыш на {date_key} обновлен."
    elif edit_target == "active":
        active["prizes"] = _normalize_prizes(prizes)
        await set_kv(db_pool, "giveaway", active)
        result_text = f"Розыгрыш на {date_key} обновлен."
    else:
        await schedule_giveaway(
            db_pool,
            str(date_key),
            prizes,
            query.from_user.id if query.from_user else None,
        )
        result_text = f"Розыгрыш на {date_key} создан."
    await state.clear()
    await _send_menu(
        query.message,
        result_text,
        None,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Dict, List, Optional

from aiogram import F, Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.enums import ParseMode

from app.images import (
    build_showcase_board_image,
    build_showcase_card_image,
    get_card_media_path,
    get_cached_menu_image,
)
from app.keyboards import (
    build_back_keyboard,
    build_kazik_open_dm_keyboard,
    build_rarity_keyboard,
    build_showcase_cards_keyboard,
    build_showcase_craft_keyboard,
    build_showcase_market_keyboard,
    build_showcase_menu_keyboard,
    build_showcase_slot_keyboard,
)
from app.messages import send_or_edit_media
from app.repo import (
    adjust_user_balance,
    adjust_user_free_rolls,
    adjust_user_stars,
    buy_showcase_listing,
    cancel_showcase_listing,
    clear_showcase_card_slot,
    clear_showcase_slot,
    consume_inventory_items,
    create_showcase_card,
    create_showcase_listing,
    get_user,
    list_inventory,
    list_showcase_active_cards,
    list_showcase_cards,
    list_showcase_market,
    set_showcase_card_slot,
    update_user_fields,
)
from app.showcase import (
    SHOWCASE_CRAFT_RARITIES,
    format_showcase_card_caption,
    format_showcase_effect,
    roll_showcase_effect,
    summarize_showcase_effects,
)
from app.utils import format_card_label, format_short_amount, now_local
from cards import Card, card_currency
from config import (
    RARITY_NAMES,
    SHOWCASE_CRAFT_COUNT,
    SHOWCASE_CRAFT_COST_BALANCE,
    SHOWCASE_MAX_ACTIVE,
    VIP_INFINITE_DAYS,
)

router = Router()
showcase_logger = logging.getLogger("cards")

_SHOWCASE_INPUT_MODE = "showcase_price"


def _normalize_showcase_session(raw: object) -> Dict[str, object]:
    if not isinstance(raw, dict):
        return {"mode": None, "rarity": None, "selected": [], "card_id": None, "index": 0}
    selected_raw = raw.get("selected", [])
    selected = []
    if isinstance(selected_raw, list):
        for item in selected_raw:
            if item:
                selected.append(str(item))
    return {
        "mode": raw.get("mode"),
        "rarity": raw.get("rarity"),
        "selected": selected,
        "card_id": raw.get("card_id"),
        "index": int(raw.get("index") or 0),
    }


def _build_craft_caption(
    card: Card, rarity: str, index: int, total: int, selected_count: int
) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    rarity_label = RARITY_NAMES.get(rarity, rarity)
    return "\n".join(
        [
            "Создание карты витрины",
            f"Редкость: {rarity_label}",
            f"Выбрано: {selected_count}/{SHOWCASE_CRAFT_COUNT}",
            f"Стоимость: {format_short_amount(SHOWCASE_CRAFT_COST_BALANCE, 'rub')}",
            f"{format_card_label(card)} - {price_text}",
            f"{index + 1}/{total}",
        ]
    )


async def _send_showcase_menu(
    message: Message, *, prefer_edit: bool, rate_limiter, owner_id: int
) -> None:
    menu_path = get_cached_menu_image("showcase", "Витрина", "Выбери действие")
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "Витрина. Выбери действие:",
            build_showcase_menu_keyboard(),
            prefer_edit=prefer_edit,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def _show_showcase_card(
    message: Message,
    cards: List[Dict[str, object]],
    index: int,
    *,
    listing_map: Dict[str, str],
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    if not cards:
        await message.answer("У тебя пока нет карт витрины.")
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    title = str(card.get("title") or "Карта")
    rarity = str(card.get("rarity") or "")
    effect_text = format_showcase_effect(
        str(card.get("effect_type") or ""),
        float(card.get("effect_value") or 0),
    )
    image = build_showcase_card_image(title, effect_text, rarity)
    caption = format_showcase_card_caption(card)
    listing_id = listing_map.get(str(card.get("card_id")))
    await send_or_edit_media(
        message,
        image,
        caption,
        build_showcase_cards_keyboard(
            index,
            len(cards),
            str(card.get("card_id")),
            slot=card.get("slot"),
            listing_id=listing_id,
        ),
        prefer_edit=prefer_edit,
        rate_limiter=rate_limiter,
        owner_id=owner_id,
    )


async def _show_market_listing(
    message: Message,
    listings: List[Dict[str, object]],
    index: int,
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    if not listings:
        await message.answer("Маркет пуст.")
        return
    index = max(0, min(index, len(listings) - 1))
    listing = listings[index]
    title = str(listing.get("title") or "Карта")
    rarity = str(listing.get("rarity") or "")
    effect_text = format_showcase_effect(
        str(listing.get("effect_type") or ""),
        float(listing.get("effect_value") or 0),
    )
    image = build_showcase_card_image(title, effect_text, rarity)
    price = int(listing.get("price") or 0)
    seller_id = listing.get("seller_id")
    caption = "\n".join(
        [
            title,
            f"Редкость: {RARITY_NAMES.get(rarity, rarity)}",
            f"Эффект: {effect_text}",
            f"Цена: {format_short_amount(price, 'rub')}",
            f"Продавец: {seller_id}",
        ]
    )
    is_owner = int(seller_id or 0) == int(owner_id)
    await send_or_edit_media(
        message,
        image,
        caption,
        build_showcase_market_keyboard(
            index,
            len(listings),
            str(listing.get("listing_id")),
            is_owner=is_owner,
        ),
        prefer_edit=prefer_edit,
        rate_limiter=rate_limiter,
        owner_id=owner_id,
    )


async def _show_craft_card(
    message: Message,
    items: List[Dict[str, object]],
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    selected: List[str],
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    entries = [item for item in items if card_map.get(item.get("file", "")) and card_map[item["file"]].rarity == rarity]
    if not entries:
        await message.answer("У тебя нет сосисок этой редкости.")
        return
    index = max(0, min(index, len(entries) - 1))
    item = entries[index]
    card = card_map.get(item.get("file", ""))
    if not card:
        await message.answer("Карточка не найдена.")
        return
    path = get_card_media_path(card)
    if not path.exists():
        await message.answer("Фото не найдено для этой карточки.")
        return
    selected_ids = list(dict.fromkeys(selected))
    selected_set = set(selected_ids)
    caption = _build_craft_caption(card, rarity, index, len(entries), len(selected_ids))
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_showcase_craft_keyboard(
                rarity,
                index,
                len(entries),
                item["id"],
                selected_count=len(selected_ids),
                selected=item.get("id") in selected_set,
            ),
            prefer_edit=prefer_edit,
            parse_mode=ParseMode.HTML,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


@router.message(Command("showcase"))
async def showcase_command(message: Message, rate_limiter) -> None:
    if not message.from_user:
        return
    await _send_showcase_menu(
        message, prefer_edit=False, rate_limiter=rate_limiter, owner_id=message.from_user.id
    )


@router.callback_query(F.data == "cmd|showcase")
async def showcase_menu_callback(query: CallbackQuery, rate_limiter) -> None:
    if not query.message:
        return
    await _send_showcase_menu(
        query.message,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "showcase_menu")
async def showcase_menu_shortcut(query: CallbackQuery, rate_limiter) -> None:
    if not query.message:
        return
    await _send_showcase_menu(
        query.message,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "showcase_view")
async def showcase_view_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    active_cards = await list_showcase_active_cards(db_pool, query.from_user.id)
    slots: List[Optional[tuple]] = [None] * SHOWCASE_MAX_ACTIVE
    for card in active_cards:
        slot = int(card.get("slot") or 0)
        if 1 <= slot <= SHOWCASE_MAX_ACTIVE:
            effect_text = format_showcase_effect(
                str(card.get("effect_type") or ""),
                float(card.get("effect_value") or 0),
            )
            slots[slot - 1] = (str(card.get("title") or "Карта"), effect_text, str(card.get("rarity") or ""))
    board = build_showcase_board_image(slots)
    effects = summarize_showcase_effects(active_cards)
    lines = ["Витрина активна."]
    if not active_cards:
        lines = ["Витрина пуста. Поставь карты в слоты."]
    else:
        balance_daily = int(effects.get("balance_daily") or 0)
        free_rolls = int(effects.get("free_rolls_daily") or 0)
        kazik_spins = int(effects.get("kazik_spins_daily") or 0)
        stars_daily = int(effects.get("stars_daily") or 0)
        drop_mult = float(effects.get("drop_multiplier") or 1.0)
        sell_mult = float(effects.get("sell_multiplier") or 1.0)
        extra_card = float(effects.get("extra_card_chance") or 0.0)
        vip_infinite = int(effects.get("vip_infinite") or 0)
        if balance_daily:
            lines.append(f"Баланс в день: {balance_daily:+d}р")
        if free_rolls:
            lines.append(f"Фри крутки в день: {free_rolls:+d}")
        if kazik_spins:
            lines.append(f"Казик-спины в день: {kazik_spins:+d}")
        if stars_daily:
            lines.append(f"Звезды в день: {stars_daily:+d}⭐")
        if drop_mult != 1.0:
            lines.append(f"Шанс редких: x{drop_mult:.2f}")
        if sell_mult != 1.0:
            lines.append(f"Продажа: x{sell_mult:.2f}")
        if extra_card != 0:
            sign = "+" if extra_card > 0 else "-"
            lines.append(f"Доп. сосиска: {sign}{abs(extra_card) * 100:.0f}%")
        if vip_infinite:
            lines.append("VIP: навсегда")
    if active_cards and any(
        int(effects.get(key) or 0) != 0
        for key in (
            "balance_daily",
            "free_rolls_daily",
            "kazik_spins_daily",
            "stars_daily",
            "vip_infinite",
        )
    ):
        lines.append("Бонусы выдаются ежедневно в 00:00 (МСК).")
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Назад", callback_data="showcase_menu")]
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
    await send_or_edit_media(
        query.message,
        board,
        "\n".join(lines),
        keyboard,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "showcase_claim")
async def showcase_claim_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    today = now_local().date()
    if user.get("showcase_daily_date") == today:
        await query.answer("Бонус уже получен.", show_alert=False)
        return
    active_cards = await list_showcase_active_cards(db_pool, query.from_user.id)
    effects = summarize_showcase_effects(active_cards)
    balance_daily = int(effects.get("balance_daily") or 0)
    free_rolls = int(effects.get("free_rolls_daily") or 0)
    kazik_spins = int(effects.get("kazik_spins_daily") or 0)
    stars_daily = int(effects.get("stars_daily") or 0)
    vip_infinite = int(effects.get("vip_infinite") or 0)
    if not any([balance_daily, free_rolls, kazik_spins, stars_daily, vip_infinite]):
        await query.answer("Нет доступных бонусов.", show_alert=False)
        return
    lines = []
    if balance_daily:
        result = await adjust_user_balance(db_pool, query.from_user.id, balance_daily)
        if result is not None:
            lines.append(f"Баланс: {balance_daily:+d}р")
    if free_rolls:
        await adjust_user_free_rolls(db_pool, query.from_user.id, free_rolls)
        lines.append(f"Фри крутки: {free_rolls:+d}")
    updates: Dict[str, object] = {"showcase_daily_date": today}
    if kazik_spins:
        bonus = int(user.get("kazik_bonus_spins", 0) or 0) + kazik_spins
        updates["kazik_bonus_spins"] = max(0, bonus)
        lines.append(f"Казик-спины: {kazik_spins:+d}")
    if stars_daily:
        await adjust_user_stars(db_pool, query.from_user.id, stars_daily)
        lines.append(f"Звезды: {stars_daily:+d}⭐")
    if vip_infinite:
        updates["vip_until"] = now_local() + timedelta(days=VIP_INFINITE_DAYS)
        updates["vip"] = True
        lines.append("VIP: навсегда")
    await update_user_fields(db_pool, query.from_user.id, updates)
    await query.message.answer("\n".join(lines) if lines else "Бонусы получены.")
    await query.answer()


@router.callback_query(F.data == "showcase_cards")
async def showcase_cards_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    market = await list_showcase_market(db_pool)
    listing_map = {
        str(item.get("card_id")): str(item.get("listing_id"))
        for item in market
        if int(item.get("seller_id") or 0) == int(query.from_user.id)
    }
    await _show_showcase_card(
        query.message,
        cards,
        0,
        listing_map=listing_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_cards_nav|"))
async def showcase_cards_nav_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, index_raw = query.data.split("|", 1)
    try:
        index = int(index_raw)
    except ValueError:
        return
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    market = await list_showcase_market(db_pool)
    listing_map = {
        str(item.get("card_id")): str(item.get("listing_id"))
        for item in market
        if int(item.get("seller_id") or 0) == int(query.from_user.id)
    }
    await _show_showcase_card(
        query.message,
        cards,
        index,
        listing_map=listing_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_slot_menu|"))
async def showcase_slot_menu_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, card_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    active_cards = await list_showcase_active_cards(db_pool, query.from_user.id)
    occupied_slots = [int(card.get("slot") or 0) for card in active_cards if card.get("slot")]
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    card = next((item for item in cards if str(item.get("card_id")) == card_id), None)
    if not card:
        await query.message.answer("Карта не найдена.")
        return
    title = str(card.get("title") or "Карта")
    rarity = str(card.get("rarity") or "")
    effect_text = format_showcase_effect(
        str(card.get("effect_type") or ""),
        float(card.get("effect_value") or 0),
    )
    image = build_showcase_card_image(title, effect_text, rarity)
    await send_or_edit_media(
        query.message,
        image,
        "Выбери слот для карты:",
        build_showcase_slot_keyboard(card_id, occupied_slots, index=index),
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_slot_set|"))
async def showcase_slot_set_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, card_id, slot_raw, index_raw = query.data.split("|", 3)
    try:
        slot = int(slot_raw)
        index = int(index_raw)
    except ValueError:
        return
    await clear_showcase_slot(db_pool, query.from_user.id, slot)
    success = await set_showcase_card_slot(db_pool, query.from_user.id, card_id, slot)
    if not success:
        await query.message.answer("Не удалось поставить карту.")
        return
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    market = await list_showcase_market(db_pool)
    listing_map = {
        str(item.get("card_id")): str(item.get("listing_id"))
        for item in market
        if int(item.get("seller_id") or 0) == int(query.from_user.id)
    }
    await _show_showcase_card(
        query.message,
        cards,
        index,
        listing_map=listing_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer("Карта поставлена.", show_alert=False)


@router.callback_query(F.data.startswith("showcase_slot_clear|"))
async def showcase_slot_clear_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, card_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    await clear_showcase_card_slot(db_pool, query.from_user.id, card_id)
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    market = await list_showcase_market(db_pool)
    listing_map = {
        str(item.get("card_id")): str(item.get("listing_id"))
        for item in market
        if int(item.get("seller_id") or 0) == int(query.from_user.id)
    }
    await _show_showcase_card(
        query.message,
        cards,
        index,
        listing_map=listing_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer("Карта снята.", show_alert=False)


@router.callback_query(F.data.startswith("showcase_list|"))
async def showcase_list_callback(query: CallbackQuery, db_pool) -> None:
    if not query.message:
        return
    _, card_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    card = next((item for item in cards if str(item.get("card_id")) == card_id), None)
    if not card:
        await query.message.answer("Карта не найдена.")
        return
    if card.get("slot") is not None:
        await query.message.answer("Сначала сними карту с витрины.")
        return
    await update_user_fields(
        db_pool,
        query.from_user.id,
        {
            "input_mode": _SHOWCASE_INPUT_MODE,
            "showcase_session": {"mode": "list_price", "card_id": card_id, "index": index},
        },
    )
    prompt = "Введи цену в рублях или напиши 'отмена'."
    if query.message.chat.type == "private":
        await query.message.answer(prompt)
    else:
        try:
            await query.bot.send_message(query.from_user.id, prompt)
            await query.message.answer(
                "Продолжи в лс бота.",
                reply_markup=build_kazik_open_dm_keyboard(),
            )
        except TelegramForbiddenError:
            await query.message.answer(
                "Открой лс бота и начни переписку.",
                reply_markup=build_kazik_open_dm_keyboard(),
            )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_unlist_card|"))
async def showcase_unlist_card_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, listing_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        index = 0
    removed = await cancel_showcase_listing(db_pool, query.from_user.id, listing_id)
    if not removed:
        await query.answer("Не удалось снять.", show_alert=False)
        return
    cards = await list_showcase_cards(db_pool, query.from_user.id)
    market = await list_showcase_market(db_pool)
    listing_map = {
        str(item.get("card_id")): str(item.get("listing_id"))
        for item in market
        if int(item.get("seller_id") or 0) == int(query.from_user.id)
    }
    await _show_showcase_card(
        query.message,
        cards,
        min(index, max(0, len(cards) - 1)),
        listing_map=listing_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer("Снято с маркета.", show_alert=False)


@router.callback_query(F.data.startswith("showcase_unlist_market|"))
async def showcase_unlist_market_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, listing_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        index = 0
    removed = await cancel_showcase_listing(db_pool, query.from_user.id, listing_id)
    if not removed:
        await query.answer("Не удалось снять.", show_alert=False)
        return
    listings = await list_showcase_market(db_pool)
    await _show_market_listing(
        query.message,
        listings,
        min(index, max(0, len(listings) - 1)),
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer("Снято с маркета.", show_alert=False)


@router.callback_query(F.data == "showcase_market")
async def showcase_market_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    listings = await list_showcase_market(db_pool)
    await _show_market_listing(
        query.message,
        listings,
        0,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_market_nav|"))
async def showcase_market_nav_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, index_raw = query.data.split("|", 1)
    try:
        index = int(index_raw)
    except ValueError:
        return
    listings = await list_showcase_market(db_pool)
    await _show_market_listing(
        query.message,
        listings,
        index,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_buy|"))
async def showcase_buy_callback(
    query: CallbackQuery, db_pool, rate_limiter
) -> None:
    if not query.message:
        return
    _, listing_id, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        index = 0
    _, error = await buy_showcase_listing(db_pool, query.from_user.id, listing_id)
    if error == "funds":
        await query.answer("Недостаточно баланса.", show_alert=False)
        return
    if error == "self":
        await query.answer("Это твоя карта.", show_alert=False)
        return
    if error:
        await query.answer("Не удалось купить.", show_alert=False)
        return
    await query.answer("Карта куплена.", show_alert=False)
    listings = await list_showcase_market(db_pool)
    await _show_market_listing(
        query.message,
        listings,
        min(index, max(0, len(listings) - 1)),
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )


@router.callback_query(F.data == "showcase_craft_menu")
async def showcase_craft_menu_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    counts = {rarity: 0 for rarity in SHOWCASE_CRAFT_RARITIES}
    for item in items:
        card = card_map.get(item.get("file", ""))
        if card and card.rarity in counts:
            counts[card.rarity] += 1
    menu_path = get_cached_menu_image("showcase_craft", "Витрина", "Выбери редкость")
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            query.message,
            photo,
            "Выбери редкость для создания карты:",
            build_rarity_keyboard(
                "showcase_craft_rarity",
                include_menu=True,
                rarities=SHOWCASE_CRAFT_RARITIES,
                counts=counts,
                back_callback="showcase_menu",
            ),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=query.from_user.id,
        )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_craft_rarity|"))
async def showcase_craft_rarity_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    rarity = query.data.split("|", 1)[1]
    session = {"mode": "craft", "rarity": rarity, "selected": []}
    await update_user_fields(db_pool, query.from_user.id, {"showcase_session": session})
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_craft_card(
        query.message,
        items,
        card_map,
        rarity,
        0,
        session["selected"],
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_craft_nav|"))
async def showcase_craft_nav_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_showcase_session(user.get("showcase_session") if user else {})
    selected = session["selected"] if session.get("rarity") == rarity else []
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_craft_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        selected,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_craft_pick|"))
async def showcase_craft_pick_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    try:
        index = int(index_raw)
    except ValueError:
        return
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_showcase_session(user.get("showcase_session") if user else {})
    if session.get("rarity") != rarity:
        session = {"mode": "craft", "rarity": rarity, "selected": []}
    selected = list(dict.fromkeys(session.get("selected", [])))
    if item_id in selected:
        selected.remove(item_id)
    else:
        if len(selected) >= SHOWCASE_CRAFT_COUNT:
            await query.answer("Нужно выбрать 5 сосисок.", show_alert=False)
            return
        selected.append(item_id)
    session["selected"] = selected
    await update_user_fields(db_pool, query.from_user.id, {"showcase_session": session})
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_craft_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        selected,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_craft_clear|"))
async def showcase_craft_clear_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    session = {"mode": "craft", "rarity": rarity, "selected": []}
    await update_user_fields(db_pool, query.from_user.id, {"showcase_session": session})
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_craft_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        session["selected"],
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("showcase_craft_confirm|"))
async def showcase_craft_confirm_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card], rate_limiter
) -> None:
    if not query.message:
        return
    _, rarity = query.data.split("|", 1)
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_showcase_session(user.get("showcase_session") if user else {})
    if session.get("rarity") != rarity:
        await query.answer("Создание не начато.", show_alert=False)
        return
    selected = list(dict.fromkeys(session.get("selected", [])))
    if len(selected) != SHOWCASE_CRAFT_COUNT:
        await query.answer("Нужно выбрать 5 сосисок.", show_alert=False)
        return
    if rarity not in SHOWCASE_CRAFT_RARITIES:
        await query.message.answer("Эту редкость нельзя использовать для витрины.")
        return
    items = await list_inventory(db_pool, query.from_user.id)
    selected_items = [item for item in items if item.get("id") in selected]
    if len(selected_items) != SHOWCASE_CRAFT_COUNT:
        session["selected"] = [item.get("id") for item in selected_items]
        await update_user_fields(db_pool, query.from_user.id, {"showcase_session": session})
        await query.message.answer("Часть сосисок пропала. Выбери заново.")
        return
    for item in selected_items:
        card = card_map.get(item.get("file", ""))
        if card and card.rarity == "exclusive":
            await query.message.answer("Эксклюзивы нельзя использовать для витрины.")
            return
        if not card or card.rarity != rarity:
            await query.message.answer("Нужны 5 сосисок одной редкости.")
            return
    balance = int(user.get("balance", 0) or 0)
    if balance < SHOWCASE_CRAFT_COST_BALANCE:
        await query.message.answer("Недостаточно средств для создания карты.")
        return
    charged = await adjust_user_balance(
        db_pool, query.from_user.id, -SHOWCASE_CRAFT_COST_BALANCE
    )
    if charged is None:
        await query.message.answer("Недостаточно средств для создания карты.")
        return
    try:
        await consume_inventory_items(db_pool, query.from_user.id, selected)
    except Exception:
        await query.message.answer("Не удалось создать карту. Попробуй позже.")
        await adjust_user_balance(
            db_pool, query.from_user.id, SHOWCASE_CRAFT_COST_BALANCE
        )
        return
    effect_type, effect_value, payload, title = roll_showcase_effect(rarity)
    card_id = await create_showcase_card(
        db_pool,
        query.from_user.id,
        rarity,
        effect_type,
        effect_value,
        payload,
        title=title,
    )
    await update_user_fields(db_pool, query.from_user.id, {"showcase_session": None})
    if not card_id:
        await query.message.answer("Не удалось сохранить карту.")
        await adjust_user_balance(
            db_pool, query.from_user.id, SHOWCASE_CRAFT_COST_BALANCE
        )
        return
    showcase_logger.info(
        "Showcase card created. user_id=%s card_id=%s rarity=%s effect=%s",
        query.from_user.id,
        card_id,
        rarity,
        effect_type,
    )
    effect_text = format_showcase_effect(effect_type, effect_value)
    image = build_showcase_card_image(title, effect_text, rarity)
    caption = "\n".join(
        [
            "Карта создана!",
            f"{title}",
            f"Редкость: {RARITY_NAMES.get(rarity, rarity)}",
            f"Эффект: {effect_text}",
        ]
    )
    await send_or_edit_media(
        query.message,
        image,
        caption,
        build_back_keyboard("showcase_menu"),
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.message(F.text & ~F.text.startswith("/"))
async def showcase_price_input(message: Message, db_pool) -> None:
    if not message.from_user:
        return
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        return
    if str(user.get("input_mode") or "") != _SHOWCASE_INPUT_MODE:
        return
    session = _normalize_showcase_session(user.get("showcase_session") if user else {})
    if session.get("mode") != "list_price":
        return
    text = (message.text or "").strip()
    if text.lower() in {"отмена", "cancel"}:
        await update_user_fields(
            db_pool, message.from_user.id, {"input_mode": None, "showcase_session": None}
        )
        await message.answer("Отменено.")
        return
    try:
        price = int(text)
    except ValueError:
        await message.answer("Введи цену числом.")
        return
    if price <= 0:
        await message.answer("Цена должна быть больше нуля.")
        return
    listing_id = await create_showcase_listing(
        db_pool, message.from_user.id, str(session.get("card_id")), price
    )
    await update_user_fields(
        db_pool, message.from_user.id, {"input_mode": None, "showcase_session": None}
    )
    if not listing_id:
        await message.answer("Не удалось выставить карту. Проверь слот и попробуй снова.")
        return
    await message.answer("Карта выставлена на маркет.")

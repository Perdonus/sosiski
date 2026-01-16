from __future__ import annotations

import logging
import random
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app.discounts import (
    consume_discount,
    ensure_discounts,
    get_discount_item,
    is_discount_active,
    restore_discount,
)
from app.images import get_card_media_path, get_cached_menu_image
from app.keyboards import (
    build_back_keyboard,
    build_draw_keyboard,
    build_draw_sell_confirm_keyboard,
    build_inventory_keyboard,
    build_contract_keyboard,
    build_my_sell_confirm_keyboard,
    build_rarity_keyboard,
    build_shop_keyboard,
    build_shop_menu_keyboard,
)
from app.messages import send_or_edit_media
from app.repo import (
    add_inventory_item_safe,
    adjust_user_balance,
    adjust_user_stars,
    consume_inventory_items,
    decrement_exclusive_stock,
    exchange_inventory_items,
    get_exclusive_stock,
    get_user,
    inventory_has_file,
    list_inventory,
    list_showcase_active_cards,
    remove_inventory_item,
    remove_inventory_item_if_current,
    sync_exclusive_stock,
    update_inventory_item_file_if_current,
    update_user_fields,
)
from app.showcase import summarize_showcase_effects
from app.utils import (
    build_draw_caption,
    escape_html,
    format_card_label,
    format_price_with_old_html,
    format_short_amount,
    get_next_rarity,
    get_user_label,
)
from cards import Card, calc_sale_price, card_currency, filter_existing_cards
from config import (
    CONTRACT_REQUIRED_COUNT,
    CONTRACT_SUCCESS_CHANCE,
    CONTRACT_COST_BALANCE,
    EXCLUSIVE_STOCK_LIMIT,
    RARITY_NAMES,
    RARITY_ORDER,
    SHOP_RARITY_ORDER,
)

router = Router()
cards_logger = logging.getLogger("cards")


def _group_items_by_rarity(
    items: List[Dict[str, object]], card_map: Dict[str, Card]
) -> Dict[str, List[Dict[str, object]]]:
    grouped: Dict[str, List[Dict[str, object]]] = {rarity: [] for rarity in RARITY_ORDER}
    for item in items:
        filename = item.get("file")
        if not filename:
            continue
        card = card_map.get(str(filename))
        if not card:
            continue
        grouped.setdefault(card.rarity, []).append(item)
    return grouped


def _build_inventory_caption(card: Card, index: int, total: int) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    sale_text = format_short_amount(calc_sale_price(card), card_currency(card))
    sale_line = format_price_with_old_html(sale_text, price_text, italic_old=False)
    return "\n".join(
        [
            f"{format_card_label(card)} - {price_text}",
            f"Цена продажи: {sale_line}",
            f"{index + 1}/{total}",
        ]
    )


def _normalize_contract_session(raw: object) -> Dict[str, object]:
    if not isinstance(raw, dict):
        return {"rarity": None, "selected": []}
    rarity = raw.get("rarity")
    selected_raw = raw.get("selected", [])
    selected = []
    if isinstance(selected_raw, list):
        for item in selected_raw:
            if not item:
                continue
            selected.append(str(item))
    return {"rarity": rarity, "selected": selected}


def _build_contract_caption(
    card: Card, rarity: str, index: int, total: int, selected_count: int
) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    rarity_label = RARITY_NAMES.get(rarity, rarity)
    chance_pct = int(round(CONTRACT_SUCCESS_CHANCE * 100))
    return "\n".join(
        [
            "Контракт на апгрейд",
            f"Редкость: {rarity_label}",
            f"Выбрано: {selected_count}/{CONTRACT_REQUIRED_COUNT}",
            f"Шанс успеха: {chance_pct}%",
            f"Стоимость: {format_short_amount(CONTRACT_COST_BALANCE, 'rub')}",
            f"{format_card_label(card)} - {price_text}",
            f"{index + 1}/{total}",
        ]
    )


async def _get_showcase_sell_multiplier(db_pool, user_id: int) -> float:
    active_cards = await list_showcase_active_cards(db_pool, user_id)
    if not active_cards:
        return 1.0
    effects = summarize_showcase_effects(active_cards)
    multiplier = float(effects.get("sell_multiplier", 1.0) or 1.0)
    return max(0.0, multiplier)


def _build_shop_caption(
    card: Card,
    index: int,
    total: int,
    discount: Optional[Dict[str, object]] = None,
    exclusive_stock: Optional[Dict[str, int]] = None,
) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    label = escape_html(format_card_label(card))
    lines = []
    if discount and is_discount_active(discount):
        percent = int(discount.get("percent", 0))
        discounted = int(discount.get("discount_price", card.price or 0))
        remaining = int(discount.get("remaining", 0))
        new_price = format_short_amount(discounted, card_currency(card))
        old_price = format_short_amount(card.price, card_currency(card))
        lines.append(
            f"{label} - {format_price_with_old_html(new_price, old_price, italic_old=True)}"
        )
        lines.append(f"АКЦИЯ -{percent}%")
        lines.append(f"Осталось: {remaining}")
    else:
        lines.append(f"{label} - {escape_html(price_text)}")
    if exclusive_stock:
        remaining = int(exclusive_stock.get("remaining", 0))
        total_stock = int(exclusive_stock.get("total", 0))
        lines.append(f"Тираж: {remaining}/{total_stock}")
    lines.append(f"{index + 1}/{total}")
    return "\n".join(lines)


async def _send_inventory_menu(
    message: Message,
    items: List[Dict[str, object]],
    card_map: Dict[str, Card],
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    counts = {rarity: 0 for rarity in RARITY_ORDER}
    grouped = _group_items_by_rarity(items, card_map)
    for rarity, entries in grouped.items():
        counts[rarity] = len(entries)
    menu_path = get_cached_menu_image("my", "Мои сосиски", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "Выбери редкость:",
            build_rarity_keyboard("my_rarity", include_menu=True, counts=counts),
            prefer_edit=prefer_edit,
            parse_mode=ParseMode.HTML,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def _send_shop_menu(
    message: Message,
    cards_by_rarity: Dict[str, List[Card]],
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    counts = {rarity: len(cards_by_rarity.get(rarity, [])) for rarity in SHOP_RARITY_ORDER}
    menu_path = get_cached_menu_image("shop", "Магазин", "Выбери редкость")
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "Магазин. Выбери редкость:",
            build_shop_menu_keyboard(counts),
            prefer_edit=prefer_edit,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def _show_inventory_card(
    message: Message,
    items: List[Dict[str, object]],
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    grouped = _group_items_by_rarity(items, card_map)
    entries = grouped.get(rarity, [])
    if not entries:
        await message.answer("У тебя нет сосисок этой редкости.")
        return
    index = max(0, min(index, len(entries) - 1))
    item = entries[index]
    card = card_map.get(item.get("file", ""))
    if not card:
        await message.answer("Карточка не найдена в базе.")
        return
    path = get_card_media_path(card)
    if not path.exists():
        await message.answer("Фото не найдено для этой карточки.")
        return
    caption = _build_inventory_caption(card, index, len(entries))
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_inventory_keyboard(rarity, index, len(entries), item["id"]),
            prefer_edit=prefer_edit,
            parse_mode=ParseMode.HTML,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def _show_contract_card(
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
    grouped = _group_items_by_rarity(items, card_map)
    entries = grouped.get(rarity, [])
    if not entries:
        await message.answer("У тебя нет сосисок этой редкости.")
        return
    index = max(0, min(index, len(entries) - 1))
    item = entries[index]
    card = card_map.get(item.get("file", ""))
    if not card:
        await message.answer("Карточка не найдена в базе.")
        return
    path = get_card_media_path(card)
    if not path.exists():
        await message.answer("Фото не найдено для этой карточки.")
        return
    selected_ids = list(dict.fromkeys(selected))
    selected_set = set(selected_ids)
    caption = _build_contract_caption(
        card, rarity, index, len(entries), len(selected_ids)
    )
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_contract_keyboard(
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


async def _show_shop_card(
    message: Message,
    user_id: int,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    db_pool,
    *,
    prefer_edit: bool,
    rate_limiter,
    owner_id: int,
) -> None:
    cards = cards_by_rarity.get(rarity, [])
    if not cards:
        await message.answer("В этой редкости пока нет карточек.")
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    discount = await ensure_discounts(db_pool, cards_by_rarity)
    discount_item = get_discount_item(discount, card.file)
    exclusive_stock = None
    allow_buy = card.price is not None
    if card.rarity == "exclusive":
        await sync_exclusive_stock(db_pool, [card.file], EXCLUSIVE_STOCK_LIMIT)
        stock = await get_exclusive_stock(db_pool, card.file)
        if stock:
            remaining, total = stock
            exclusive_stock = {"remaining": remaining, "total": total}
            allow_buy = remaining > 0
        if await inventory_has_file(db_pool, user_id, card.file):
            allow_buy = False
    caption = _build_shop_caption(
        card, index, len(cards), discount=discount_item, exclusive_stock=exclusive_stock
    )
    path = get_card_media_path(card)
    if not path.exists():
        await message.answer("Фото не найдено для этой карточки.")
        return
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_shop_keyboard(rarity, index, len(cards), allow_buy=allow_buy),
            prefer_edit=prefer_edit,
            parse_mode=ParseMode.HTML,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def _start_contract_from_item(
    message: Message,
    *,
    user_id: int,
    item_id: str,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    items = await list_inventory(db_pool, user_id)
    item = next((entry for entry in items if entry.get("id") == item_id), None)
    if not item:
        await message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card:
        await message.answer("Карточка не найдена.")
        return
    if card.rarity == "meme":
        await message.answer("Мемные сосиски нельзя улучшать через контракт.")
        return
    next_rarity = get_next_rarity(card.rarity)
    if not next_rarity:
        await message.answer("Это максимальная редкость.")
        return
    session = {"rarity": card.rarity, "selected": [item_id]}
    await update_user_fields(db_pool, user_id, {"contract_session": session})
    grouped = _group_items_by_rarity(items, card_map)
    entries = grouped.get(card.rarity, [])
    index = next(
        (idx for idx, entry in enumerate(entries) if entry.get("id") == item_id), 0
    )
    await _show_contract_card(
        message,
        items,
        card_map,
        card.rarity,
        index,
        session["selected"],
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=user_id,
    )

@router.message(Command("my"))
async def my_command(
    message: Message,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not message.from_user:
        return
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        return
    items = await list_inventory(db_pool, message.from_user.id)
    await _send_inventory_menu(
        message,
        items,
        card_map,
        prefer_edit=False,
        rate_limiter=rate_limiter,
        owner_id=message.from_user.id,
    )


@router.callback_query(F.data == "cmd|my")
async def my_menu_shortcut_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    await _send_inventory_menu(
        query.message,
        items,
        card_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "my_menu")
async def my_menu_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    await _send_inventory_menu(
        query.message,
        items,
        card_map,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("my_rarity|"))
async def my_rarity_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    rarity = query.data.split("|", 1)[1]
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_inventory_card(
        query.message,
        items,
        card_map,
        rarity,
        0,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("my_nav|"))
async def my_nav_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_inventory_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.message(Command("shop"))
async def shop_command(
    message: Message,
    cards_by_rarity: Dict[str, List[Card]],
    rate_limiter,
) -> None:
    if not message.from_user:
        return
    await _send_shop_menu(
        message,
        cards_by_rarity,
        prefer_edit=False,
        rate_limiter=rate_limiter,
        owner_id=message.from_user.id,
    )


@router.callback_query(F.data == "cmd|shop")
async def shop_menu_shortcut_callback(
    query: CallbackQuery,
    cards_by_rarity: Dict[str, List[Card]],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await _send_shop_menu(
        query.message,
        cards_by_rarity,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "shop_menu")
async def shop_menu_callback(
    query: CallbackQuery,
    cards_by_rarity: Dict[str, List[Card]],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await _send_shop_menu(
        query.message,
        cards_by_rarity,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("shop_rarity|"))
async def shop_rarity_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    rarity = query.data.split("|", 1)[1]
    if rarity not in SHOP_RARITY_ORDER:
        await query.answer("Эти сосиски нельзя купить.", show_alert=True)
        return
    await _show_shop_card(
        query.message,
        query.from_user.id,
        cards_by_rarity,
        card_map,
        rarity,
        0,
        db_pool,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("shop_nav|"))
async def shop_nav_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    if rarity not in SHOP_RARITY_ORDER:
        await query.answer("Эти сосиски нельзя купить.", show_alert=True)
        return
    await _show_shop_card(
        query.message,
        query.from_user.id,
        cards_by_rarity,
        card_map,
        rarity,
        index,
        db_pool,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("shop_buy|"))
async def shop_buy_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    if rarity not in SHOP_RARITY_ORDER:
        await query.answer("Эти сосиски нельзя купить.", show_alert=True)
        return
    cards = cards_by_rarity.get(rarity, [])
    if not cards:
        await query.message.answer("Нет карточек для покупки.")
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    if card.price is None:
        await query.message.answer("Цена не задана, купить нельзя.")
        return

    if card.rarity == "exclusive":
        if await inventory_has_file(db_pool, query.from_user.id, card.file):
            await query.message.answer("Ты уже покупал этот эксклюзив.")
            return
        await sync_exclusive_stock(db_pool, [card.file], EXCLUSIVE_STOCK_LIMIT)
        stock = await get_exclusive_stock(db_pool, card.file)
        if not stock or stock[0] <= 0:
            await query.message.answer("Этот эксклюзив уже раскупили.")
            return
        stars_left = await adjust_user_stars(
            db_pool, query.from_user.id, -int(card.price)
        )
        if stars_left is None:
            await query.message.answer(
                f"Нужно {format_short_amount(card.price, 'stars')} для покупки."
            )
            return
        remaining = await decrement_exclusive_stock(db_pool, card.file)
        if remaining is None:
            await adjust_user_stars(db_pool, query.from_user.id, int(card.price))
            await query.message.answer("Этот эксклюзив уже раскупили.")
            return
        item_id = await add_inventory_item_safe(db_pool, query.from_user.id, card.file)
        if not item_id:
            await adjust_user_stars(db_pool, query.from_user.id, int(card.price))
            await sync_exclusive_stock(db_pool, [card.file], EXCLUSIVE_STOCK_LIMIT)
            await query.message.answer("Не удалось сохранить покупку. Попробуй ещё раз.")
            return
        cards_logger.info(
            "Shop buy exclusive. user_id=%s item_id=%s file=%s",
            query.from_user.id,
            item_id,
            card.file,
        )
        await query.message.answer(
            f"Куплено за {format_short_amount(card.price, 'stars')}."
        )
        return

    discounts = await ensure_discounts(db_pool, cards_by_rarity)
    discount_item = get_discount_item(discounts, card.file)
    price = int(card.price)
    used_discount = False
    if discount_item and is_discount_active(discount_item):
        consumed = await consume_discount(db_pool, card.file)
        if consumed:
            price = int(consumed.get("discount_price", price))
            used_discount = True
    balance_left = await adjust_user_balance(db_pool, query.from_user.id, -price)
    if balance_left is None:
        if used_discount:
            await restore_discount(db_pool, card.file)
        await query.message.answer("Недостаточно средств.")
        return
    item_id = await add_inventory_item_safe(db_pool, query.from_user.id, card.file)
    if not item_id:
        await adjust_user_balance(db_pool, query.from_user.id, price)
        if used_discount:
            await restore_discount(db_pool, card.file)
        await query.message.answer("Не удалось сохранить покупку. Попробуй ещё раз.")
        return
    cards_logger.info(
        "Shop buy. user_id=%s item_id=%s file=%s price=%s currency=%s",
        query.from_user.id,
        item_id,
        card.file,
        price,
        "rub",
    )
    price_label = format_short_amount(price, "rub")
    if used_discount:
        price_label += " (акция)"
    await query.message.answer(f"Куплено за {price_label}.")


@router.callback_query(F.data.startswith("draw_sell|"))
async def draw_sell_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    item_id = query.data.split("|", 1)[1]
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card or card.price is None:
        await query.message.answer("Цена не задана, продать нельзя.")
        return
    sell_multiplier = await _get_showcase_sell_multiplier(db_pool, query.from_user.id)
    sale_price = int(round((calc_sale_price(card) or 0) * sell_multiplier))
    currency = card_currency(card)
    sale_label = format_short_amount(sale_price, currency)
    original_label = format_short_amount(card.price, currency)
    confirm_caption = "\n".join(
        [
            "Продать эту сосиску?",
            f"{escape_html(format_card_label(card))} - {sale_label} <s>{escape_html(original_label)}</s>",
        ]
    )
    await query.message.edit_caption(
        caption=confirm_caption,
        reply_markup=build_draw_sell_confirm_keyboard(item_id),
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data.startswith("draw_sell_cancel|"))
async def draw_sell_cancel_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    item_id = query.data.split("|", 1)[1]
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    expected_file = str(item.get("file", ""))
    card = card_map.get(expected_file)
    if not card:
        await query.message.answer("Карточка не найдена.")
        return
    caption = build_draw_caption(get_user_label(query.from_user), card)
    await query.message.edit_caption(
        caption=caption,
        reply_markup=build_draw_keyboard(item_id),
    )


@router.callback_query(F.data.startswith("draw_sell_confirm|"))
async def draw_sell_confirm_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    item_id = query.data.split("|", 1)[1]
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card or card.price is None:
        await query.message.answer("Цена не задана, продать нельзя.")
        return
    sell_multiplier = await _get_showcase_sell_multiplier(db_pool, query.from_user.id)
    sale_price = int(round((calc_sale_price(card) or 0) * sell_multiplier))
    if card_currency(card) == "stars":
        await adjust_user_stars(db_pool, query.from_user.id, sale_price)
    else:
        await adjust_user_balance(db_pool, query.from_user.id, sale_price)
    await remove_inventory_item(db_pool, query.from_user.id, item_id)
    cards_logger.info(
        "Sell card (inventory). user_id=%s item_id=%s file=%s price=%s currency=%s",
        query.from_user.id,
        item_id,
        card.file,
        sale_price,
        card_currency(card),
    )
    cards_logger.info(
        "Sell card. user_id=%s item_id=%s file=%s price=%s currency=%s",
        query.from_user.id,
        item_id,
        card.file,
        sale_price,
        card_currency(card),
    )
    await query.message.edit_caption(
        caption=f"Продано за {format_short_amount(sale_price, card_currency(card))}.",
        reply_markup=None,
    )


@router.callback_query(F.data.startswith("draw_upgrade|"))
async def draw_upgrade_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    item_id = query.data.split("|", 1)[1]
    await _start_contract_from_item(
        query.message,
        user_id=query.from_user.id,
        item_id=item_id,
        db_pool=db_pool,
        card_map=card_map,
        rate_limiter=rate_limiter,
    )


@router.callback_query(F.data.startswith("draw_upgrade_cancel|"))
async def draw_upgrade_cancel_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    item_id = query.data.split("|", 1)[1]
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card:
        await query.message.answer("Карточка не найдена.")
        return
    caption = build_draw_caption(get_user_label(query.from_user), card)
    await query.message.edit_caption(
        caption=caption,
        reply_markup=build_draw_keyboard(item_id),
    )


@router.callback_query(F.data.startswith("draw_upgrade_confirm|"))
async def draw_upgrade_confirm_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await query.message.edit_caption(
        caption="Апгрейды заменены на контракты. Открой «Мои» и выбери сосиску.",
        reply_markup=build_back_keyboard("my_menu"),
    )


@router.callback_query(F.data.startswith("contract_nav|"))
async def contract_nav_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_contract_session(user.get("contract_session") if user else {})
    selected = session["selected"] if session.get("rarity") == rarity else []
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_contract_card(
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


@router.callback_query(F.data.startswith("contract_pick|"))
async def contract_pick_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    try:
        index = int(index_raw)
    except ValueError:
        return
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_contract_session(user.get("contract_session") if user else {})
    if session.get("rarity") != rarity:
        session = {"rarity": rarity, "selected": []}
    selected = list(dict.fromkeys(session.get("selected", [])))
    if item_id in selected:
        selected.remove(item_id)
    else:
        if len(selected) >= CONTRACT_REQUIRED_COUNT:
            await query.answer("Нужно выбрать ровно 4 сосиски.", show_alert=False)
            return
        selected.append(item_id)
    session["selected"] = selected
    await update_user_fields(db_pool, query.from_user.id, {"contract_session": session})
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_contract_card(
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


@router.callback_query(F.data.startswith("contract_clear|"))
async def contract_clear_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity, index_raw = query.data.split("|", 2)
    try:
        index = int(index_raw)
    except ValueError:
        return
    session = {"rarity": rarity, "selected": []}
    await update_user_fields(db_pool, query.from_user.id, {"contract_session": session})
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_contract_card(
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


@router.callback_query(F.data.startswith("contract_confirm|"))
async def contract_confirm_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, rarity = query.data.split("|", 1)
    user = await get_user(db_pool, query.from_user.id)
    session = _normalize_contract_session(user.get("contract_session") if user else {})
    if session.get("rarity") != rarity:
        await query.answer("Контракт не выбран.", show_alert=False)
        return
    selected = list(dict.fromkeys(session.get("selected", [])))
    if len(selected) != CONTRACT_REQUIRED_COUNT:
        await query.answer("Нужно выбрать ровно 4 сосиски.", show_alert=False)
        return
    items = await list_inventory(db_pool, query.from_user.id)
    selected_items = [item for item in items if item.get("id") in selected]
    if len(selected_items) != CONTRACT_REQUIRED_COUNT:
        session["selected"] = [item.get("id") for item in selected_items]
        await update_user_fields(db_pool, query.from_user.id, {"contract_session": session})
        await query.message.answer("Часть сосисок пропала. Выбери заново.")
        return
    for item in selected_items:
        card = card_map.get(item.get("file", ""))
        if not card or card.rarity != rarity:
            session["selected"] = []
            await update_user_fields(db_pool, query.from_user.id, {"contract_session": session})
            await query.message.answer("Контракт требует 4 сосиски одной редкости.")
            return
    if rarity == "meme":
        await query.message.answer("Мемные сосиски нельзя улучшать через контракт.")
        return
    next_rarity = get_next_rarity(rarity)
    if not next_rarity:
        await query.message.answer("Это максимальная редкость.")
        return
    balance = int(user.get("balance", 0) or 0)
    if balance < CONTRACT_COST_BALANCE:
        await query.message.answer("Недостаточно средств для контракта.")
        return
    charged = await adjust_user_balance(
        db_pool, query.from_user.id, -CONTRACT_COST_BALANCE
    )
    if charged is None:
        await query.message.answer("Недостаточно средств для контракта.")
        return
    success = random.random() < CONTRACT_SUCCESS_CHANCE
    if success:
        available = filter_existing_cards(cards_by_rarity).get(next_rarity, [])
        if not available:
            await query.message.answer("Нет карточек следующей редкости.")
            await adjust_user_balance(
                db_pool, query.from_user.id, CONTRACT_COST_BALANCE
            )
            return
        upgraded = random.choice(available)
        try:
            new_item_id, consumed = await exchange_inventory_items(
                db_pool, query.from_user.id, selected, upgraded.file
            )
        except Exception:
            await query.message.answer("Не удалось провести контракт. Попробуй снова.")
            await adjust_user_balance(
                db_pool, query.from_user.id, CONTRACT_COST_BALANCE
            )
            return
        cards_logger.info(
            "Contract success. user_id=%s from=%s to=%s item_id=%s",
            query.from_user.id,
            rarity,
            upgraded.rarity,
            new_item_id,
        )
        await update_user_fields(db_pool, query.from_user.id, {"contract_session": None})
        path = get_card_media_path(upgraded)
        caption = "\n".join(
            [
                "Контракт успешен!",
                f"{format_card_label(upgraded)} - {format_short_amount(upgraded.price, card_currency(upgraded))}",
            ]
        )
        with path.open("rb") as photo:
            await send_or_edit_media(
                query.message,
                photo,
                caption,
                build_back_keyboard("my_menu"),
                prefer_edit=True,
                parse_mode=ParseMode.HTML,
                rate_limiter=rate_limiter,
                owner_id=query.from_user.id,
            )
        await query.answer()
        return
    try:
        await consume_inventory_items(db_pool, query.from_user.id, selected)
    except Exception:
        await query.message.answer("Контракт не удалось выполнить. Попробуй снова.")
        await adjust_user_balance(
            db_pool, query.from_user.id, CONTRACT_COST_BALANCE
        )
        return
    cards_logger.info(
        "Contract failed. user_id=%s rarity=%s items=%s",
        query.from_user.id,
        rarity,
        selected,
    )
    await update_user_fields(db_pool, query.from_user.id, {"contract_session": None})
    await query.message.edit_caption(
        caption="Контракт провален. Сосиски потеряны.",
        reply_markup=build_back_keyboard("my_menu"),
    )
    await query.answer()


@router.callback_query(F.data.startswith("my_sell|"))
async def my_sell_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card or card.price is None:
        await query.message.answer("Цена не задана, продать нельзя.")
        return
    sell_multiplier = await _get_showcase_sell_multiplier(db_pool, query.from_user.id)
    sale_price = int(round((calc_sale_price(card) or 0) * sell_multiplier))
    currency = card_currency(card)
    sale_label = format_short_amount(sale_price, currency)
    original_label = format_short_amount(card.price, currency)
    confirm_caption = "\n".join(
        [
            "Продать эту сосиску?",
            f"{escape_html(format_card_label(card))} - {sale_label} <s>{escape_html(original_label)}</s>",
        ]
    )
    await query.message.edit_caption(
        caption=confirm_caption,
        reply_markup=build_my_sell_confirm_keyboard(item_id, rarity, int(index_raw)),
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data.startswith("my_sell_cancel|"))
async def my_sell_cancel_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    try:
        index = int(index_raw)
    except ValueError:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_inventory_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )


@router.callback_query(F.data.startswith("my_sell_confirm|"))
async def my_sell_confirm_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    try:
        index = int(index_raw)
    except ValueError:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    item = next((item for item in items if item.get("id") == item_id), None)
    if not item:
        await query.message.answer("Эта сосиска уже продана.")
        return
    card = card_map.get(item.get("file", ""))
    if not card or card.price is None:
        await query.message.answer("Цена не задана, продать нельзя.")
        return
    sell_multiplier = await _get_showcase_sell_multiplier(db_pool, query.from_user.id)
    sale_price = int(round((calc_sale_price(card) or 0) * sell_multiplier))
    if card_currency(card) == "stars":
        await adjust_user_stars(db_pool, query.from_user.id, sale_price)
    else:
        await adjust_user_balance(db_pool, query.from_user.id, sale_price)
    await remove_inventory_item(db_pool, query.from_user.id, item_id)
    items = await list_inventory(db_pool, query.from_user.id)
    grouped = _group_items_by_rarity(items, card_map)
    entries = grouped.get(rarity, [])
    if not entries:
        await query.message.edit_caption(
            caption="У тебя больше нет сосисок этой редкости.",
            reply_markup=build_rarity_keyboard(
                "my_rarity", include_menu=True, back_callback="menu"
            ),
        )
        return
    new_index = min(index, len(entries) - 1)
    await _show_inventory_card(
        query.message,
        items,
        card_map,
        rarity,
        new_index,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )
    await query.message.answer(
        f"Продано за {format_short_amount(sale_price, card_currency(card))}."
    )


@router.callback_query(F.data.startswith("my_upgrade|"))
async def my_upgrade_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, item_id, rarity, index_raw = query.data.split("|", 3)
    await _start_contract_from_item(
        query.message,
        user_id=query.from_user.id,
        item_id=item_id,
        db_pool=db_pool,
        card_map=card_map,
        rate_limiter=rate_limiter,
    )


@router.callback_query(F.data.startswith("my_upgrade_cancel|"))
async def my_upgrade_cancel_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, _, rarity, index_raw = query.data.split("|", 3)
    try:
        index = int(index_raw)
    except ValueError:
        return
    items = await list_inventory(db_pool, query.from_user.id)
    await _show_inventory_card(
        query.message,
        items,
        card_map,
        rarity,
        index,
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=query.from_user.id,
    )


@router.callback_query(F.data.startswith("my_upgrade_confirm|"))
async def my_upgrade_confirm_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await query.message.edit_caption(
        caption="Апгрейды заменены на контракты. Открой «Мои» и выбери сосиску.",
        reply_markup=build_back_keyboard("my_menu"),
    )

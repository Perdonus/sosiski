from __future__ import annotations

import secrets
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app.images import get_card_media_path, get_cached_menu_image
from app.keyboards import (
    build_trade_accept_keyboard,
    build_trade_confirm_keyboard,
    build_trade_item_keyboard,
    build_trade_rarity_keyboard,
)
from app.messages import send_or_edit_media
from app.ownership import remember_owner
from app.repo import (
    create_trade,
    delete_trade,
    fetch_user_by_tag,
    get_trade,
    list_inventory,
    transfer_inventory_item,
    update_trade,
)
from app.utils import escape_html, format_card_label, format_short_amount
from cards import Card, card_currency, card_display_name
from config import PUBLIC_BOT_USERNAME, RARITY_NAMES

router = Router()


@router.callback_query(F.data == "cmd|trade")
async def trade_menu_callback(query: CallbackQuery) -> None:
    if not query.message:
        return
    await query.message.answer("Используй: /trade @username")
    await query.answer()


def _trade_user_label(trade: Dict[str, object], role: str) -> str:
    tag = trade.get(f"{role}_tag")
    name = trade.get(f"{role}_name")
    if tag:
        return f"@{tag}"
    if name:
        return str(name)
    user_id = trade.get(f"{role}_id")
    return str(user_id) if user_id else "Пользователь"


def _filter_inventory_by_rarity(
    items: List[Dict[str, object]], card_map: Dict[str, Card], rarity: str
) -> List[Dict[str, object]]:
    result = []
    for item in items:
        card = card_map.get(item.get("file", ""))
        if card and card.rarity == rarity:
            result.append(item)
    return result


def _build_trade_caption(card: Card, index: int, total: int) -> str:
    price = format_short_amount(card.price, card_currency(card))
    label = escape_html(format_card_label(card))
    return "\n".join([f"{label} - {price}", f"{index + 1}/{total}"])


async def _show_trade_card(
    message: Message,
    db_pool,
    user_id: int,
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    token: str,
    role: str,
    rate_limiter,
    owner_id: int,
) -> None:
    items = await list_inventory(db_pool, user_id)
    entries = _filter_inventory_by_rarity(items, card_map, rarity)
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
    caption = _build_trade_caption(card, index, len(entries))
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_trade_item_keyboard(token, role, rarity, index, len(entries), item["id"]),
            prefer_edit=True,
            parse_mode=ParseMode.HTML,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


@router.message(Command("trade"))
async def trade_command(
    message: Message,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not message.from_user:
        return
    if not message.text or len(message.text.split()) < 2:
        await message.answer("Используй: /trade @username")
        return
    target_raw = message.text.split(maxsplit=1)[1].strip()
    target = await fetch_user_by_tag(db_pool, target_raw)
    if not target:
        await message.answer("Пользователь не найден. Пусть он напишет боту /start.")
        return
    target_id = int(target["user_id"])
    if target_id == message.from_user.id:
        await message.answer("Нельзя трейдить с самим собой.")
        return
    items = await list_inventory(db_pool, message.from_user.id)
    if not items:
        await message.answer("У тебя нет сосисок для трейда.")
        return

    token = secrets.token_urlsafe(6)
    trade = {
        "token": token,
        "from_id": message.from_user.id,
        "from_name": message.from_user.full_name or "",
        "from_tag": message.from_user.username or "",
        "to_id": target_id,
        "to_name": target.get("username", ""),
        "to_tag": target.get("user_tag", ""),
        "from_item_id": None,
        "to_item_id": None,
        "status": "draft",
    }
    await create_trade(db_pool, trade)
    caption = ""
    menu_path = get_cached_menu_image("trade", "Трейд", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_trade_rarity_keyboard(token, "offer"),
            prefer_edit=False,
            rate_limiter=rate_limiter,
            owner_id=message.from_user.id,
        )


@router.message(Command("trade_accept"))
async def trade_accept_command(
    message: Message,
    db_pool,
    rate_limiter,
) -> None:
    if not message.from_user:
        return
    if not message.text or len(message.text.split()) < 2:
        await message.answer("Используй: /trade_accept <код>")
        return
    token = message.text.split(maxsplit=1)[1].strip()
    trade = await get_trade(db_pool, token)
    if not trade or trade.get("status") != "open":
        await message.answer("Трейд не найден или закрыт.")
        return
    if int(trade.get("from_id", 0)) == message.from_user.id:
        await message.answer("Нельзя принять свой трейд.")
        return
    if int(trade.get("to_id", 0)) not in {0, message.from_user.id}:
        await message.answer("Этот трейд предназначен другому игроку.")
        return
    await update_trade(db_pool, token, {"status": "accepting", "to_id": message.from_user.id})
    menu_path = get_cached_menu_image("trade_accept", "Трейд", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "",
            build_trade_rarity_keyboard(token, "accept"),
            prefer_edit=False,
            rate_limiter=rate_limiter,
            owner_id=message.from_user.id,
        )


@router.callback_query(F.data.startswith("trade_rarity_menu|"))
async def trade_rarity_menu_callback(query: CallbackQuery, db_pool) -> None:
    if not query.message:
        return
    _, role, token = query.data.split("|", 2)
    trade = await get_trade(db_pool, token)
    if not trade:
        await query.message.answer("Трейд не найден.")
        return
    user_id = query.from_user.id
    if role == "offer" and int(trade.get("from_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    if role == "accept" and int(trade.get("to_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    await query.message.edit_reply_markup(
        reply_markup=build_trade_rarity_keyboard(token, role)
    )
    await query.answer()


@router.callback_query(F.data.startswith("trade_rarity|"))
async def trade_rarity_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, role, token, rarity = query.data.split("|", 3)
    trade = await get_trade(db_pool, token)
    if not trade:
        await query.message.answer("Трейд не найден.")
        return
    user_id = query.from_user.id
    expected_status = "draft" if role == "offer" else "accepting"
    if trade.get("status") != expected_status:
        await query.message.answer("Трейд не готов к этому действию.")
        return
    if role == "offer" and int(trade.get("from_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    if role == "accept" and int(trade.get("to_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    await _show_trade_card(
        query.message,
        db_pool,
        user_id,
        card_map,
        rarity,
        0,
        token,
        role,
        rate_limiter,
        owner_id=user_id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("trade_nav|"))
async def trade_nav_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
    rate_limiter,
) -> None:
    if not query.message:
        return
    _, role, token, rarity, index_raw = query.data.split("|", 4)
    try:
        index = int(index_raw)
    except ValueError:
        return
    trade = await get_trade(db_pool, token)
    if not trade:
        await query.message.answer("Трейд не найден.")
        return
    user_id = query.from_user.id
    expected_status = "draft" if role == "offer" else "accepting"
    if trade.get("status") != expected_status:
        await query.message.answer("Трейд не готов к этому действию.")
        return
    if role == "offer" and int(trade.get("from_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    if role == "accept" and int(trade.get("to_id", 0)) != user_id:
        await query.message.answer("Это не твой трейд.")
        return
    await _show_trade_card(
        query.message,
        db_pool,
        user_id,
        card_map,
        rarity,
        index,
        token,
        role,
        rate_limiter,
        owner_id=user_id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("trade_pick|"))
async def trade_pick_callback(
    query: CallbackQuery,
    db_pool,
    card_map: Dict[str, Card],
) -> None:
    if not query.message:
        return
    _, role, token, item_id, rarity, index_raw = query.data.split("|", 5)
    trade = await get_trade(db_pool, token)
    if not trade:
        await query.message.answer("Трейд не найден.")
        return
    user_id = query.from_user.id
    if role == "offer":
        if trade.get("status") != "draft" or int(trade.get("from_id", 0)) != user_id:
            await query.message.answer("Это не твой трейд.")
            return
        items = await list_inventory(db_pool, user_id)
        item = next((it for it in items if it.get("id") == item_id), None)
        if not item:
            await query.message.answer("Эта сосиска уже не в инвентаре.")
            return
        await update_trade(db_pool, token, {"from_item_id": item_id, "status": "open"})
        target_label = _trade_user_label(trade, "to")
        await query.message.edit_caption(
            caption=f"Трейд отправлен для {target_label}.",
            reply_markup=None,
        )
        offer_card = card_map.get(item.get("file", ""))
        offer_line = (
            f"Тебе предлагают: {card_display_name(offer_card)}"
            if offer_card
            else "Тебе предлагают трейд."
        )
        try:
            sent = await query.message.bot.send_message(
                chat_id=int(trade["to_id"]),
                text="\n".join(
                    [
                        f"Трейд от: {_trade_user_label(trade, 'from')}",
                        offer_line,
                    ]
                ),
                reply_markup=build_trade_accept_keyboard(token),
            )
            if sent:
                remember_owner(sent.chat.id, sent.message_id, int(trade["to_id"]))
        except (TelegramForbiddenError, TelegramBadRequest) as exc:
            message_text = str(exc).lower()
            if isinstance(exc, TelegramForbiddenError) or "chat not found" in message_text:
                username = (PUBLIC_BOT_USERNAME or "sosiskikazikbot").lstrip("@")
                link = f"https://t.me/{username}?start=trade"
                await query.message.answer(
                    "\n".join(
                        [
                            "Не могу написать пользователю в ЛС.",
                            "Пусть он зайдёт в личку с ботом и нажмёт /start:",
                            link,
                        ]
                    )
                )
            else:
                await query.message.answer("Не удалось отправить трейд в личку.")
        await query.answer()
        return

    if role == "accept":
        if trade.get("status") != "accepting" or int(trade.get("to_id", 0)) != user_id:
            await query.message.answer("Это не твой трейд.")
            return
        items = await list_inventory(db_pool, user_id)
        item = next((it for it in items if it.get("id") == item_id), None)
        if not item:
            await query.message.answer("Эта сосиска уже не в инвентаре.")
            return
        await update_trade(
            db_pool, token, {"to_item_id": item_id, "status": "confirming"}
        )
        offered = await list_inventory(db_pool, int(trade.get("from_id", 0)))
        offered_item = next(
            (it for it in offered if it.get("id") == trade.get("from_item_id")),
            None,
        )
        offered_card = (
            card_map.get(offered_item.get("file", "")) if offered_item else None
        )
        give_card = card_map.get(item.get("file", ""))
        offer_text = card_display_name(offered_card) if offered_card else "сосиску"
        give_text = card_display_name(give_card) if give_card else "сосиску"
        summary = "\n".join(
            [
                f"{_trade_user_label(trade, 'from')} отдаёт: {offer_text}",
                f"{_trade_user_label(trade, 'to')} отдаёт: {give_text}",
                "Подтверди трейд.",
            ]
        )
        try:
            sent = await query.message.bot.send_message(
                chat_id=int(trade["from_id"]),
                text=summary,
                reply_markup=build_trade_confirm_keyboard(token),
            )
            if sent:
                remember_owner(sent.chat.id, sent.message_id, int(trade["from_id"]))
        except Exception:
            await query.message.answer("Не удалось отправить запрос на подтверждение.")
        await query.message.answer("Запрос отправлен. Жди подтверждения.")
        await query.answer()


@router.callback_query(F.data.startswith("trade_accept|"))
async def trade_accept_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade or trade.get("status") != "open":
        await query.message.answer("Трейд не найден или закрыт.")
        return
    if int(trade.get("from_id", 0)) == query.from_user.id:
        await query.message.answer("Нельзя принять свой трейд.")
        return
    await update_trade(
        db_pool, token, {"status": "accepting", "to_id": query.from_user.id}
    )
    menu_path = get_cached_menu_image("trade_accept", "Трейд", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            query.message,
            photo,
            "",
            build_trade_rarity_keyboard(token, "accept"),
            prefer_edit=False,
            rate_limiter=rate_limiter,
            owner_id=query.from_user.id,
        )
    await query.answer()


@router.callback_query(F.data.startswith("trade_accept_none|"))
async def trade_accept_none_callback(
    query: CallbackQuery, db_pool, card_map: Dict[str, Card]
) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade or trade.get("status") != "accepting":
        await query.message.answer("Трейд не найден или закрыт.")
        return
    if int(trade.get("to_id", 0)) != query.from_user.id:
        await query.message.answer("Это не твой трейд.")
        return
    await update_trade(
        db_pool, token, {"to_item_id": None, "status": "confirming"}
    )
    offered = await list_inventory(db_pool, int(trade.get("from_id", 0)))
    offered_item = next(
        (it for it in offered if it.get("id") == trade.get("from_item_id")), None
    )
    if not offered_item:
        await delete_trade(db_pool, token)
        await query.message.answer("Трейд отменён: сосиски нет у автора.")
        return
    offered_card = card_map.get(offered_item.get("file", "")) if offered_item else None
    offer_text = card_display_name(offered_card) if offered_card else "сосиску"
    summary = "\n".join(
        [
            f"{_trade_user_label(trade, 'from')} отдаёт: {offer_text}",
            f"{_trade_user_label(trade, 'to')} отдаёт: ничего",
            "Подтверди трейд.",
        ]
    )
    try:
        sent = await query.message.bot.send_message(
            chat_id=int(trade["from_id"]),
            text=summary,
            reply_markup=build_trade_confirm_keyboard(token),
        )
        if sent:
            remember_owner(sent.chat.id, sent.message_id, int(trade["from_id"]))
    except Exception:
        await query.message.answer("Не удалось отправить запрос на подтверждение.")
    await query.message.answer("Запрос отправлен. Жди подтверждения.")
    await query.answer()


@router.callback_query(F.data.startswith("trade_confirm|"))
async def trade_confirm_callback(query: CallbackQuery, db_pool, card_map: Dict[str, Card]) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade or trade.get("status") != "confirming":
        await query.message.answer("Трейд не найден или закрыт.")
        return
    if int(trade.get("from_id", 0)) != query.from_user.id:
        await query.message.answer("Это не твой трейд.")
        return
    from_id = int(trade.get("from_id", 0))
    to_id = int(trade.get("to_id", 0) or 0)
    if to_id <= 0:
        await delete_trade(db_pool, token)
        await query.message.answer("Трейд отменён: нет второй стороны.")
        return
    offered_items = await list_inventory(db_pool, from_id)
    offered_item = next(
        (it for it in offered_items if it.get("id") == trade.get("from_item_id")),
        None,
    )
    if not offered_item:
        await delete_trade(db_pool, token)
        await query.message.answer("Трейд отменён: сосиски нет у автора.")
        return
    give_item = None
    if trade.get("to_item_id"):
        to_items = await list_inventory(db_pool, to_id)
        give_item = next(
            (it for it in to_items if it.get("id") == trade.get("to_item_id")), None
        )
        if not give_item:
            await delete_trade(db_pool, token)
            await query.message.answer("Трейд отменён: сосиски нет у второй стороны.")
            return

    await transfer_inventory_item(db_pool, offered_item["id"], from_id, to_id)
    if give_item:
        await transfer_inventory_item(db_pool, give_item["id"], to_id, from_id)
    await delete_trade(db_pool, token)
    offered_card = card_map.get(offered_item.get("file", ""))
    give_card = card_map.get(give_item.get("file", "")) if give_item else None
    receive_text = card_display_name(give_card) if give_card else "ничего"
    await query.message.answer(f"Трейд завершён. Ты получил {receive_text}.")
    try:
        offer_text = card_display_name(offered_card) if offered_card else "сосиску"
        await query.message.bot.send_message(
            chat_id=to_id,
            text=f"Трейд завершён. Ты получил {offer_text}.",
        )
    except Exception:
        pass
    await query.answer()


@router.callback_query(F.data.startswith("trade_confirm_cancel|"))
async def trade_confirm_cancel_callback(query: CallbackQuery, db_pool) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade:
        return
    if int(trade.get("from_id", 0)) != query.from_user.id:
        await query.message.answer("Это не твой трейд.")
        return
    to_id = trade.get("to_id")
    await delete_trade(db_pool, token)
    await query.message.answer("Трейд отменён.")
    try:
        if to_id:
            await query.message.bot.send_message(
                chat_id=int(to_id), text="Трейд отменён."
            )
    except Exception:
        pass
    await query.answer()


@router.callback_query(F.data.startswith("trade_cancel|"))
async def trade_cancel_callback(query: CallbackQuery, db_pool) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade:
        return
    if int(trade.get("from_id", 0)) != query.from_user.id:
        await query.message.answer("Это не твой трейд.")
        return
    await delete_trade(db_pool, token)
    await query.message.answer("Трейд отменён.")
    await query.answer()


@router.callback_query(F.data.startswith("trade_decline|"))
async def trade_decline_callback(query: CallbackQuery, db_pool) -> None:
    if not query.message:
        return
    _, token = query.data.split("|", 1)
    trade = await get_trade(db_pool, token)
    if not trade:
        return
    if int(trade.get("to_id", 0)) != query.from_user.id:
        return
    from_id = trade.get("from_id")
    await delete_trade(db_pool, token)
    await query.message.answer("Трейд отменён.")
    try:
        if from_id:
            await query.message.bot.send_message(
                chat_id=int(from_id), text="Трейд отклонён."
            )
    except Exception:
        pass
    await query.answer()

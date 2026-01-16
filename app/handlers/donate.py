from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from aiogram import Router, F
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile, LabeledPrice, Message, PreCheckoutQuery

from app.images import get_cached_menu_image
from app.keyboards import (
    build_stars_menu_keyboard,
    build_vip_menu_keyboard,
)
from app.messages import send_or_edit_media
from app.repo import (
    adjust_user_stars,
    adjust_user_stars_donated,
    get_or_create_user,
    get_user,
    update_user_fields,
)
from app.utils import format_vip_remaining
from config import (
    STARS_CURRENCY,
    STARS_PROVIDER_TOKEN,
    VIP_COST_STARS,
    VIP_DURATION_DAYS,
    VIP_RENEW_WINDOW_DAYS,
)

router = Router()


def build_stars_payload(amount: int) -> str:
    return f"stars_topup:{int(amount)}"


def parse_stars_payload(payload: str) -> Optional[int]:
    if not payload or not payload.startswith("stars_topup:"):
        return None
    raw = payload.split(":", 1)[1]
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _vip_left_seconds(user: dict, now: datetime) -> int:
    vip_until = user.get("vip_until")
    if isinstance(vip_until, datetime) and vip_until > now:
        return int((vip_until - now).total_seconds())
    return 0


def compute_vip_until(user: dict, now: datetime) -> datetime:
    current = user.get("vip_until")
    base = current if isinstance(current, datetime) and current > now else now
    return base + timedelta(days=VIP_DURATION_DAYS)


async def send_stars_invoice(message: Message, amount: int) -> None:
    payload = build_stars_payload(amount)
    await message.answer_invoice(
        title=f"{amount} звёзд",
        description="Пополнение баланса звёзд.",
        payload=payload,
        provider_token=STARS_PROVIDER_TOKEN,
        currency=STARS_CURRENCY,
        prices=[LabeledPrice(label=f"{amount}⭐", amount=amount)],
    )


async def _send_stars_menu(
    message: Message,
    db_pool,
    user: dict,
    *,
    rate_limiter,
    prefer_edit: bool,
    owner_id: int,
) -> None:
    await update_user_fields(db_pool, owner_id, {"input_mode": None})
    stars = int(user.get("stars", 0) or 0)
    caption = "\n".join(
        [
            f"Звёзд на балансе: {stars}⭐",
            "Выберите сумму пополнения:",
        ]
    )
    menu_path = get_cached_menu_image("donate_stars", "Звёзды", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            caption,
            build_stars_menu_keyboard(),
            prefer_edit=prefer_edit,
            rate_limiter=rate_limiter,
            owner_id=owner_id,
        )


async def send_stars_menu_to_user(bot, db_pool, user_id: int) -> None:
    user = await get_user(db_pool, user_id)
    if not user:
        user = await get_or_create_user(db_pool, user_id, "", "")
    await update_user_fields(db_pool, user_id, {"input_mode": None})
    stars = int(user.get("stars", 0) or 0)
    caption = "\n".join(
        [
            f"Звёзд на балансе: {stars}⭐",
            "Выберите сумму пополнения:",
        ]
    )
    menu_path = get_cached_menu_image("donate_stars", "Звёзды", None)
    await bot.send_photo(
        chat_id=user_id,
        photo=FSInputFile(str(menu_path)),
        caption=caption,
        reply_markup=build_stars_menu_keyboard(),
    )


@router.message(Command("pay"))
async def stars_menu_command(message: Message, db_pool, rate_limiter) -> None:
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        return
    await _send_stars_menu(
        message,
        db_pool,
        user,
        rate_limiter=rate_limiter,
        prefer_edit=False,
        owner_id=message.from_user.id,
    )


@router.callback_query(F.data == "donate_stars")
async def donate_stars_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    await _send_stars_menu(
        query.message,
        db_pool,
        user,
        rate_limiter=rate_limiter,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data == "donate_stars_topup")
async def donate_stars_topup_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    await _send_stars_menu(
        query.message,
        db_pool,
        user,
        rate_limiter=rate_limiter,
        prefer_edit=True,
        owner_id=query.from_user.id,
    )
    await query.answer()


@router.callback_query(F.data.startswith("stars_buy|"))
async def stars_buy_callback(query: CallbackQuery) -> None:
    if not query.message:
        return
    _, amount_raw = query.data.split("|", 1)
    try:
        amount = int(amount_raw)
    except ValueError:
        return
    await send_stars_invoice(query.message, amount)
    await query.answer()


@router.message(F.text & ~F.text.startswith("/"))
async def text_input_handler(message: Message, db_pool) -> None:
    if message.chat.type != "private":
        raise SkipHandler()
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        raise SkipHandler()
    mode = str(user.get("input_mode") or "")
    if mode != "stars_topup":
        raise SkipHandler()
    text = (message.text or "").strip()
    await update_user_fields(db_pool, message.from_user.id, {"input_mode": None})
    await message.answer("Используй кнопки для пополнения.")


@router.message(Command("vip"))
async def vip_menu_command(message: Message, db_pool, rate_limiter, tg_user=None) -> None:
    if tg_user is None:
        tg_user = message.from_user
    if not tg_user:
        return
    user = await get_user(db_pool, tg_user.id)
    if not user:
        return
    now = datetime.now(timezone.utc)
    left_seconds = _vip_left_seconds(user, now)
    status = f"Осталось: {format_vip_remaining(left_seconds)}" if left_seconds else "VIP: нет"
    lines = [
        status,
        "VIP бонусы:",
        "- Быстрее откат крутки",
        "- Больше фри-спинов в Казике",
        "- Повышенный шанс редких сосисок",
        f"Срок: {VIP_DURATION_DAYS} дней",
        f"Стоимость: {VIP_COST_STARS}⭐",
    ]
    menu_path = get_cached_menu_image("vip", "VIP", None)
    with menu_path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            "\n".join(lines),
            build_vip_menu_keyboard(),
            prefer_edit=False,
            rate_limiter=rate_limiter,
            owner_id=tg_user.id,
        )


@router.callback_query(F.data == "donate_vip")
async def donate_vip_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    await vip_menu_command(
        query.message,
        db_pool=db_pool,
        rate_limiter=rate_limiter,
        tg_user=query.from_user,
    )
    await query.answer()


@router.callback_query(F.data == "vip_buy_stars")
async def vip_buy_stars_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    now = datetime.now(timezone.utc)
    left_seconds = _vip_left_seconds(user, now)
    if left_seconds > VIP_RENEW_WINDOW_DAYS * 86400:
        await query.message.answer(
            f"Продление доступно за {VIP_RENEW_WINDOW_DAYS}д до конца подписки."
        )
        await query.answer()
        return
    stars_left = await adjust_user_stars(db_pool, query.from_user.id, -VIP_COST_STARS)
    if stars_left is None:
        await _send_stars_menu(
            query.message,
            db_pool,
            user,
            rate_limiter=rate_limiter,
            prefer_edit=True,
            owner_id=query.from_user.id,
        )
        await query.answer()
        return
    vip_until = compute_vip_until(user, now)
    await update_user_fields(
        db_pool,
        query.from_user.id,
        {"vip_until": vip_until, "vip": True},
    )
    left = int((vip_until - now).total_seconds())
    await query.message.answer(
        f"✅ VIP активирован! Осталось {format_vip_remaining(left)}."
    )


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery) -> None:
    amount = parse_stars_payload(query.invoice_payload)
    if amount is None or amount <= 0:
        await query.answer(
            ok=False,
            error_message="Неверный платёж.",
        )
        return
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message, db_pool) -> None:
    payment = message.successful_payment
    if not payment:
        return
    amount = parse_stars_payload(payment.invoice_payload)
    if amount is None or amount <= 0:
        return
    stars_left = await adjust_user_stars(db_pool, message.from_user.id, amount)
    await adjust_user_stars_donated(db_pool, message.from_user.id, amount)
    await message.answer(
        f"✅ Зачислено {amount}⭐. Теперь на балансе {stars_left}⭐."
    )

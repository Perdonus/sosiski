from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app.images import (
    build_kazik_spin_image,
    get_card_media_path,
    get_cached_kazik_title_image,
    get_cached_menu_image,
)
from app.kazik import (
    kazik_reset_remaining_seconds,
    kazik_should_reset,
    kazik_free_spins_limit,
    kazik_daily_free_left,
    kazik_spin_button_label,
)
from app.keyboards import (
    build_draw_keyboard,
    build_kazik_buy_keyboard,
    build_kazik_open_dm_keyboard,
    build_kazik_spin_keyboard,
    build_kazik_webapp_keyboard,
    build_stars_menu_keyboard,
)
from app.logic import get_cooldown_seconds, is_vip
from app.messages import send_or_edit_media
from app.repo import (
    add_inventory_item_safe,
    get_user,
    list_showcase_active_cards,
    update_user_fields,
)
from app.showcase import summarize_showcase_effects
from app.utils import (
    boost_drop_chances,
    format_duration,
    format_card_label,
    get_user_label,
    kazik_reward_rarities,
    now_local,
    roll_kazik_digits,
    user_age_days,
)
from cards import (
    Card,
    card_display_name,
    filter_existing_cards,
    pick_random_card,
)
from config import (
    KAZIK_SPIN_DELAY,
    KAZIK_PAID_SPINS_FOR_BONUS,
    KAZIK_BONUS_SPINS_PER_BATCH,
    KAZIK_GUARANTEE_SPINS,
    KAZIK_STAR_SPIN_COST,
    KAZIK_WIN_CHANCE,
    NEWBIE_DAYS_STRONG,
    NEWBIE_DAYS_VIP,
    NEWBIE_DROP_CHANCE_MULTIPLIER,
    NEWBIE_KAZIK_WIN_MULTIPLIER,
    NON_VIP_DROP_CHANCE_MULTIPLIER,
    NON_VIP_DROP_NERF_RARITIES,
    RARITY_NAMES,
    VIP_DROP_CHANCE_MULTIPLIER,
    VIP_DROP_BOOST_RARITIES,
    VIP_KAZIK_WIN_CHANCE,
    VIP_DAILY_ROLL_LIMIT,
)

router = Router()
cards_logger = logging.getLogger("cards")
kazik_logger = logging.getLogger("kazik")


def pick_kazik_reward_card(
    by_rarity: Dict[str, List[Card]],
    digit: int,
) -> Optional[Card]:
    available_by_rarity = filter_existing_cards(by_rarity)
    pool: List[Card] = []
    for rarity in kazik_reward_rarities(digit):
        pool.extend(available_by_rarity.get(rarity, []))
    if not pool:
        return None
    return pool[0] if len(pool) == 1 else random.choice(pool)


def build_draw_caption(user_label: str, card: Card) -> str:
    return "\n".join(
        [
            f"{user_label} выбил {card_display_name(card)}",
            f"Редкость: {RARITY_NAMES.get(card.rarity, card.rarity)}",
        ]
    )


def _apply_drop_modifiers(
    user: Dict[str, object],
    drop_chances: Dict[str, float],
) -> Dict[str, float]:
    now = now_local()
    age_days = user_age_days(user, now)
    vip = is_vip(user)
    adjusted = drop_chances
    if age_days is not None:
        if age_days < NEWBIE_DAYS_VIP:
            multiplier = VIP_DROP_CHANCE_MULTIPLIER
            if age_days < NEWBIE_DAYS_STRONG:
                multiplier *= NEWBIE_DROP_CHANCE_MULTIPLIER
            adjusted = boost_drop_chances(
                adjusted, VIP_DROP_BOOST_RARITIES, multiplier
            )
            return adjusted
    if vip:
        return boost_drop_chances(
            adjusted, VIP_DROP_BOOST_RARITIES, VIP_DROP_CHANCE_MULTIPLIER
        )
    return boost_drop_chances(
        adjusted, NON_VIP_DROP_NERF_RARITIES, NON_VIP_DROP_CHANCE_MULTIPLIER
    )


def _kazik_win_chance(user: Dict[str, object], now: datetime) -> float:
    vip = is_vip(user)
    base = VIP_KAZIK_WIN_CHANCE if vip else KAZIK_WIN_CHANCE
    age_days = user_age_days(user, now)
    if age_days is None:
        return base
    vip_level = max(base, VIP_KAZIK_WIN_CHANCE)
    if age_days < NEWBIE_DAYS_STRONG:
        return vip_level * NEWBIE_KAZIK_WIN_MULTIPLIER
    if age_days < NEWBIE_DAYS_VIP:
        return vip_level
    return base


def _kazik_menu_lines(user: Dict[str, object], now: datetime) -> List[str]:
    daily_free = kazik_daily_free_left(user, now)
    bonus_spins = int(user.get("kazik_bonus_spins", 0) or 0)
    lines = [
        f"Фри спинов: {daily_free}",
        f"Куплено спинов: {bonus_spins}",
    ]
    reset_left = kazik_reset_remaining_seconds(user, now)
    if reset_left:
        lines.append(f"Сброс через: {format_duration(reset_left)}")
    return lines


async def _send_stars_menu(
    message: Message,
    db_pool,
    user: Dict[str, object],
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


async def _send_kazik_notice(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer(
            "Игры в mini apps.",
            reply_markup=build_kazik_open_dm_keyboard(),
        )
        return
    await message.answer(
        "Игры в mini apps.",
        reply_markup=build_kazik_webapp_keyboard(),
    )


@router.message(Command("sosiska"))
async def sosiska_command(
    message: Message,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    drop_chances: Dict[str, float],
    rate_limiter,
    tg_user=None,
) -> None:
    if tg_user is None:
        tg_user = message.from_user
    if not tg_user:
        return
    user = await get_user(db_pool, tg_user.id)
    if not user:
        return
    now = now_local()
    if is_vip(user):
        daily_date = user.get("rolls_daily_date")
        if daily_date != now.date():
            user["rolls_daily_date"] = now.date()
            user["rolls_daily_used"] = 0
        daily_used = int(user.get("rolls_daily_used", 0) or 0)
        if daily_used >= VIP_DAILY_ROLL_LIMIT:
            await message.answer("Вы превысили лимит круток, попробуйте завтра.")
            return
    drop_chances = _apply_drop_modifiers(user, drop_chances)
    active_cards = await list_showcase_active_cards(db_pool, tg_user.id)
    extra_card_chance = 0.0
    if active_cards:
        effects = summarize_showcase_effects(active_cards)
        drop_multiplier = float(effects.get("drop_multiplier", 1.0) or 1.0)
        if drop_multiplier != 1.0:
            drop_chances = boost_drop_chances(
                drop_chances, VIP_DROP_BOOST_RARITIES, drop_multiplier
            )
        extra_card_chance = float(effects.get("extra_card_chance", 0.0) or 0.0)
    free_rolls = int(user.get("free_rolls", 0) or 0)
    use_free = free_rolls > 0
    if not use_free:
        cooldown = get_cooldown_seconds(user)
        last_roll = user.get("last_roll_at")
        if isinstance(last_roll, datetime):
            diff = datetime.now(timezone.utc) - last_roll
            if diff.total_seconds() < cooldown:
                left = cooldown - int(diff.total_seconds())
                await message.answer(
                    f"Следующая сосиска через {format_duration(left)}."
                )
                return

    available_by_rarity = filter_existing_cards(cards_by_rarity)
    card = pick_random_card(available_by_rarity, drop_chances)
    if not card:
        await message.answer("Пока нет карточек.")
        return

    item_id = await add_inventory_item_safe(db_pool, tg_user.id, card.file)
    if not item_id:
        cards_logger.error(
            "Roll insert failed. user_id=%s file=%s rarity=%s",
            tg_user.id,
            card.file,
            card.rarity,
        )
        await message.answer("Не удалось сохранить сосиску. Попробуй ещё раз.")
        return
    cards_logger.info(
        "Roll: user_id=%s item_id=%s file=%s rarity=%s",
        tg_user.id,
        item_id,
        card.file,
        card.rarity,
    )
    updates = {"last_roll_at": datetime.now(timezone.utc)}
    if use_free:
        updates["free_rolls"] = max(0, free_rolls - 1)
    if is_vip(user):
        daily_used = int(user.get("rolls_daily_used", 0) or 0) + 1
        updates["rolls_daily_used"] = daily_used
        updates["rolls_daily_date"] = now.date()
    if user.get("referred_by") and not user.get("ref_activated"):
        updates["ref_activated"] = True
    await update_user_fields(db_pool, tg_user.id, updates)

    path = get_card_media_path(card)
    if not path.exists():
        await message.answer("Фото не найдено для этой карточки.")
        return
    with path.open("rb") as photo:
        await send_or_edit_media(
            message,
            photo,
            build_draw_caption(get_user_label(tg_user), card),
            build_draw_keyboard(item_id),
            prefer_edit=False,
            rate_limiter=rate_limiter,
            owner_id=tg_user.id,
        )
    if extra_card_chance > 0 and random.random() < extra_card_chance:
        bonus_card = pick_random_card(available_by_rarity, drop_chances)
        if bonus_card:
            bonus_id = await add_inventory_item_safe(db_pool, tg_user.id, bonus_card.file)
            if bonus_id:
                await message.answer(
                    f"Бонусная сосиска: {format_card_label(bonus_card)}"
                )


@router.callback_query(F.data == "cmd|sosiska")
async def sosiska_callback(
    query: CallbackQuery,
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
    drop_chances: Dict[str, float],
    rate_limiter,
) -> None:
    if not query.message:
        return
    await sosiska_command(
        query.message,
        db_pool=db_pool,
        cards_by_rarity=cards_by_rarity,
        drop_chances=drop_chances,
        rate_limiter=rate_limiter,
        tg_user=query.from_user,
    )
    await query.answer()


@router.message(Command("kazik"))
async def kazik_command(
    message: Message, db_pool, rate_limiter, *, tg_user=None, prefer_edit: bool = False
) -> None:
    await _send_kazik_notice(message)


@router.callback_query(F.data == "cmd|kazik")
async def kazik_callback(query: CallbackQuery, db_pool, rate_limiter) -> None:
    if not query.message:
        return
    await _send_kazik_notice(query.message)
    await query.answer()


@router.callback_query(F.data == "kazik_buy_menu")
async def kazik_buy_menu_callback(
    query: CallbackQuery,
    db_pool,
    rate_limiter,
) -> None:
    if not query.message:
        return
    await _send_kazik_notice(query.message)
    await query.answer()
    return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    now = now_local()
    lines = _kazik_menu_lines(user, now)
    lines.append("")
    lines.append("Выберите пакет спинов:")
    image_path = get_cached_kazik_title_image()
    with image_path.open("rb") as photo:
        await send_or_edit_media(
            query.message,
            photo,
            "\n".join(lines),
            build_kazik_buy_keyboard(),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=query.from_user.id,
        )
    await query.answer()


@router.callback_query(F.data.startswith("kazik_buy|"))
async def kazik_buy_callback(
    query: CallbackQuery,
    db_pool,
    rate_limiter,
) -> None:
    if not query.message:
        return
    await _send_kazik_notice(query.message)
    await query.answer()
    return
    parts = query.data.split("|")
    if len(parts) != 3:
        return
    try:
        spins = int(parts[1])
        cost = int(parts[2])
    except ValueError:
        return
    if spins <= 0 or cost <= 0:
        return
    user = await get_user(db_pool, query.from_user.id)
    if not user:
        return
    stars = int(user.get("stars", 0) or 0)
    if stars < cost:
        await query.answer("Недостаточно ⭐.", show_alert=True)
        return
    bonus_spins = int(user.get("kazik_bonus_spins", 0) or 0)
    await update_user_fields(
        db_pool,
        query.from_user.id,
        {"stars": stars - cost, "kazik_bonus_spins": bonus_spins + spins},
    )
    kazik_logger.info(
        "Kazik buy spins. user_id=%s spins=%s cost=%s stars_before=%s stars_after=%s",
        query.from_user.id,
        spins,
        cost,
        stars,
        stars - cost,
    )
    user = await get_user(db_pool, query.from_user.id)
    now = now_local()
    lines = _kazik_menu_lines(user, now)
    lines.append(f"Добавлено: +{spins} спинов за {cost}⭐")
    image_path = get_cached_kazik_title_image()
    with image_path.open("rb") as photo:
        await send_or_edit_media(
            query.message,
            photo,
            "\n".join(lines),
            build_kazik_spin_keyboard(kazik_spin_button_label(user)),
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=query.from_user.id,
        )
    await query.answer()


@router.callback_query(F.data == "kazik_spin")
async def kazik_spin(query: CallbackQuery, db_pool, cards_by_rarity, rate_limiter) -> None:
    message = query.message
    tg_user = query.from_user
    if not message or not tg_user:
        return
    await _send_kazik_notice(message)
    await query.answer()
    return

    now = now_local()
    updates = {}
    if kazik_should_reset(user, now):
        updates["kazik_daily_used"] = 0
        user["kazik_daily_used"] = 0
        updates["kazik_reset_started_at"] = None
        user["kazik_reset_started_at"] = None

    bonus_spins = int(user.get("kazik_bonus_spins", 0) or 0)
    daily_used = int(user.get("kazik_daily_used", 0) or 0)
    daily_limit = kazik_free_spins_limit(user)

    spent_free = False
    if bonus_spins > 0:
        bonus_spins -= 1
        updates["kazik_bonus_spins"] = bonus_spins
        spent_free = True
    elif daily_used < daily_limit:
        daily_used += 1
        updates["kazik_daily_used"] = daily_used
        spent_free = True
    if spent_free:
        if is_vip(user):
            if not user.get("kazik_reset_started_at"):
                updates["kazik_reset_started_at"] = now
                user["kazik_reset_started_at"] = now
        else:
            updates["kazik_reset_started_at"] = now
            user["kazik_reset_started_at"] = now
    if not spent_free:
        stars = int(user.get("stars", 0) or 0)
        if stars < KAZIK_STAR_SPIN_COST:
            await _send_stars_menu(
                message,
                db_pool,
                user,
                rate_limiter=rate_limiter,
                prefer_edit=True,
                owner_id=tg_user.id,
            )
            await query.answer()
            return
        updates["stars"] = stars - KAZIK_STAR_SPIN_COST
        paid_counter = int(user.get("kazik_paid_counter", 0) or 0) + 1
        if paid_counter >= KAZIK_PAID_SPINS_FOR_BONUS:
            batches = paid_counter // KAZIK_PAID_SPINS_FOR_BONUS
            paid_counter = paid_counter % KAZIK_PAID_SPINS_FOR_BONUS
            bonus_spins += batches * KAZIK_BONUS_SPINS_PER_BATCH
            updates["kazik_bonus_spins"] = bonus_spins
        updates["kazik_paid_counter"] = paid_counter

    win_chance = _kazik_win_chance(user, now)
    no_win_streak = int(user.get("kazik_no_win_streak", 0) or 0)
    force_win = KAZIK_GUARANTEE_SPINS > 0 and no_win_streak >= KAZIK_GUARANTEE_SPINS - 1
    digits = roll_kazik_digits(win_chance=1.0 if force_win else win_chance)
    win_digit = digits[0] if digits[0] == digits[1] == digits[2] else None

    reward_card = None
    reward_saved = True
    if win_digit is not None:
        reward_card = pick_kazik_reward_card(cards_by_rarity, win_digit)
        if reward_card:
            item_id = await add_inventory_item_safe(
                db_pool, tg_user.id, reward_card.file
            )
            if not item_id:
                reward_saved = False
                cards_logger.error(
                    "Kazik win insert failed. user_id=%s file=%s rarity=%s",
                    tg_user.id,
                    reward_card.file,
                    reward_card.rarity,
                )
            else:
                cards_logger.info(
                    "Kazik win card. user_id=%s item_id=%s file=%s rarity=%s",
                    tg_user.id,
                    item_id,
                    reward_card.file,
                    reward_card.rarity,
                )
        updates["kazik_no_win_streak"] = 0
    else:
        updates["kazik_no_win_streak"] = no_win_streak + 1

    kazik_logger.info(
        "Kazik spin. user_id=%s spent_free=%s win_digit=%s win=%s bonus_spins=%s daily_used=%s stars_delta=%s",
        tg_user.id,
        spent_free,
        win_digit,
        bool(win_digit),
        updates.get("kazik_bonus_spins", bonus_spins),
        updates.get("kazik_daily_used", daily_used),
        -KAZIK_STAR_SPIN_COST if not spent_free else 0,
    )

    if updates:
        await update_user_fields(db_pool, tg_user.id, updates)
        user = {**user, **updates}

    try:
        spin_image = build_kazik_spin_image(digits, 0, title="Крутим...")
        await send_or_edit_media(
            message,
            spin_image,
            "",
            None,
            prefer_edit=True,
            rate_limiter=rate_limiter,
            owner_id=tg_user.id,
        )
        await asyncio.sleep(KAZIK_SPIN_DELAY)
    except Exception:
        pass

    win_text = ""
    if win_digit is not None:
        if reward_card and reward_saved:
            win_text = (
                f"Выигрыш: {card_display_name(reward_card)} "
                f"({RARITY_NAMES.get(reward_card.rarity, reward_card.rarity)})"
            )
        elif reward_card and not reward_saved:
            win_text = "Ошибка сохранения выигрыша. Напиши /support."
        else:
            win_text = "Выигрыш есть, но карточек нет."
    user = await get_user(db_pool, tg_user.id)
    title_text = win_text or "Не повезло"
    result_lines = _kazik_menu_lines(user, now)
    result_image = build_kazik_spin_image(digits, 3, title=title_text)
    await send_or_edit_media(
        message,
        result_image,
        "\n".join(result_lines),
        build_kazik_spin_keyboard(kazik_spin_button_label(user)),
        prefer_edit=True,
        rate_limiter=rate_limiter,
        owner_id=tg_user.id,
    )
    await query.answer()

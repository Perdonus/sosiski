from __future__ import annotations

from datetime import datetime, timezone

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message

from app.images import get_cached_referral_road_image
from app.repo import (
    add_inventory_item_safe,
    adjust_user_free_rolls,
    adjust_user_stars,
    get_or_create_user,
    get_user,
    update_user_fields,
)
from cards import card_display_name, filter_existing_cards, pick_random_card
from config import PUBLIC_BOT_USERNAME
from app.handlers.donate import compute_vip_until
from config import VIP_DURATION_DAYS

router = Router()


async def _count_referrals(db_pool, user_id: int) -> tuple[int, int]:
    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by = $1",
            int(user_id),
        )
        activated = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by = $1 AND ref_activated = TRUE",
            int(user_id),
        )
    return int(total or 0), int(activated or 0)


async def _apply_ref_rewards(
    db_pool,
    user_id: int,
    start_step: int,
    end_step: int,
    cards_by_rarity,
    drop_chances,
) -> list[str]:
    rewards: list[str] = []
    available_by_rarity = filter_existing_cards(cards_by_rarity)
    for step in range(start_step + 1, end_step + 1):
        if step > 15:
            break
        if step == 1:
            card = pick_random_card(available_by_rarity, drop_chances)
            if card:
                item_id = await add_inventory_item_safe(db_pool, user_id, card.file)
                if item_id:
                    rewards.append(f"1: {card_display_name(card)}")
                    continue
            rewards.append("1: сосиска (не удалось выдать)")
            continue
        if step == 5:
            await adjust_user_free_rolls(db_pool, user_id, 30)
            rewards.append("5: +30 фри круток")
            continue
        if step == 10:
            await adjust_user_stars(db_pool, user_id, 10)
            rewards.append("10: +10⭐")
            continue
        if step == 15:
            user = await get_user(db_pool, user_id)
            if user:
                vip_until = compute_vip_until(user, datetime.now(timezone.utc))
                await update_user_fields(
                    db_pool, user_id, {"vip": True, "vip_until": vip_until}
                )
                rewards.append(f"15: VIP {VIP_DURATION_DAYS}д")
            else:
                rewards.append("15: VIP (не удалось выдать)")
            continue
        await adjust_user_free_rolls(db_pool, user_id, 3)
        rewards.append(f"{step}: +3 фри крутки")
    return rewards


@router.message(Command("ref"))
async def ref_command(
    message: Message,
    db_pool,
    cards_by_rarity,
    drop_chances,
) -> None:
    if message.chat.type != "private":
        return
    tg_user = message.from_user
    if not tg_user:
        return
    username = (PUBLIC_BOT_USERNAME or "").lstrip("@")
    if not username:
        await message.answer(
            "Не удалось определить username бота. Укажи PUBLIC_BOT_USERNAME в .env."
        )
        return
    user = await get_user(db_pool, tg_user.id)
    if not user:
        user = await get_or_create_user(
            db_pool, tg_user.id, tg_user.full_name or "", tg_user.username or ""
        )
    total, activated = await _count_referrals(db_pool, tg_user.id)
    claimed = int(user.get("ref_reward_count", 0) or 0)
    eligible = min(activated, 15)
    if eligible > claimed:
        await _apply_ref_rewards(
            db_pool, tg_user.id, claimed, eligible, cards_by_rarity, drop_chances
        )
        await update_user_fields(db_pool, tg_user.id, {"ref_reward_count": eligible})
    link = f"https://t.me/{username}?start=ref_{message.from_user.id}"
    image_path = get_cached_referral_road_image(eligible)
    caption_lines = [
        "Твоя реферальная ссылка:",
        link,
        "",
        f"Всего перешло: {total}",
        f"Активировано: {activated}",
    ]
    await message.answer_photo(
        photo=FSInputFile(str(image_path)),
        caption="\n".join(caption_lines),
    )

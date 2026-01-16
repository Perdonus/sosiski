from __future__ import annotations

from typing import Dict, List

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from app.discounts import ensure_discounts, is_discount_active
from app.giveaway import (
    ensure_giveaway,
    format_giveaway_prize,
    format_prize_label,
    get_giveaway_schedule,
)
from app.utils import now_local
from cards import Card, card_display_name
from config import ADMIN_BROADCAST_USER_ID

router = Router()


def _is_admin(message: Message) -> bool:
    if not message.from_user:
        return False
    return message.from_user.id == int(ADMIN_BROADCAST_USER_ID)


def _extract_first_prize(item: Dict[str, object], card_map: Dict[str, Card]) -> str:
    prizes = item.get("prizes", {})
    prize = None
    if isinstance(prizes, dict):
        prize = prizes.get("1") or prizes.get(1)
    if not isinstance(prize, dict):
        return "не задан"
    return format_prize_label(prize, card_map)


def _format_upcoming_giveaways(
    items: List[Dict[str, object]],
    card_map: Dict[str, Card],
) -> List[str]:
    today_key = now_local().date().isoformat()
    upcoming = [
        item for item in items if str(item.get("date", "")) >= str(today_key)
    ]
    if not upcoming:
        return ["Предстоящие розыгрыши: нет"]
    lines = ["Предстоящие розыгрыши:"]
    for item in sorted(upcoming, key=lambda entry: str(entry.get("date", "")))[:8]:
        date_key = str(item.get("date", ""))
        label = _extract_first_prize(item, card_map)
        lines.append(f"- {date_key}: {label}")
    return lines


@router.message(Command("admin"))
async def admin_status_command(
    message: Message,
    db_pool,
    cards_by_rarity,
    card_map,
) -> None:
    if not _is_admin(message):
        return
    discounts = await ensure_discounts(db_pool, cards_by_rarity)
    items = discounts.get("items", [])
    if not isinstance(items, list):
        items = []
    active = [item for item in items if is_discount_active(item)]
    lines = [
        f"Акции ({discounts.get('date', '?')}): {len(active)}/{len(items)} активны"
    ]
    for item in active[:6]:
        file_name = str(item.get("file", ""))
        card = card_map.get(file_name)
        name = card_display_name(card) if card else file_name
        percent = int(item.get("percent", 0))
        remaining = int(item.get("remaining", 0))
        lines.append(f"- {name}: -{percent}% (осталось {remaining})")

    giveaway = await ensure_giveaway(db_pool)
    if giveaway:
        entries = giveaway.get("entries", [])
        winners = giveaway.get("winners", {})
        prizes = giveaway.get("prizes", {})
        lines.extend(
            [
                "",
                f"Розыгрыш {giveaway.get('date', '?')} [{giveaway.get('status', '?')}]",
                f"Приз (1 место): {format_giveaway_prize(giveaway, card_map)}",
                f"Призовых мест: {len(prizes) if isinstance(prizes, dict) else 0}",
                f"Участников: {len(entries) if isinstance(entries, list) else 0}, "
                f"победителей: {len(winners) if isinstance(winners, dict) else 0}",
                "",
            ]
        )
    else:
        lines.extend(["", "Розыгрыш: нет активного.", ""])

    schedule_items = await get_giveaway_schedule(db_pool)
    lines.extend(_format_upcoming_giveaways(schedule_items, card_map))
    lines.extend(
        [
            "",
            "Команды: /giveaway_list, /giveaway_new, /giveaway_edit, /giveaway_delete, /giveaway_say, /bd",
        ]
    )
    await message.answer("\n".join(line for line in lines if line != ""))


@router.message(Command("giveaway_list"))
async def giveaway_list_command(
    message: Message,
    db_pool,
    card_map,
) -> None:
    if not _is_admin(message):
        return
    schedule_items = await get_giveaway_schedule(db_pool)
    lines = _format_upcoming_giveaways(schedule_items, card_map)
    await message.answer("\n".join(lines))

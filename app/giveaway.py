from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from aiogram import Bot

from cards import Card, card_display_name, card_currency
from config import (
    GIVEAWAY_ANNOUNCE_HOUR,
    GIVEAWAY_MIN_RARITY,
    GIVEAWAY_SIGNUP_END_HOUR,
    GIVEAWAY_START_HOUR,
    EXCLUSIVE_STOCK_LIMIT,
    RARITY_NAMES,
    RARITY_ORDER,
)
from app.repo import (
    add_inventory_item_safe,
    adjust_user_balance,
    adjust_user_free_rolls,
    get_kv,
    get_user,
    set_kv,
    sync_exclusive_stock,
    update_exclusive_reserved,
    update_user_fields,
)
from app.utils import format_short_amount, now_local

GIVEAWAY_KV_KEY = "giveaway"
GIVEAWAY_SCHEDULE_KV_KEY = "giveaway_schedule"

giveaway_logger = logging.getLogger("giveaway")


def giveaway_day_key(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    day = now.date()
    if now.hour < GIVEAWAY_START_HOUR:
        day = day - timedelta(days=1)
    return day.isoformat()


def giveaway_phase(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    hour = now.hour
    if hour < GIVEAWAY_START_HOUR:
        return "idle"
    if hour < GIVEAWAY_SIGNUP_END_HOUR:
        return "open"
    if hour < GIVEAWAY_ANNOUNCE_HOUR:
        return "closed"
    return "announce"


def _extract_schedule_for_date(
    items: List[Dict[str, object]],
    date_key: str,
) -> Tuple[Optional[Dict[str, object]], List[Dict[str, object]]]:
    match = None
    rest: List[Dict[str, object]] = []
    for item in items:
        if match is None and item.get("date") == date_key:
            match = item
        else:
            rest.append(item)
    return match, rest


def pick_giveaway_card(cards_by_rarity: Dict[str, List[Card]]) -> Optional[Card]:
    if GIVEAWAY_MIN_RARITY in RARITY_ORDER:
        min_index = RARITY_ORDER.index(GIVEAWAY_MIN_RARITY)
    else:
        min_index = RARITY_ORDER.index("epic")
    pool: List[Card] = []
    for rarity in RARITY_ORDER[min_index:]:
        if rarity == "exclusive":
            continue
        pool.extend(cards_by_rarity.get(rarity, []))
    if pool:
        return random.choice(pool)
    fallback: List[Card] = []
    for rarity in RARITY_ORDER:
        if rarity == "exclusive":
            continue
        fallback.extend(cards_by_rarity.get(rarity, []))
    return random.choice(fallback) if fallback else None


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


def _normalize_schedule_items(value: object) -> List[Dict[str, object]]:
    if not isinstance(value, list):
        return []
    items: List[Dict[str, object]] = []
    for raw_item in value:
        if not isinstance(raw_item, dict):
            continue
        date_key = str(raw_item.get("date", "")).strip()
        if not date_key:
            continue
        entry = dict(raw_item)
        entry["date"] = date_key
        entry["prizes"] = _normalize_prizes(entry.get("prizes"))
        items.append(entry)
    return items


def build_giveaway(
    date_key: str,
    prizes: Dict[str, Dict[str, object]],
    *,
    scheduled_by: Optional[int] = None,
    scheduled_at: Optional[str] = None,
) -> Dict[str, object]:
    now = now_local()
    data: Dict[str, object] = {
        "date": date_key,
        "created_at": now.isoformat(),
        "status": "open",
        "entries": [],
        "winners": {},
        "prizes": _normalize_prizes(prizes),
    }
    if scheduled_by is not None:
        data["scheduled_by"] = scheduled_by
    if scheduled_at:
        data["scheduled_at"] = scheduled_at
    return data


async def get_giveaway_schedule(db_pool) -> List[Dict[str, object]]:
    schedule = await get_kv(db_pool, GIVEAWAY_SCHEDULE_KV_KEY) or {}
    return _normalize_schedule_items(schedule.get("items"))


async def set_giveaway_schedule(db_pool, items: List[Dict[str, object]]) -> None:
    await set_kv(db_pool, GIVEAWAY_SCHEDULE_KV_KEY, {"items": items})


async def schedule_giveaway(
    db_pool,
    date_key: str,
    prizes: Dict[str, Dict[str, object]],
    created_by: Optional[int],
) -> Dict[str, object]:
    items = await get_giveaway_schedule(db_pool)
    items = [item for item in items if item.get("date") != date_key]
    entry: Dict[str, object] = {
        "date": date_key,
        "created_at": now_local().isoformat(),
        "created_by": int(created_by) if created_by is not None else None,
        "prizes": _normalize_prizes(prizes),
    }
    items.append(entry)
    items.sort(key=lambda item: item.get("date", ""))
    await set_giveaway_schedule(db_pool, items)
    giveaway_logger.info(
        "Scheduled giveaway date=%s prizes=%s created_by=%s",
        date_key,
        len(entry.get("prizes", {})),
        created_by,
    )
    return entry


async def ensure_giveaway(db_pool) -> Optional[Dict[str, object]]:
    giveaway = await get_kv(db_pool, GIVEAWAY_KV_KEY) or {}
    today = giveaway_day_key()
    if giveaway.get("date") == today:
        giveaway["prizes"] = _normalize_prizes(giveaway.get("prizes"))
        return giveaway

    scheduled = None
    schedule_items = await get_giveaway_schedule(db_pool)
    if schedule_items:
        scheduled, schedule_items = _extract_schedule_for_date(schedule_items, today)
    if not scheduled:
        return None
    await set_giveaway_schedule(db_pool, schedule_items)
    prizes = _normalize_prizes(scheduled.get("prizes"))
    giveaway = build_giveaway(
        today,
        prizes,
        scheduled_by=scheduled.get("created_by"),
        scheduled_at=scheduled.get("created_at"),
    )
    await set_kv(db_pool, GIVEAWAY_KV_KEY, giveaway)
    giveaway_logger.info(
        "Activated giveaway date=%s prizes=%s",
        today,
        len(prizes),
    )
    return giveaway


def format_giveaway_prize(
    giveaway: Dict[str, object],
    card_map: Dict[str, Card],
) -> str:
    prizes = _normalize_prizes(giveaway.get("prizes"))
    prize = prizes.get("1")
    if not prize:
        return "приз"
    return format_prize_label(prize, card_map)


def format_prize_label(prize: Dict[str, object], card_map: Dict[str, Card]) -> str:
    prize_type = str(prize.get("type", ""))
    if prize_type == "balance":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount}р"
    if prize_type == "free_rolls":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount} фри круток"
    if prize_type == "vip":
        days = int(prize.get("days", 0) or 0)
        return f"VIP на {days}д"
    if prize_type == "card":
        file_name = str(prize.get("file", ""))
        card = card_map.get(file_name) if file_name else None
        rarity = prize.get("rarity") or (card.rarity if card else None)
        label = card_display_name(card) if card else file_name or "сосиску"
        if rarity:
            rarity_label = RARITY_NAMES.get(str(rarity), rarity)
            return f"{label} ({rarity_label})"
        return label
    return "приз"


async def add_giveaway_entry(
    db_pool,
    user_id: int,
    giveaway: Dict[str, object],
) -> Tuple[bool, Dict[str, object]]:
    entries = giveaway.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    uid = str(user_id)
    added = uid not in entries
    if added:
        entries.append(uid)
        giveaway["entries"] = entries
        giveaway["status"] = "open"
        await set_kv(db_pool, GIVEAWAY_KV_KEY, giveaway)
    return added, giveaway


def _sorted_prize_places(prizes: Dict[str, Dict[str, object]]) -> List[str]:
    places = []
    for key in prizes.keys():
        try:
            places.append(int(key))
        except (TypeError, ValueError):
            continue
    return [str(place) for place in sorted(places)]


def _format_prize_message(prize: Dict[str, object], card_map: Dict[str, Card]) -> str:
    prize_type = str(prize.get("type", ""))
    if prize_type == "balance":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount}р на баланс"
    if prize_type == "free_rolls":
        amount = int(prize.get("amount", 0) or 0)
        return f"{amount} фри круток"
    if prize_type == "vip":
        days = int(prize.get("days", 0) or 0)
        return f"VIP на {days} дней"
    if prize_type == "card":
        file_name = str(prize.get("file", ""))
        card = card_map.get(file_name) if file_name else None
        rarity = prize.get("rarity") or (card.rarity if card else None)
        price_text = ""
        if card and card.price is not None:
            price_text = format_short_amount(card.price, card_currency(card))
        label = card_display_name(card) if card else file_name or "сосиску"
        if rarity:
            rarity_label = RARITY_NAMES.get(str(rarity), rarity)
            if price_text:
                return f"{label} ({rarity_label}, {price_text})"
            return f"{label} ({rarity_label})"
        return label
    return "приз"


def _exclusive_prize_counts(prizes: Dict[str, Dict[str, object]]) -> Dict[str, int]:
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


async def announce_giveaway(
    db_pool,
    bot: Bot,
    giveaway: Dict[str, object],
    card_map: Dict[str, Card],
) -> None:
    if giveaway.get("status") == "announced":
        return
    entries = giveaway.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    unique_entries = list(dict.fromkeys(str(uid) for uid in entries))
    prizes = _normalize_prizes(giveaway.get("prizes"))
    places = _sorted_prize_places(prizes)
    if not unique_entries:
        giveaway["status"] = "announced"
        giveaway["announced_at"] = now_local().isoformat()
        await set_kv(db_pool, GIVEAWAY_KV_KEY, giveaway)
        giveaway_logger.info("Giveaway announced with no entries date=%s", giveaway.get("date"))
        await _release_exclusive_reserve(db_pool, prizes)
        return

    winners_count = min(len(unique_entries), len(places))
    winners_list = random.sample(unique_entries, winners_count)
    winners = {
        str(place): uid for place, uid in zip(places[:winners_count], winners_list)
    }
    giveaway["winners"] = winners
    giveaway["status"] = "announced"
    giveaway["announced_at"] = now_local().isoformat()
    await set_kv(db_pool, GIVEAWAY_KV_KEY, giveaway)
    giveaway_logger.info(
        "Giveaway announced date=%s winners=%s",
        giveaway.get("date"),
        winners,
    )

    for place in sorted(winners, key=lambda value: int(value)):
        uid = winners[place]
        prize = prizes.get(place)
        if not prize:
            continue
        prize_type = str(prize.get("type", ""))
        if prize_type == "balance":
            await adjust_user_balance(
                db_pool, int(uid), int(prize.get("amount", 0) or 0)
            )
        elif prize_type == "free_rolls":
            await adjust_user_free_rolls(
                db_pool, int(uid), int(prize.get("amount", 0) or 0)
            )
        elif prize_type == "vip":
            days = int(prize.get("days", 0) or 0)
            if days > 0:
                await _apply_vip_reward(db_pool, int(uid), days)
        elif prize_type == "card":
            file_name = str(prize.get("file", ""))
            if file_name:
                item_id = await add_inventory_item_safe(db_pool, int(uid), file_name)
                if not item_id:
                    giveaway_logger.error(
                        "Giveaway card insert failed. date=%s user_id=%s file=%s",
                        giveaway.get("date"),
                        uid,
                        file_name,
                    )
                    try:
                        await bot.send_message(
                            chat_id=int(uid),
                            text="Ошибка выдачи призовой сосиски. Напиши /support.",
                        )
                    except Exception:
                        pass

        giveaway_logger.info(
            "Giveaway reward. date=%s user_id=%s place=%s prize=%s",
            giveaway.get("date"),
            uid,
            place,
            prize,
        )
        try:
            place_label = f"{place} место"
            prize_text = _format_prize_message(prize, card_map)
            await bot.send_message(
                chat_id=int(uid),
                text=f"Поздравляем! {place_label}: {prize_text}.",
            )
        except Exception:
            continue

    await _release_exclusive_reserve(db_pool, prizes)


async def _apply_vip_reward(db_pool, user_id: int, days: int) -> None:
    user = await get_user(db_pool, user_id)
    now = now_local()
    vip_until = user.get("vip_until")
    base = now
    if isinstance(vip_until, datetime) and vip_until > now:
        base = vip_until
    new_until = base + timedelta(days=days)
    await update_user_fields(
        db_pool,
        user_id,
        {"vip_until": new_until, "vip": True},
    )


async def _release_exclusive_reserve(
    db_pool,
    prizes: Dict[str, Dict[str, object]],
) -> None:
    counts = _exclusive_prize_counts(prizes)
    if not counts:
        return
    updates = {file_name: -count for file_name, count in counts.items()}
    await update_exclusive_reserved(db_pool, updates)
    await sync_exclusive_stock(db_pool, counts.keys(), EXCLUSIVE_STOCK_LIMIT)

from __future__ import annotations

import random
from typing import Dict, List, Optional

from cards import Card
from config import (
    DISCOUNT_ITEMS_PER_DAY,
    DISCOUNT_PERCENT_MAX,
    DISCOUNT_PERCENT_MIN,
    DISCOUNT_QUANTITY_BY_RARITY,
    DISCOUNT_RARITY_WEIGHTS,
)
from app.repo import get_kv, set_kv
from app.utils import now_local

DISCOUNT_KV_KEY = "discounts"


def discount_day_key() -> str:
    return now_local().date().isoformat()


def build_discount_index(discounts: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    items = discounts.get("items", [])
    if not isinstance(items, list):
        return {}
    return {str(item.get("file")): item for item in items if item.get("file")}


def get_discount_item(
    discounts: Dict[str, object], card_file: str
) -> Optional[Dict[str, object]]:
    index = build_discount_index(discounts)
    return index.get(card_file)


def is_discount_active(item: Optional[Dict[str, object]]) -> bool:
    if not item:
        return False
    try:
        return int(item.get("remaining", 0)) > 0
    except (TypeError, ValueError):
        return False


def pick_weighted_cards(
    cards_by_rarity: Dict[str, List[Card]], count: int
) -> List[Card]:
    weighted_pool = []
    for rarity, cards in cards_by_rarity.items():
        if rarity == "exclusive":
            continue
        weight = float(DISCOUNT_RARITY_WEIGHTS.get(rarity, 0))
        if weight <= 0:
            continue
        for card in cards:
            if card.price is None:
                continue
            weighted_pool.append((card, weight))
    selected = []
    pool = weighted_pool[:]
    for _ in range(min(count, len(pool))):
        total_weight = sum(weight for _, weight in pool)
        if total_weight <= 0:
            break
        pick = random.random() * total_weight
        cumulative = 0.0
        chosen_index = 0
        for index, (_, weight) in enumerate(pool):
            cumulative += weight
            if pick <= cumulative:
                chosen_index = index
                break
        card, _ = pool.pop(chosen_index)
        selected.append(card)
    return selected


def generate_discounts(cards_by_rarity: Dict[str, List[Card]]) -> Dict[str, object]:
    day_key = discount_day_key()
    items = []
    percent_min = min(DISCOUNT_PERCENT_MIN, DISCOUNT_PERCENT_MAX)
    percent_max = max(DISCOUNT_PERCENT_MIN, DISCOUNT_PERCENT_MAX)
    for card in pick_weighted_cards(cards_by_rarity, DISCOUNT_ITEMS_PER_DAY):
        percent = random.randint(percent_min, percent_max)
        original_price = int(card.price or 0)
        discount_price = int(round(original_price * (100 - percent) / 100))
        if discount_price >= original_price:
            discount_price = max(1, original_price - 1)
        quantity = int(DISCOUNT_QUANTITY_BY_RARITY.get(card.rarity, 0))
        items.append(
            {
                "file": card.file,
                "rarity": card.rarity,
                "percent": percent,
                "original_price": original_price,
                "discount_price": discount_price,
                "remaining": quantity,
                "initial": quantity,
            }
        )
    return {
        "date": day_key,
        "generated_at": now_local().isoformat(),
        "items": items,
    }


async def ensure_discounts(
    db_pool,
    cards_by_rarity: Dict[str, List[Card]],
) -> Dict[str, object]:
    discounts = await get_kv(db_pool, DISCOUNT_KV_KEY) or {}
    today = discount_day_key()
    if discounts.get("date") != today:
        discounts = generate_discounts(cards_by_rarity)
        await set_kv(db_pool, DISCOUNT_KV_KEY, discounts)
    return discounts


async def consume_discount(
    db_pool,
    card_file: str,
) -> Optional[Dict[str, object]]:
    discounts = await get_kv(db_pool, DISCOUNT_KV_KEY) or {}
    items = discounts.get("items", [])
    if not isinstance(items, list):
        return None
    updated_item = None
    for item in items:
        if item.get("file") != card_file:
            continue
        remaining = int(item.get("remaining", 0))
        if remaining <= 0:
            break
        item["remaining"] = remaining - 1
        updated_item = item
        break
    if updated_item:
        await set_kv(db_pool, DISCOUNT_KV_KEY, discounts)
    return updated_item


async def restore_discount(
    db_pool,
    card_file: str,
) -> Optional[Dict[str, object]]:
    discounts = await get_kv(db_pool, DISCOUNT_KV_KEY) or {}
    items = discounts.get("items", [])
    if not isinstance(items, list):
        return None
    updated_item = None
    for item in items:
        if item.get("file") != card_file:
            continue
        remaining = int(item.get("remaining", 0))
        initial = int(item.get("initial", remaining))
        if remaining < initial:
            item["remaining"] = remaining + 1
            updated_item = item
        break
    if updated_item:
        await set_kv(db_pool, DISCOUNT_KV_KEY, discounts)
    return updated_item

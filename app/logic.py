from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple

from cards import Card
from config import ROLL_COOLDOWN_SEC, VIP_ROLL_COOLDOWN_SEC


def is_vip(user: Dict[str, object]) -> bool:
    until = user.get("vip_until")
    if isinstance(until, datetime):
        return until > datetime.now(timezone.utc)
    return bool(user.get("vip"))


def get_cooldown_seconds(user: Dict[str, object]) -> int:
    return VIP_ROLL_COOLDOWN_SEC if is_vip(user) else ROLL_COOLDOWN_SEC


def inventory_value(items: List[Dict[str, object]], card_map: Dict[str, Card]) -> int:
    total = 0
    for item in items:
        filename = item.get("file")
        if not filename:
            continue
        card = card_map.get(str(filename))
        if not card or card.price is None:
            continue
        if card.rarity == "exclusive":
            continue
        total += int(card.price)
    return total


def total_wealth(
    user: Dict[str, object],
    inventory_total: int,
) -> int:
    balance = int(user.get("balance", 0) or 0)
    return inventory_total + balance


def compute_rank(
    totals: List[Tuple[int, int]],
    user_id: int,
) -> Tuple[int, int]:
    totals_sorted = sorted(totals, key=lambda item: (-item[1], item[0]))
    rank = 1
    for index, (uid, _) in enumerate(totals_sorted, start=1):
        if uid == user_id:
            rank = index
            break
    return rank, len(totals_sorted)


def compute_leaderboard(
    users: List[Dict[str, object]],
    inventory_map: Dict[int, List[Dict[str, object]]],
    card_map: Dict[str, Card],
    limit: int,
) -> Tuple[List[Tuple[int, str, int, bool]], int]:
    leaderboard: List[Tuple[int, str, int, bool]] = []
    for user in users:
        uid = int(user.get("user_id", 0))
        name = str(user.get("username") or "Без имени").strip() or "Без имени"
        items = inventory_map.get(uid, [])
        total_val = inventory_value(items, card_map)
        total = total_wealth(user, total_val)
        vip = is_vip(user)
        leaderboard.append((uid, name, total, vip))
    leaderboard.sort(key=lambda item: (-item[2], item[1].lower(), item[0]))
    return leaderboard[:limit], len(leaderboard)

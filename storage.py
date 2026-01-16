import json
import secrets
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from cards import Card
from config import (
    DB_PATH,
    KAZIK_SPIN_COST,
    KAZIK_WIN_CHANCE,
    ROLL_COOLDOWN_SEC,
    VIP_KAZIK_SPIN_COST,
    VIP_KAZIK_WIN_CHANCE,
    VIP_ROLL_COOLDOWN_SEC,
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def load_db() -> Dict[str, object]:
    if not DB_PATH.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        DB_PATH.write_text(
            json.dumps({"meta": {"version": 1}, "users": {}, "trades": {}}, indent=2),
            encoding="utf-8",
        )
    db = json.loads(DB_PATH.read_text(encoding="utf-8"))
    if not isinstance(db, dict):
        db = {"meta": {"version": 1}, "users": {}, "trades": {}}
    db.setdefault("meta", {"version": 1})
    db.setdefault("users", {})
    db.setdefault("trades", {})
    db.setdefault("exclusive_stock", {})
    return db


def save_db(db: Dict[str, object]) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(db, ensure_ascii=False, indent=2)
    tmp_path = DB_PATH.with_suffix(DB_PATH.suffix + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(DB_PATH)


def get_user_label(tg_user) -> str:
    if tg_user.username:
        return f"@{tg_user.username}"
    if tg_user.full_name:
        return str(tg_user.full_name)
    return str(tg_user.id)


def normalize_user_tag(tag: str) -> str:
    return tag.strip().lstrip("@").lower()


def find_user_by_tag(
    db: Dict[str, object], tag: str
) -> Optional[Tuple[str, Dict[str, object]]]:
    normalized = normalize_user_tag(tag)
    if not normalized:
        return None
    for uid, data in db.get("users", {}).items():
        user_tag = str(data.get("user_tag") or "").lstrip("@").lower()
        if user_tag and user_tag == normalized:
            return uid, data
    return None


def ensure_user(db: Dict[str, object], tg_user) -> Dict[str, object]:
    users = db.setdefault("users", {})
    user_id = str(tg_user.id)
    if user_id not in users:
        users[user_id] = {
            "username": tg_user.full_name,
            "user_tag": tg_user.username or "",
            "balance": 0,
            "stars": 0,
            "inventory": [],
            "last_roll_at": None,
            "last_kazik_at": None,
            "kazik_session": None,
            "kazik_daily_date": None,
            "kazik_daily_used": 0,
            "kazik_bonus_spins": 0,
            "star_roll_daily_date": None,
            "star_roll_daily_used": 0,
            "referred_by": None,
            "vip": False,
            "vip_until": None,
            "vip_reward_pending": False,
            "free_rolls": 0,
            "last_vip_star_spin_at": None,
            "last_reminder_at": None,
            "input_mode": None,
        }
    user = users[user_id]
    user.setdefault("balance", 0)
    user.setdefault("stars", 0)
    user.setdefault("inventory", [])
    user.setdefault("last_roll_at", None)
    user.setdefault("last_kazik_at", None)
    user.setdefault("kazik_session", None)
    user.setdefault("kazik_daily_date", None)
    user.setdefault("kazik_daily_used", 0)
    user.setdefault("kazik_bonus_spins", 0)
    user.setdefault("star_roll_daily_date", None)
    user.setdefault("star_roll_daily_used", 0)
    user.setdefault("referred_by", None)
    user.setdefault("vip", False)
    user.setdefault("vip_until", None)
    user.setdefault("vip_reward_pending", False)
    user.setdefault("free_rolls", 0)
    user.setdefault("last_vip_star_spin_at", None)
    user.setdefault("last_reminder_at", None)
    user.setdefault("input_mode", None)
    user["username"] = str(tg_user.full_name or "")
    if tg_user.username:
        user["user_tag"] = tg_user.username
    return user


def inventory_value(user: Dict[str, object], card_map: Dict[str, Card]) -> int:
    total = 0
    for item in user.get("inventory", []):
        filename = item.get("file")
        if not filename:
            continue
        card = card_map.get(filename)
        if not card or card.price is None:
            continue
        if card.rarity == "exclusive":
            continue
        total += card.price
    return total


def get_balance(user: Dict[str, object]) -> int:
    try:
        return int(user.get("balance", 0))
    except (TypeError, ValueError):
        return 0


def get_star_balance(user: Dict[str, object]) -> int:
    try:
        return int(user.get("stars", 0))
    except (TypeError, ValueError):
        return 0


def is_vip(user: Dict[str, object]) -> bool:
    until = parse_iso(user.get("vip_until"))
    if until:
        return until > now_utc()
    return bool(user.get("vip"))


def get_kazik_spin_cost(user: Dict[str, object]) -> int:
    return VIP_KAZIK_SPIN_COST if is_vip(user) else KAZIK_SPIN_COST


def get_kazik_win_chance(user: Dict[str, object]) -> float:
    return VIP_KAZIK_WIN_CHANCE if is_vip(user) else KAZIK_WIN_CHANCE


def get_cooldown_seconds(user: Dict[str, object]) -> int:
    return VIP_ROLL_COOLDOWN_SEC if is_vip(user) else ROLL_COOLDOWN_SEC


def total_wealth(user: Dict[str, object], card_map: Dict[str, Card]) -> int:
    return inventory_value(user, card_map) + get_balance(user)


def compute_rank(
    db: Dict[str, object],
    card_map: Dict[str, Card],
    user_id: str,
) -> Tuple[int, int]:
    users = db.get("users", {})
    totals = []
    for uid, data in users.items():
        totals.append((uid, total_wealth(data, card_map)))
    totals.sort(key=lambda item: item[1], reverse=True)
    rank = 1
    for index, (uid, _) in enumerate(totals, start=1):
        if uid == user_id:
            rank = index
            break
    return rank, max(1, len(totals))


def compute_leaderboard(
    db: Dict[str, object],
    card_map: Dict[str, Card],
    limit: int,
) -> Tuple[List[Tuple[str, str, int, bool]], int]:
    users = db.get("users", {})
    leaderboard = []
    for uid, data in users.items():
        raw_name = data.get("username")
        name = str(raw_name).strip() if raw_name else "Без имени"
        if not name:
            name = "Без имени"
        total = total_wealth(data, card_map)
        vip = is_vip(data)
        leaderboard.append((uid, name, total, vip))
    leaderboard.sort(key=lambda item: (-item[2], item[1].lower(), item[0]))
    trimmed = [(uid, name, total, vip) for uid, name, total, vip in leaderboard[:limit]]
    return trimmed, len(leaderboard)


def make_inventory_item(filename: str) -> Dict[str, object]:
    return {"id": f"it_{secrets.token_urlsafe(6)}", "file": filename}


def find_inventory_item(
    user: Dict[str, object], item_id: str
) -> Optional[Dict[str, object]]:
    for item in user.get("inventory", []):
        if item.get("id") == item_id:
            return item
    return None


def sync_exclusive_stock(
    db: Dict[str, object], card_map: Dict[str, Card], limit: int
) -> bool:
    stock = db.setdefault("exclusive_stock", {})
    owned_counts: Dict[str, int] = {}
    for user in db.get("users", {}).values():
        if not isinstance(user, dict):
            continue
        for item in user.get("inventory", []):
            filename = item.get("file")
            if not filename:
                continue
            card = card_map.get(filename)
            if card and card.rarity == "exclusive":
                owned_counts[filename] = owned_counts.get(filename, 0) + 1
    changed = False
    for filename, card in card_map.items():
        if card.rarity != "exclusive":
            continue
        owned = owned_counts.get(filename, 0)
        remaining = max(0, limit - owned)
        record = stock.get(filename)
        if not isinstance(record, dict):
            stock[filename] = {"total": limit, "remaining": remaining}
            changed = True
            continue
        if int(record.get("total", limit)) != limit or int(
            record.get("remaining", remaining)
        ) != remaining:
            record["total"] = limit
            record["remaining"] = remaining
            stock[filename] = record
            changed = True
    return changed


__all__ = [
    "now_utc",
    "parse_iso",
    "load_db",
    "save_db",
    "get_user_label",
    "normalize_user_tag",
    "find_user_by_tag",
    "ensure_user",
    "inventory_value",
    "get_balance",
    "get_star_balance",
    "is_vip",
    "get_kazik_spin_cost",
    "get_kazik_win_chance",
    "get_cooldown_seconds",
    "total_wealth",
    "compute_rank",
    "compute_leaderboard",
    "make_inventory_item",
    "find_inventory_item",
    "sync_exclusive_stock",
]

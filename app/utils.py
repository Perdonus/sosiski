from __future__ import annotations

import html
import random
import secrets
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from cards import Card, card_currency, card_display_name
from config import KAZIK_DIGITS, KAZIK_WIN_WEIGHTS, RARITY_NAMES, RARITY_ORDER, TIMEZONE


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes:
        parts.append(f"{minutes}м")
    if secs or not parts:
        parts.append(f"{secs}с")
    return " ".join(parts)


def format_vip_remaining(seconds: int) -> str:
    return format_duration(seconds)


def escape_html(text: str) -> str:
    return html.escape(text or "")


def format_short_amount(value: Optional[int], currency: str) -> str:
    if value is None:
        return "не задана"
    if currency == "stars":
        return f"{int(value)}⭐"
    return f"{int(value)}р"


def format_card_label(card: Card) -> str:
    rarity = RARITY_NAMES.get(card.rarity, card.rarity)
    return f"({rarity}) {card_display_name(card)}"


def format_price_with_old_html(
    new_price: str, old_price: str, *, italic_old: bool
) -> str:
    old_text = escape_html(old_price)
    if italic_old:
        old_text = f"<i>{old_text}</i>"
    return f"{escape_html(new_price)} <s>{old_text}</s>"


def now_local() -> datetime:
    tz_name = TIMEZONE
    if not tz_name:
        return datetime.now().astimezone()
    try:
        return datetime.now(tz=ZoneInfo(tz_name))
    except Exception:
        return datetime.now().astimezone()


def user_age_days(user: Dict[str, object], now: Optional[datetime] = None) -> Optional[int]:
    if now is None:
        now = now_local()
    created = user.get("created_at")
    if not isinstance(created, datetime):
        return None
    if created.tzinfo is None and now.tzinfo is not None:
        created = created.replace(tzinfo=now.tzinfo)
    elif created.tzinfo is not None and now.tzinfo is None:
        now = now.replace(tzinfo=created.tzinfo)
    delta = now - created
    if delta.total_seconds() < 0:
        return 0
    return int(delta.total_seconds() // 86400)


def greeting_by_time(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    hour = now.hour
    if 5 <= hour < 12:
        return "Доброе утро"
    if 12 <= hour < 18:
        return "Добрый день"
    if 18 <= hour < 23:
        return "Добрый вечер"
    return "Доброй ночи"


def parse_referrer_id(payload: str) -> Optional[str]:
    raw = (payload or "").strip()
    if not raw:
        return None
    if raw.startswith("ref_"):
        candidate = raw[4:]
    elif raw.startswith("ref"):
        candidate = raw[3:]
        if candidate.startswith("_"):
            candidate = candidate[1:]
    else:
        return None
    candidate = candidate.strip()
    return candidate if candidate.isdigit() else None


def make_item_id(user_id: int) -> str:
    token = secrets.token_urlsafe(6)
    return f"it_{user_id}_{token}"


def get_user_label(user) -> str:
    if not user:
        return ""
    username = getattr(user, "username", None)
    if username:
        return f"@{username}"
    full_name = getattr(user, "full_name", None)
    if full_name:
        return str(full_name)
    user_id = getattr(user, "id", None)
    return str(user_id) if user_id is not None else ""


def kazik_reward_rarities(digit: int) -> List[str]:
    if digit == 1:
        return ["dno", "common", "uncommon"]
    if digit == 2:
        return ["uncommon", "rare", "epic"]
    return ["legendary", "platinum", "meme"]


def build_draw_caption(user_label: str, card: Card) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    return "\n".join(
        [
            f"{user_label}, вам выпала сосиска!",
            f"{format_card_label(card)} - {price_text}",
        ]
    )


def build_upgrade_warning_caption(user_label: str, card: Card) -> str:
    base = build_draw_caption(user_label, card)
    return "\n".join(
        [
            "Вы уверены? Вы можете потерять карту.",
            base,
        ]
    )


def build_upgrade_success_caption(user_label: str, card: Card) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    return "\n".join(
        [
            f"{user_label}, редкость повышена (1)!",
            f"{format_card_label(card)} - {price_text}",
        ]
    )


def build_upgrade_fail_caption(user_label: str) -> str:
    return "\n".join(
        [
            f"{user_label}, не повезло.",
            "Сосиска потеряна.",
        ]
    )


def get_next_rarity(
    rarity: str, *, allow_exclusive: bool = False, allow_meme: bool = False
) -> Optional[str]:
    if rarity not in RARITY_ORDER:
        return None
    index = RARITY_ORDER.index(rarity)
    for next_rarity in RARITY_ORDER[index + 1 :]:
        if next_rarity == "meme" and not allow_meme:
            continue
        if next_rarity == "exclusive" and not allow_exclusive:
            continue
        return next_rarity
    return None


def build_kazik_text_line(digits: List[int], revealed: int) -> str:
    parts = []
    for index in range(3):
        if index < revealed and index < len(digits):
            parts.append(str(digits[index]))
        else:
            parts.append("?")
    return " | ".join(parts)


def roll_kazik_digits(*, win_chance: float) -> List[int]:
    if random.random() < win_chance:
        weights = [float(KAZIK_WIN_WEIGHTS.get(digit, 1.0)) for digit in KAZIK_DIGITS]
        winner = random.choices(KAZIK_DIGITS, weights=weights, k=1)[0]
        return [winner, winner, winner]
    digits = [random.choice(KAZIK_DIGITS) for _ in range(3)]
    while digits[0] == digits[1] == digits[2]:
        digits = [random.choice(KAZIK_DIGITS) for _ in range(3)]
    return digits


def boost_drop_chances(
    drop_chances: Dict[str, float],
    boost_rarities: List[str],
    multiplier: float,
) -> Dict[str, float]:
    if multiplier == 1 or not boost_rarities:
        return drop_chances
    boosted = dict(drop_chances)
    for rarity in boost_rarities:
        if rarity in boosted:
            boosted[rarity] = max(0.0, boosted[rarity] * multiplier)
    return boosted

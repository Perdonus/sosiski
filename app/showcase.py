from __future__ import annotations

import random
from typing import Dict, List, Tuple

from config import RARITY_NAMES

SHOWCASE_CRAFT_RARITIES = ["epic", "legendary", "platinum", "meme"]

EFFECT_LABELS = {
    "balance_daily": "Баланс в день",
    "free_rolls_daily": "Фри крутки в день",
    "kazik_spins_daily": "Казик-спины в день",
    "stars_daily": "Звезды в день",
    "drop_multiplier": "Шанс редких",
    "sell_multiplier": "Продажа",
    "extra_card_chance": "Доп. сосиска",
    "vip_infinite": "VIP навсегда",
}

POSITIVE_EFFECT_TITLES = {
    "balance_daily": "Дивиденды",
    "free_rolls_daily": "Фри-ускорение",
    "kazik_spins_daily": "Барабанный бонус",
    "stars_daily": "Звездный поток",
    "drop_multiplier": "Синяя полоса",
    "sell_multiplier": "Торговый нюх",
    "extra_card_chance": "Вторая порция",
    "vip_infinite": "Бессрочный VIP",
}

NEGATIVE_EFFECT_TITLES = {
    "balance_daily": "Налог",
    "free_rolls_daily": "Тормоз",
    "kazik_spins_daily": "Злой барабан",
    "stars_daily": "Черная дыра",
    "drop_multiplier": "Кривой шанс",
    "sell_multiplier": "Плохая сделка",
    "extra_card_chance": "Пустая тарелка",
}

EFFECT_RANGES: Dict[str, Dict[str, Tuple[float, float]]] = {
    "epic": {
        "balance_daily": (250, 520),
        "free_rolls_daily": (10, 22),
        "kazik_spins_daily": (2, 5),
        "drop_multiplier": (1.1, 1.25),
        "sell_multiplier": (1.1, 1.25),
        "extra_card_chance": (0.08, 0.16),
    },
    "legendary": {
        "balance_daily": (550, 1000),
        "free_rolls_daily": (25, 60),
        "kazik_spins_daily": (5, 12),
        "drop_multiplier": (1.22, 1.45),
        "sell_multiplier": (1.22, 1.45),
        "extra_card_chance": (0.12, 0.25),
        "stars_daily": (2, 6),
    },
    "platinum": {
        "balance_daily": (1200, 2400),
        "free_rolls_daily": (150, 260),
        "kazik_spins_daily": (12, 22),
        "drop_multiplier": (1.4, 1.75),
        "sell_multiplier": (1.4, 1.75),
        "extra_card_chance": (0.18, 0.3),
        "stars_daily": (4, 10),
        "vip_infinite": (1, 1),
    },
    "meme": {
        "balance_daily": (-600, -200),
        "free_rolls_daily": (-25, -10),
        "kazik_spins_daily": (-6, -2),
        "drop_multiplier": (0.6, 0.9),
        "sell_multiplier": (0.6, 0.9),
        "extra_card_chance": (-0.1, -0.02),
        "stars_daily": (-3, -1),
    },
}


def roll_showcase_effect(rarity: str) -> Tuple[str, float, Dict[str, object], str]:
    ranges = EFFECT_RANGES.get(rarity, EFFECT_RANGES["epic"])
    if rarity == "platinum" and random.random() < 0.05:
        effect_type = "vip_infinite"
    else:
        effect_type = random.choice(list(ranges.keys()))
    low, high = ranges[effect_type]
    if effect_type in {"balance_daily", "free_rolls_daily", "kazik_spins_daily"}:
        value = random.randint(int(low), int(high))
    elif effect_type == "vip_infinite":
        value = 1.0
    else:
        value = round(random.uniform(float(low), float(high)), 2)
    title_map = NEGATIVE_EFFECT_TITLES if rarity == "meme" else POSITIVE_EFFECT_TITLES
    title = title_map.get(effect_type, "Усиление")
    payload: Dict[str, object] = {}
    return effect_type, float(value), payload, title


def format_showcase_effect(effect_type: str, value: float) -> str:
    if effect_type == "balance_daily":
        sign = "+" if value >= 0 else "-"
        return f"{sign}{abs(int(value))}р в день"
    if effect_type == "free_rolls_daily":
        sign = "+" if value >= 0 else "-"
        return f"{sign}{abs(int(value))} фри круток в день"
    if effect_type == "kazik_spins_daily":
        sign = "+" if value >= 0 else "-"
        return f"{sign}{abs(int(value))} казик-спинов в день"
    if effect_type == "stars_daily":
        sign = "+" if value >= 0 else "-"
        return f"{sign}{abs(int(value))}⭐ в день"
    if effect_type == "extra_card_chance":
        sign = "+" if value >= 0 else "-"
        pct = abs(value) * 100
        return f"{sign}{pct:.0f}% доп. сосиска"
    if effect_type == "vip_infinite":
        return "VIP навсегда"
    if effect_type == "drop_multiplier":
        return f"Шанс редких x{value:.2f}"
    if effect_type == "sell_multiplier":
        return f"Продажа x{value:.2f}"
    label = EFFECT_LABELS.get(effect_type, effect_type)
    return f"{label}: {value}"


def format_showcase_card_caption(card: Dict[str, object]) -> str:
    rarity = str(card.get("rarity") or "")
    rarity_label = RARITY_NAMES.get(rarity, rarity)
    title = str(card.get("title") or "Карта")
    effect_type = str(card.get("effect_type") or "")
    value = float(card.get("effect_value") or 0)
    effect_text = format_showcase_effect(effect_type, value)
    slot = card.get("slot")
    slot_text = f"Слот: {slot}" if slot is not None else "Слот: нет"
    return "\n".join(
        [
            title,
            f"Редкость: {rarity_label}",
            f"Эффект: {effect_text}",
            slot_text,
        ]
    )


def summarize_showcase_effects(cards: List[Dict[str, object]]) -> Dict[str, float]:
    summary = {
        "balance_daily": 0.0,
        "free_rolls_daily": 0.0,
        "kazik_spins_daily": 0.0,
        "stars_daily": 0.0,
        "drop_multiplier": 1.0,
        "sell_multiplier": 1.0,
        "extra_card_chance": 0.0,
        "vip_infinite": 0.0,
    }
    for card in cards:
        effect_type = str(card.get("effect_type") or "")
        value = float(card.get("effect_value") or 0)
        if effect_type == "drop_multiplier":
            summary["drop_multiplier"] *= value if value else 1.0
        elif effect_type == "sell_multiplier":
            summary["sell_multiplier"] *= value if value else 1.0
        elif effect_type == "extra_card_chance":
            summary["extra_card_chance"] += value
        elif effect_type == "vip_infinite":
            summary["vip_infinite"] += 1.0
        elif effect_type in summary:
            summary[effect_type] += value
    summary["extra_card_chance"] = max(0.0, min(0.6, summary["extra_card_chance"]))
    return summary

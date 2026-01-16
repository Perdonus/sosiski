import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import (
    DROP_CHANCE_KEYS,
    IMAGE_EXTENSIONS,
    RARITY_DIRS,
    RARITY_ORDER,
    ROLL_RARITY_ORDER,
    SAUSAGE_DIR,
)


@dataclass
class Card:
    rarity: str
    file: str
    name: Optional[str]
    price: Optional[int]


def parse_price(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() == "none":
            return None
        try:
            return int(float(cleaned))
        except ValueError:
            return None
    return None


def parse_drop_chance(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() == "none":
            return None
        try:
            return float(cleaned.replace(",", "."))
        except ValueError:
            return None
    return None


def format_drop_chance(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def format_price(value: Optional[int]) -> str:
    if value is None:
        return "не задана"
    return f"{value} руб."


def format_stars(value: Optional[int]) -> str:
    if value is None:
        return "не задана"
    return f"{value} ★"


def card_currency(card: Card) -> str:
    return "stars" if card.rarity == "exclusive" else "rub"


def format_amount(value: Optional[int], currency: str) -> str:
    if currency == "stars":
        return format_stars(value)
    return format_price(value)


def format_card_price(card: Card) -> str:
    return format_amount(card.price, card_currency(card))


def calc_sale_price(card: Card) -> Optional[int]:
    if card.price is None:
        return None
    if card.rarity == "exclusive":
        return card.price
    return int(card.price * 0.6)


def format_card_sale_price(card: Card) -> str:
    return format_amount(calc_sale_price(card), card_currency(card))


def compute_default_drop_chances(
    cards: Dict[str, List[Dict[str, object]]],
) -> Dict[str, float]:
    total = sum(len(cards.get(rarity, [])) for rarity in ROLL_RARITY_ORDER)
    if total <= 0:
        equal = 100 / len(ROLL_RARITY_ORDER)
        return {rarity: equal for rarity in ROLL_RARITY_ORDER}
    result = {}
    for rarity in ROLL_RARITY_ORDER:
        count = len(cards.get(rarity, []))
        result[rarity] = (count / total) * 100
    return result


def load_drop_chances(
    env: Dict[str, str],
    defaults: Dict[str, float],
) -> Dict[str, float]:
    result: Dict[str, float] = {}
    for rarity in ROLL_RARITY_ORDER:
        key = DROP_CHANCE_KEYS[rarity]
        parsed = parse_drop_chance(env.get(key))
        if parsed is None:
            parsed = defaults.get(rarity, 0.0)
        result[rarity] = max(0.0, float(parsed))
    return result


def scan_card_files() -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {rarity: [] for rarity in RARITY_ORDER}
    for rarity in RARITY_ORDER:
        folder = SAUSAGE_DIR / RARITY_DIRS[rarity]
        folder.mkdir(parents=True, exist_ok=True)
        for path in folder.iterdir():
            if not path.is_file():
                continue
            if path.suffix.lower() not in IMAGE_EXTENSIONS:
                continue
            result[rarity].append(path.name)
        result[rarity].sort()
    return result


def merge_cards(
    existing: Dict[str, List[Dict[str, object]]],
    scanned: Dict[str, List[str]],
) -> Dict[str, List[Dict[str, object]]]:
    merged = {rarity: list(existing.get(rarity, [])) for rarity in RARITY_ORDER}
    file_to_item: Dict[str, Tuple[str, Dict[str, object]]] = {}
    for rarity in RARITY_ORDER:
        cleaned = []
        for item in merged[rarity]:
            if not isinstance(item, dict):
                continue
            filename = item.get("file")
            if not filename or not isinstance(filename, str):
                continue
            if filename in file_to_item:
                continue
            file_to_item[filename] = (rarity, item)
            cleaned.append(item)
        merged[rarity] = cleaned

    for rarity in RARITY_ORDER:
        for filename in scanned.get(rarity, []):
            if filename in file_to_item:
                old_rarity, item = file_to_item[filename]
                if old_rarity != rarity:
                    merged[old_rarity] = [
                        existing_item
                        for existing_item in merged[old_rarity]
                        if existing_item.get("file") != filename
                    ]
                    merged[rarity].append(item)
                    file_to_item[filename] = (rarity, item)
            else:
                new_item = {"file": filename, "name": None, "price": None}
                merged[rarity].append(new_item)
                file_to_item[filename] = (rarity, new_item)

    for rarity in RARITY_ORDER:
        merged[rarity].sort(key=lambda item: str(item.get("file", "")).lower())
    return merged


def build_card_index(
    cards: Dict[str, List[Dict[str, object]]],
) -> Tuple[Dict[str, Card], Dict[str, List[Card]]]:
    card_map: Dict[str, Card] = {}
    by_rarity: Dict[str, List[Card]] = {rarity: [] for rarity in RARITY_ORDER}
    for rarity in RARITY_ORDER:
        for item in cards.get(rarity, []):
            filename = item.get("file")
            if not filename or not isinstance(filename, str):
                continue
            card = Card(
                rarity=rarity,
                file=filename,
                name=item.get("name"),
                price=parse_price(item.get("price")),
            )
            by_rarity[rarity].append(card)
            card_map[filename] = card
    return card_map, by_rarity


def card_display_name(card: Card) -> str:
    if card.name and str(card.name).strip():
        return str(card.name).strip()
    stem = Path(card.file).stem
    return stem if stem else "Без имени"


def card_file_path(card: Card) -> Path:
    return SAUSAGE_DIR / RARITY_DIRS[card.rarity] / card.file


def filter_existing_cards(
    by_rarity: Dict[str, List[Card]],
) -> Dict[str, List[Card]]:
    result: Dict[str, List[Card]] = {}
    for rarity, cards in by_rarity.items():
        result[rarity] = [card for card in cards if card_file_path(card).exists()]
    return result


def pick_random_card(
    by_rarity: Dict[str, List[Card]],
    drop_chances: Dict[str, float],
    rarity_order: Optional[List[str]] = None,
) -> Optional[Card]:
    if rarity_order is None:
        rarity_order = ROLL_RARITY_ORDER
    available = [rarity for rarity in rarity_order if by_rarity.get(rarity)]
    if not available:
        return None
    weights = [drop_chances.get(rarity, 0.0) for rarity in available]
    if sum(weights) <= 0:
        weights = [1.0 for _ in available]
    picked_rarity = random.choices(available, weights=weights, k=1)[0]
    return random.choice(by_rarity[picked_rarity])


__all__ = [
    "Card",
    "parse_price",
    "parse_drop_chance",
    "format_drop_chance",
    "format_price",
    "format_stars",
    "card_currency",
    "format_amount",
    "format_card_price",
    "calc_sale_price",
    "format_card_sale_price",
    "compute_default_drop_chances",
    "load_drop_chances",
    "scan_card_files",
    "merge_cards",
    "build_card_index",
    "card_display_name",
    "card_file_path",
    "filter_existing_cards",
    "pick_random_card",
]

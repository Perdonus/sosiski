from __future__ import annotations

import json
from typing import Dict, List, Tuple

from config import (
    DROP_CHANCE_KEYS,
    ENV_PATH,
    RARITY_PRICE_MULTIPLIERS,
    RARITY_ORDER,
    ROLL_RARITY_ORDER,
    ensure_env_defaults,
    read_env_file,
    strip_quotes,
    upsert_env_lines,
)
from cards import (
    Card,
    build_card_index,
    compute_default_drop_chances,
    format_drop_chance,
    load_drop_chances,
    merge_cards,
    parse_drop_chance,
    scan_card_files,
)


def load_cards() -> Tuple[Dict[str, Card], Dict[str, List[Card]], Dict[str, float]]:
    lines, env = read_env_file(ENV_PATH)
    lines, env = ensure_env_defaults(lines, env)
    raw_cards = env.get("SOSISKA_CARDS", "")
    cards_data: Dict[str, List[Dict[str, object]]] = {}
    if raw_cards:
        try:
            cards_data = json.loads(strip_quotes(raw_cards))
        except json.JSONDecodeError:
            cards_data = {}
    for rarity in RARITY_ORDER:
        cards_data.setdefault(rarity, [])
    scanned = scan_card_files()
    merged = merge_cards(cards_data, scanned)
    updated_cards_json = json.dumps(merged, ensure_ascii=False)
    updates = {}
    if env.get("SOSISKA_CARDS") != updated_cards_json:
        updates["SOSISKA_CARDS"] = updated_cards_json

    default_drop = compute_default_drop_chances(merged)
    if "meme" in default_drop and "platinum" in default_drop:
        default_drop["meme"] = default_drop["platinum"]
    for rarity in ROLL_RARITY_ORDER:
        key = DROP_CHANCE_KEYS[rarity]
        if parse_drop_chance(env.get(key)) is None:
            updates[key] = format_drop_chance(default_drop[rarity])

    if updates:
        lines = upsert_env_lines(lines, updates)
        env.update(updates)
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    card_map, cards_by_rarity = build_card_index(merged)
    if RARITY_PRICE_MULTIPLIERS:
        for card in card_map.values():
            if card.price is None:
                continue
            multiplier = float(RARITY_PRICE_MULTIPLIERS.get(card.rarity, 1.0))
            if multiplier <= 0:
                continue
            card.price = int(round(card.price * multiplier))
    drop_chances = load_drop_chances(env, default_drop)
    return card_map, cards_by_rarity, drop_chances

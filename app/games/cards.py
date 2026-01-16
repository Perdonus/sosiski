from __future__ import annotations

import random
import time
from typing import Dict, List, Optional, Tuple

SUITS = ["S", "H", "D", "C"]
RANKS_36 = ["6", "7", "8", "9", "10", "J", "Q", "K", "A"]
RANKS_52 = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
RANK_VALUES = {rank: idx for idx, rank in enumerate(RANKS_52, start=2)}
TURN_TIMEOUT_SEC = 60


def build_cards_deck(deck_size: int) -> List[Dict[str, object]]:
    ranks = RANKS_36 if int(deck_size) == 36 else RANKS_52
    return [{"rank": rank, "suit": suit, "value": RANK_VALUES[rank]} for suit in SUITS for rank in ranks]


def _card_id(card: Dict[str, object]) -> str:
    return f"{card.get('rank', '')}{card.get('suit', '')}"


def _card_beats(
    attack: Dict[str, object],
    defend: Dict[str, object],
    trump: str,
) -> bool:
    if not attack or not defend:
        return False
    if defend["suit"] == attack["suit"] and defend["value"] > attack["value"]:
        return True
    if defend["suit"] == trump and attack["suit"] != trump:
        return True
    return False


def _lowest_trump_index(
    order: List[int], hands: Dict[int, List[Dict[str, object]]], trump: str
) -> int:
    lowest_value = None
    lowest_index = 0
    for idx, user_id in enumerate(order):
        for card in hands.get(user_id, []):
            if card.get("suit") != trump:
                continue
            value = int(card.get("value", 0))
            if lowest_value is None or value < lowest_value:
                lowest_value = value
                lowest_index = idx
    return lowest_index


def _next_active_index(order: List[int], active: Dict[int, bool], start_index: int) -> int:
    if not order:
        return 0
    count = len(order)
    for offset in range(1, count + 1):
        idx = (start_index + offset) % count
        if active.get(order[idx], True):
            return idx
    return start_index


def _active_map(players: List[Dict[str, object]]) -> Dict[int, bool]:
    return {int(player["user_id"]): not bool(player.get("finished")) for player in players}


def _player_by_id(players: List[Dict[str, object]], user_id: int) -> Optional[Dict[str, object]]:
    for player in players:
        if int(player.get("user_id", 0)) == int(user_id):
            return player
    return None


def _set_turn(state: Dict[str, object], user_id: Optional[int]) -> None:
    if user_id is None:
        state["turn_owner_id"] = None
        state["turn_started_at"] = None
        return
    state["turn_owner_id"] = int(user_id)
    state["turn_started_at"] = int(time.time())


def _turn_owner_from_phase(state: Dict[str, object]) -> Optional[int]:
    phase = state.get("phase")
    if phase == "attack":
        return _order_id(state, state.get("attacker_index"))
    if phase == "defend":
        return _order_id(state, state.get("defender_index"))
    if phase in {"throw", "throw_take"}:
        return _order_id(state, state.get("attacker_index"))
    return None


def _sync_turn(state: Dict[str, object]) -> None:
    if state.get("status") != "active":
        _set_turn(state, None)
        return
    _set_turn(state, _turn_owner_from_phase(state))


def init_cards_game_state(
    players: List[Dict[str, object]],
    deck_size: int,
    mode: str,
) -> Dict[str, object]:
    deck = build_cards_deck(deck_size)
    random.shuffle(deck)
    trump = deck[-1] if deck else None
    trump_suit = trump["suit"] if trump else ""
    order = [int(player["user_id"]) for player in players]
    hands: Dict[int, List[Dict[str, object]]] = {uid: [] for uid in order}
    for _ in range(6):
        for uid in order:
            if deck:
                hands[uid].append(deck.pop())
    for player in players:
        uid = int(player["user_id"])
        player["hand"] = hands.get(uid, [])
        player["finished"] = False
    attacker_index = _lowest_trump_index(order, hands, trump_suit) if trump else 0
    active = _active_map(players)
    defender_index = _next_active_index(order, active, attacker_index)
    max_attack = min(len(players[defender_index]["hand"]), 6) if players else 0
    state = {
        "status": "active",
        "mode": mode,
        "deck_size": int(deck_size),
        "order": order,
        "players": players,
        "deck": deck,
        "trump": trump,
        "table": [],
        "discard": [],
        "attacker_index": attacker_index,
        "defender_index": defender_index,
        "phase": "attack",
        "passes": [],
        "pending_take": False,
        "max_attack": max_attack,
        "finish_order": [],
        "winner_id": None,
    }
    _set_turn(state, int(order[attacker_index]) if order else None)
    return state


def serialize_cards_state(state: Dict[str, object], viewer_id: int) -> Dict[str, object]:
    viewer_id = int(viewer_id)
    players = []
    for player in state.get("players", []):
        uid = int(player.get("user_id", 0))
        hand = player.get("hand", [])
        item = {
            "user_id": uid,
            "name": str(player.get("name") or ""),
            "hand_count": len(hand),
            "finished": bool(player.get("finished")),
        }
        if uid == viewer_id:
            item["hand"] = hand
        players.append(item)
    return {
        "status": state.get("status"),
        "mode": state.get("mode"),
        "deck_size": state.get("deck_size"),
        "trump": state.get("trump"),
        "table": state.get("table"),
        "discard_count": len(state.get("discard", [])),
        "deck_count": len(state.get("deck", [])),
        "players": players,
        "order": state.get("order", []),
        "attacker_id": _order_id(state, state.get("attacker_index")),
        "defender_id": _order_id(state, state.get("defender_index")),
        "phase": state.get("phase"),
        "pending_take": bool(state.get("pending_take")),
        "max_attack": int(state.get("max_attack", 0)),
        "winner_id": state.get("winner_id"),
        "finish_order": state.get("finish_order", []),
        "turn_owner_id": state.get("turn_owner_id"),
        "turn_started_at": state.get("turn_started_at"),
        "turn_timeout_sec": TURN_TIMEOUT_SEC,
    }


def _order_id(state: Dict[str, object], index: Optional[int]) -> Optional[int]:
    order = state.get("order") or []
    if index is None or not order:
        return None
    try:
        return int(order[int(index)])
    except (ValueError, IndexError, TypeError):
        return None


def _table_ranks(table: List[Dict[str, object]]) -> List[str]:
    ranks = []
    for entry in table:
        attack = entry.get("attack")
        defense = entry.get("defense")
        if attack:
            ranks.append(str(attack.get("rank")))
        if defense:
            ranks.append(str(defense.get("rank")))
    return ranks


def _can_attack(table: List[Dict[str, object]], card: Dict[str, object]) -> bool:
    if not table:
        return True
    ranks = _table_ranks(table)
    return str(card.get("rank")) in ranks


def _select_card(hand: List[Dict[str, object]], card_id: str) -> Optional[Dict[str, object]]:
    for card in hand:
        if _card_id(card) == card_id:
            return card
    return None


def _remove_card(hand: List[Dict[str, object]], card: Dict[str, object]) -> None:
    for index, item in enumerate(hand):
        if item is card or _card_id(item) == _card_id(card):
            del hand[index]
            return


def _resolve_round(state: Dict[str, object], defender_took: bool) -> None:
    table = state.get("table", [])
    players = state.get("players", [])
    order = state.get("order", [])
    attacker_index = int(state.get("attacker_index", 0) or 0)
    defender_index = int(state.get("defender_index", 0) or 0)
    deck = state.get("deck", [])
    discard = state.get("discard", [])

    if defender_took:
        defender_id = int(order[defender_index])
        defender = _player_by_id(players, defender_id)
        if defender is not None:
            for entry in table:
                if entry.get("attack"):
                    defender["hand"].append(entry["attack"])
                if entry.get("defense"):
                    defender["hand"].append(entry["defense"])
    else:
        for entry in table:
            if entry.get("attack"):
                discard.append(entry["attack"])
            if entry.get("defense"):
                discard.append(entry["defense"])
    state["table"] = []
    state["passes"] = []
    state["pending_take"] = False

    active = _active_map(players)
    for offset in range(len(order)):
        idx = (attacker_index + offset) % len(order)
        uid = int(order[idx])
        player = _player_by_id(players, uid)
        if not player or player.get("finished"):
            continue
        while deck and len(player["hand"]) < 6:
            player["hand"].append(deck.pop())

    if not deck:
        for player in players:
            if not player.get("finished") and not player.get("hand"):
                player["finished"] = True
                state["finish_order"].append(int(player["user_id"]))
                if state.get("winner_id") is None:
                    state["winner_id"] = int(player["user_id"])

    active = _active_map(players)
    if sum(1 for value in active.values() if value) <= 1:
        state["status"] = "finished"
        return

    if defender_took:
        attacker_index = attacker_index
        defender_index = _next_active_index(order, active, attacker_index)
    else:
        attacker_index = defender_index
        defender_index = _next_active_index(order, active, attacker_index)

    state["attacker_index"] = attacker_index
    state["defender_index"] = defender_index
    defender_id = int(order[defender_index])
    defender = _player_by_id(players, defender_id)
    state["max_attack"] = min(len(defender["hand"]) if defender else 0, 6)
    state["phase"] = "attack"
    _set_turn(state, int(order[attacker_index]) if order else None)


def apply_cards_action(
    state: Dict[str, object],
    user_id: int,
    action: str,
    payload: Dict[str, object],
) -> Tuple[bool, Optional[str]]:
    if state.get("status") != "active":
        return False, "game_closed"
    user_id = int(user_id)
    players = state.get("players", [])
    order = state.get("order", [])
    player = _player_by_id(players, user_id)
    if not player or player.get("finished"):
        return False, "not_player"

    trump = state.get("trump") or {}
    trump_suit = trump.get("suit", "")
    mode = str(state.get("mode") or "classic")
    attacker_id = _order_id(state, state.get("attacker_index"))
    defender_id = _order_id(state, state.get("defender_index"))
    table = state.get("table", [])
    phase = state.get("phase")
    max_attack = int(state.get("max_attack", 0) or 0)

    card_id = str(payload.get("card_id") or "")
    card = _select_card(player.get("hand", []), card_id) if card_id else None

    if action == "attack":
        if user_id != attacker_id or phase != "attack":
            return False, "not_turn"
        if not card:
            return False, "card_missing"
        if len(table) >= max_attack:
            return False, "limit"
        if not _can_attack(table, card):
            return False, "rank"
        _remove_card(player["hand"], card)
        table.append({"attack": card, "defense": None})
        state["phase"] = "defend"
        _sync_turn(state)
        return True, None

    if action == "defend":
        if user_id != defender_id or phase != "defend":
            return False, "not_turn"
        if not card:
            return False, "card_missing"
        try:
            target_index = int(payload.get("target_index", 0))
        except (TypeError, ValueError):
            target_index = 0
        if target_index < 0 or target_index >= len(table):
            return False, "target"
        target = table[target_index]
        if target.get("defense"):
            return False, "already_defended"
        if not _card_beats(target.get("attack"), card, trump_suit):
            can_transfer = (
                mode == "transfer"
                and not any(entry.get("defense") for entry in table)
                and str(card.get("rank")) == str(target.get("attack", {}).get("rank"))
            )
            if not can_transfer:
                return False, "no_beat"
            _remove_card(player["hand"], card)
            table.append({"attack": card, "defense": None})
            active = _active_map(players)
            new_defender_index = _next_active_index(order, active, int(state.get("defender_index", 0)))
            state["attacker_index"] = int(state.get("defender_index", 0))
            state["defender_index"] = new_defender_index
            new_defender_id = int(order[new_defender_index])
            defender = _player_by_id(players, new_defender_id)
            state["max_attack"] = min(len(defender["hand"]) if defender else 0, 6)
            state["phase"] = "defend"
            _sync_turn(state)
            return True, None
        _remove_card(player["hand"], card)
        target["defense"] = card
        if all(entry.get("defense") for entry in table):
            state["phase"] = "throw"
            _sync_turn(state)
            return True, None
        _sync_turn(state)
        return True, None

    if action == "take":
        if user_id != defender_id or phase != "defend":
            return False, "not_turn"
        state["pending_take"] = True
        state["phase"] = "throw_take"
        _sync_turn(state)
        return True, None

    if action == "throw":
        if user_id == defender_id:
            return False, "not_turn"
        if phase not in {"throw", "throw_take"}:
            return False, "not_turn"
        if mode == "classic" and user_id != attacker_id:
            return False, "not_turn"
        if not card:
            return False, "card_missing"
        if len(table) >= max_attack:
            return False, "limit"
        if not _can_attack(table, card):
            return False, "rank"
        _remove_card(player["hand"], card)
        table.append({"attack": card, "defense": None})
        state["phase"] = "defend" if not state.get("pending_take") else "throw_take"
        state["passes"] = []
        _sync_turn(state)
        return True, None

    if action == "pass":
        if user_id == defender_id or phase not in {"throw", "throw_take"}:
            return False, "not_turn"
        if mode == "classic" and user_id != attacker_id:
            return False, "not_turn"
        passes = state.get("passes", [])
        if user_id not in passes:
            passes.append(user_id)
        state["passes"] = passes
        eligible = []
        for uid in order:
            uid_int = int(uid)
            if uid_int == int(defender_id):
                continue
            candidate = _player_by_id(players, uid_int)
            if not candidate or candidate.get("finished"):
                continue
            eligible.append(uid_int)
        if all(uid in passes for uid in eligible):
            _resolve_round(state, bool(state.get("pending_take")))
        _sync_turn(state)
        return True, None

    return False, "unknown"


def apply_cards_timeout(state: Dict[str, object], now_ts: Optional[int] = None) -> bool:
    if state.get("status") != "active":
        return False
    players = state.get("players", [])
    owner_id = state.get("turn_owner_id") or _turn_owner_from_phase(state)
    if owner_id is None:
        _sync_turn(state)
        return False
    owner = _player_by_id(players, int(owner_id))
    if owner and owner.get("finished"):
        timed_out = True
    else:
        started = int(state.get("turn_started_at") or 0)
        if started <= 0:
            _set_turn(state, owner_id)
            return False
        now_value = int(now_ts or time.time())
        timed_out = (now_value - started) >= TURN_TIMEOUT_SEC
    if not timed_out:
        return False
    if owner and not owner.get("finished"):
        owner["finished"] = True
        state.setdefault("finish_order", []).append(int(owner_id))
    active = [p for p in players if not p.get("finished")]
    if len(active) <= 1:
        state["status"] = "finished"
        state["winner_id"] = int(active[0]["user_id"]) if active else None
        state["table"] = []
        state["pending_take"] = False
        state["phase"] = "finished"
        _set_turn(state, None)
        return True
    _resolve_round(state, defender_took=False)
    _sync_turn(state)
    return True

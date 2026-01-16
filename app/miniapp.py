from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qsl, quote

from aiohttp import web

from app.kazik import (
    kazik_daily_free_left,
    kazik_free_spins_limit,
    kazik_reset_remaining_seconds,
    kazik_should_reset,
)
from app.logic import is_vip
from app.handlers.donate import send_stars_menu_to_user
from app.games import (
    apply_cards_action,
    apply_cards_timeout,
    init_cards_game_state,
    serialize_cards_state,
    apply_chess_action,
    apply_chess_timeout,
    init_chess_state,
    serialize_chess_state,
)
from app.repo import (
    add_inventory_item_safe,
    adjust_user_balance,
    consume_inventory_items,
    create_game_lobby,
    delete_game_lobby,
    get_game_lobby,
    get_or_create_user,
    get_user,
    list_game_lobbies,
    list_inventory,
    update_game_lobby,
)
from app.utils import kazik_reward_rarities, now_local, roll_kazik_digits, user_age_days
from cards import Card, card_display_name, filter_existing_cards
from config import (
    BASE_DIR,
    BOT_TOKEN,
    KAZIK_BONUS_SPINS_PER_BATCH,
    KAZIK_DIGITS,
    KAZIK_GUARANTEE_SPINS,
    KAZIK_PAID_SPINS_FOR_BONUS,
    KAZIK_STAR_SPIN_COST,
    KAZIK_WIN_CHANCE,
    MINIAPP_URL,
    NEWBIE_DAYS_STRONG,
    NEWBIE_DAYS_VIP,
    NEWBIE_KAZIK_WIN_MULTIPLIER,
    RARITY_DIRS,
    RARITY_NAMES,
    ROLL_RARITY_EXCLUDE,
    SAUSAGE_DIR,
    VIP_KAZIK_WIN_CHANCE,
)

MINIAPP_DIR = BASE_DIR / "miniapp"
STATIC_DIR = MINIAPP_DIR / "static"

KAZIK_BUY_PACKS = (
    {"spins": 1, "cost": 1},
    {"spins": 5, "cost": 4},
    {"spins": 10, "cost": 7},
    {"spins": 15, "cost": 11},
)

kazik_logger = logging.getLogger("kazik")
cards_logger = logging.getLogger("cards")

CARDS_GAME = "cards"
CARDS_MAX_PLAYERS = 4

CHESS_GAME = "chess"
CHESS_MAX_PLAYERS = 2

UPGRADE_MAX_ITEMS = 1
UPGRADE_FILTERS = (75, 50, 25)
UPGRADE_MIN_CHANCE = 0.05
UPGRADE_MAX_CHANCE = 0.95
UPGRADE_TARGET_LIMIT = 60
UPGRADE_EXCLUDED_RARITIES = {"exclusive", "meme"}


def _parse_init_data(init_data: str) -> Optional[Dict[str, str]]:
    if not BOT_TOKEN:
        return None
    if not init_data:
        return None
    data = dict(parse_qsl(init_data, keep_blank_values=True))
    provided_hash = data.pop("hash", None)
    if not provided_hash:
        return None
    secret_key = hmac.new(
        b"WebAppData",
        BOT_TOKEN.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    data_check = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    computed_hash = hmac.new(
        secret_key,
        data_check.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(computed_hash, str(provided_hash)):
        return None
    return data


def _extract_user_data(parsed: Dict[str, str]) -> Optional[Dict[str, object]]:
    raw_user = parsed.get("user")
    if not raw_user:
        return None
    try:
        return json.loads(raw_user)
    except json.JSONDecodeError:
        return None


def _user_identity(user_data: Dict[str, object]) -> Optional[Tuple[int, str, str]]:
    try:
        user_id = int(user_data.get("id"))
    except (TypeError, ValueError):
        return None
    first = str(user_data.get("first_name") or "").strip()
    last = str(user_data.get("last_name") or "").strip()
    full_name = " ".join(part for part in [first, last] if part).strip()
    username = str(user_data.get("username") or "").strip()
    return user_id, full_name, username


def _init_data_from_request(request: web.Request) -> Optional[str]:
    return (
        request.headers.get("X-Init-Data")
        or request.query.get("initData")
        or request.query.get("init_data")
    )


async def _load_user(request: web.Request) -> Optional[Dict[str, object]]:
    init_data = _init_data_from_request(request)
    parsed = _parse_init_data(init_data or "")
    if not parsed:
        return None
    user_data = _extract_user_data(parsed)
    if not user_data:
        return None
    identity = _user_identity(user_data)
    if not identity:
        return None
    user_id, full_name, username = identity
    pool = request.app.get("db_pool")
    if not pool:
        return None
    return await get_or_create_user(pool, user_id, full_name, username)


def _pick_kazik_reward_card(
    by_rarity: Dict[str, list[Card]],
    digit: int,
) -> Optional[Card]:
    available_by_rarity = filter_existing_cards(by_rarity)
    pool: list[Card] = []
    for rarity in kazik_reward_rarities(digit):
        pool.extend(available_by_rarity.get(rarity, []))
    if not pool:
        return None
    return pool[0] if len(pool) == 1 else pool[random.randrange(len(pool))]


def _kazik_win_chance(user: Dict[str, object]) -> float:
    base = VIP_KAZIK_WIN_CHANCE if is_vip(user) else KAZIK_WIN_CHANCE
    now = now_local()
    age_days = user_age_days(user, now)
    if age_days is None:
        return base
    vip_level = max(base, VIP_KAZIK_WIN_CHANCE)
    if age_days < NEWBIE_DAYS_STRONG:
        return vip_level * NEWBIE_KAZIK_WIN_MULTIPLIER
    if age_days < NEWBIE_DAYS_VIP:
        return vip_level
    return base


def _build_state(user: Dict[str, object]) -> Dict[str, object]:
    now = now_local()
    daily_free = kazik_daily_free_left(user, now)
    bonus_spins = int(user.get("kazik_bonus_spins", 0) or 0)
    free_rolls = int(user.get("free_rolls", 0) or 0)
    reset_seconds = kazik_reset_remaining_seconds(user, now)
    return {
        "balance": int(user.get("balance", 0) or 0),
        "stars": int(user.get("stars", 0) or 0),
        "vip": bool(is_vip(user)),
        "kazik": {
            "daily_free_left": daily_free,
            "free_limit": kazik_free_spins_limit(user),
            "bonus_spins": bonus_spins + free_rolls,
            "reset_seconds": reset_seconds,
            "spin_cost": KAZIK_STAR_SPIN_COST,
            "buy_packs": list(KAZIK_BUY_PACKS),
            "digits": list(KAZIK_DIGITS),
        },
    }


def _card_media_url(card: Card) -> str:
    base_url = MINIAPP_URL.rstrip("/") if MINIAPP_URL else "/miniapp"
    safe_file = quote(card.file)
    return f"{base_url}/media/{card.rarity}/{safe_file}"


def _upgrade_allowed_card(card: Optional[Card]) -> bool:
    if not card or card.price is None:
        return False
    if card.rarity in UPGRADE_EXCLUDED_RARITIES:
        return False
    return True


def _calculate_upgrade_chance(total_value: int, target_value: int) -> float:
    if total_value <= 0 or target_value <= 0:
        return 0.0
    raw = total_value / target_value
    return max(UPGRADE_MIN_CHANCE, min(UPGRADE_MAX_CHANCE, raw))


def _list_upgrade_targets(
    cards_by_rarity: Dict[str, list[Card]],
    total_value: int,
    min_chance: int,
) -> list[Dict[str, object]]:
    available_by_rarity = filter_existing_cards(cards_by_rarity)
    targets: list[Dict[str, object]] = []
    for rarity, cards in available_by_rarity.items():
        if rarity in UPGRADE_EXCLUDED_RARITIES or rarity in ROLL_RARITY_EXCLUDE:
            continue
        for card in cards:
            if not _upgrade_allowed_card(card):
                continue
            target_value = int(card.price or 0)
            if target_value <= total_value:
                continue
            chance = _calculate_upgrade_chance(total_value, target_value)
            chance_pct = int(round(chance * 100))
            if chance_pct < min_chance:
                continue
            targets.append(
                {
                    "file": card.file,
                    "name": card_display_name(card),
                    "rarity": card.rarity,
                    "rarity_label": RARITY_NAMES.get(card.rarity, card.rarity),
                    "price": int(card.price or 0),
                    "chance": chance_pct,
                    "media_url": _card_media_url(card),
                }
            )
    targets.sort(
        key=lambda item: (
            -int(item["chance"]),
            int(item["price"]),
        )
    )
    return targets[:UPGRADE_TARGET_LIMIT]


def _cards_display_name(user: Dict[str, object]) -> str:
    tag = str(user.get("user_tag") or "").strip()
    name = str(user.get("username") or "").strip()
    if tag:
        return f"@{tag}"
    return name or str(user.get("user_id"))


def _cards_lobby_summary(
    lobby: Dict[str, object], user_id: int
) -> Dict[str, object]:
    state = lobby.get("state") or {}
    players = state.get("players") or []
    joined = any(int(item.get("user_id", 0)) == int(user_id) for item in players)
    return {
        "lobby_id": lobby.get("lobby_id"),
        "mode": lobby.get("mode"),
        "deck_size": lobby.get("deck_size"),
        "bet_type": lobby.get("bet_type"),
        "bet_amount": lobby.get("bet_amount"),
        "owner_id": lobby.get("owner_id"),
        "status": lobby.get("status"),
        "players": len(players),
        "joined": joined,
    }


def _chess_lobby_summary(lobby: Dict[str, object], user_id: int) -> Dict[str, object]:
    state = lobby.get("state") or {}
    players = state.get("players") or []
    joined = any(int(item.get("user_id", 0)) == int(user_id) for item in players)
    return {
        "lobby_id": lobby.get("lobby_id"),
        "bet_type": lobby.get("bet_type"),
        "bet_amount": lobby.get("bet_amount"),
        "owner_id": lobby.get("owner_id"),
        "status": lobby.get("status"),
        "players": len(players),
        "joined": joined,
    }


async def _reserve_balance(conn, user_id: int, amount: int) -> bool:
    row = await conn.fetchrow(
        """
        UPDATE users
        SET balance = balance - $2, updated_at = now()
        WHERE user_id = $1 AND balance >= $2
        RETURNING balance
        """,
        int(user_id),
        int(amount),
    )
    return bool(row)


async def _refund_balance(conn, user_id: int, amount: int) -> None:
    await conn.execute(
        """
        UPDATE users
        SET balance = balance + $2, updated_at = now()
        WHERE user_id = $1
        """,
        int(user_id),
        int(amount),
    )


async def _reserve_inventory_item(
    conn, user_id: int, item_id: str
) -> Optional[Dict[str, str]]:
    row = await conn.fetchrow(
        """
        SELECT item_id, file
        FROM inventory
        WHERE user_id = $1 AND item_id = $2
        FOR UPDATE
        """,
        int(user_id),
        str(item_id),
    )
    if not row:
        return None
    await conn.execute(
        "DELETE FROM inventory WHERE user_id = $1 AND item_id = $2",
        int(user_id),
        str(item_id),
    )
    return {"item_id": row["item_id"], "file": row["file"]}


async def miniapp_cards_lobbies(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    lobbies = await list_game_lobbies(pool, CARDS_GAME)
    payload = [_cards_lobby_summary(lobby, int(user["user_id"])) for lobby in lobbies]
    current = next((item["lobby_id"] for item in payload if item.get("joined")), None)
    return web.json_response({"ok": True, "lobbies": payload, "current_lobby": current})


async def miniapp_cards_inventory(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        min_price = int(request.query.get("min_price", "0"))
    except ValueError:
        min_price = 0
    items = await list_inventory(pool, int(user["user_id"]))
    result = []
    for item in items:
        card = card_map.get(item.get("file", ""))
        if not card or card.price is None:
            continue
        price = int(card.price or 0)
        if price < min_price:
            continue
        result.append(
            {
                "id": item["id"],
                "file": card.file,
                "name": card_display_name(card),
                "rarity": card.rarity,
                "rarity_label": RARITY_NAMES.get(card.rarity, card.rarity),
                "price": price,
                "media_url": _card_media_url(card),
            }
        )
    return web.json_response({"ok": True, "items": result})


async def miniapp_cards_create(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    mode = str(payload.get("mode") or "classic")
    deck_size = int(payload.get("deck_size") or 36)
    bet_type = str(payload.get("bet_type") or "balance")
    bet_amount = int(payload.get("bet_amount") or 0)
    item_id = str(payload.get("item_id") or "")
    if mode not in {"classic", "podkidnoy", "transfer"}:
        return web.json_response({"ok": False, "error": "mode"}, status=200)
    if deck_size not in {36, 52}:
        return web.json_response({"ok": False, "error": "deck"}, status=200)
    if bet_type not in {"balance", "sausage"}:
        return web.json_response({"ok": False, "error": "bet_type"}, status=200)
    if bet_amount <= 0:
        return web.json_response({"ok": False, "error": "bet_amount"}, status=200)

    name = _cards_display_name(user)
    base_state = {"players": [{"user_id": int(user["user_id"]), "name": name}], "stakes": {}}

    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                if bet_type == "balance":
                    ok = await _reserve_balance(conn, int(user["user_id"]), bet_amount)
                    if not ok:
                        return web.json_response({"ok": False, "error": "funds"}, status=200)
                    base_state["stakes"][str(user["user_id"])] = {
                        "type": "balance",
                        "amount": bet_amount,
                    }
                else:
                    if not item_id:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    taken = await _reserve_inventory_item(conn, int(user["user_id"]), item_id)
                    if not taken:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    card = card_map.get(taken.get("file", ""))
                    if not card or card.price is None or int(card.price) < bet_amount:
                        await conn.execute(
                            "INSERT INTO inventory (item_id, user_id, file) VALUES ($1, $2, $3)",
                            str(taken["item_id"]),
                            int(user["user_id"]),
                            str(taken["file"]),
                        )
                        return web.json_response({"ok": False, "error": "item_price"}, status=200)
                    base_state["stakes"][str(user["user_id"])] = {
                        "type": "sausage",
                        "amount": bet_amount,
                        "file": card.file,
                    }
                lobby_id = await create_game_lobby(
                    pool,
                    game_type=CARDS_GAME,
                    mode=mode,
                    deck_size=deck_size,
                    bet_type=bet_type,
                    bet_amount=bet_amount,
                    owner_id=int(user["user_id"]),
                    state=base_state,
                    conn=conn,
                )
                if not lobby_id:
                    raise RuntimeError("create_failed")
        except RuntimeError:
            return web.json_response({"ok": False, "error": "create_failed"}, status=200)

    return web.json_response({"ok": True, "lobby_id": lobby_id})


async def miniapp_cards_join(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    item_id = str(payload.get("item_id") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
                if not lobby or lobby.get("game_type") != CARDS_GAME:
                    return web.json_response({"ok": False, "error": "not_found"}, status=200)
                if lobby.get("status") != "open":
                    return web.json_response({"ok": False, "error": "closed"}, status=200)
                state = lobby.get("state") or {}
                players = state.get("players") or []
                if any(int(p.get("user_id", 0)) == int(user["user_id"]) for p in players):
                    return web.json_response({"ok": True, "lobby_id": lobby_id})
                if len(players) >= CARDS_MAX_PLAYERS:
                    return web.json_response({"ok": False, "error": "full"}, status=200)
                bet_type = str(lobby.get("bet_type") or "balance")
                bet_amount = int(lobby.get("bet_amount") or 0)
                if bet_type == "balance":
                    ok = await _reserve_balance(conn, int(user["user_id"]), bet_amount)
                    if not ok:
                        return web.json_response({"ok": False, "error": "funds"}, status=200)
                    state.setdefault("stakes", {})[str(user["user_id"])] = {
                        "type": "balance",
                        "amount": bet_amount,
                    }
                else:
                    if not item_id:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    taken = await _reserve_inventory_item(conn, int(user["user_id"]), item_id)
                    if not taken:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    card = card_map.get(taken.get("file", ""))
                    if not card or card.price is None or int(card.price) < bet_amount:
                        await conn.execute(
                            "INSERT INTO inventory (item_id, user_id, file) VALUES ($1, $2, $3)",
                            str(taken["item_id"]),
                            int(user["user_id"]),
                            str(taken["file"]),
                        )
                        return web.json_response({"ok": False, "error": "item_price"}, status=200)
                    state.setdefault("stakes", {})[str(user["user_id"])] = {
                        "type": "sausage",
                        "amount": bet_amount,
                        "file": card.file,
                    }
                players.append(
                    {"user_id": int(user["user_id"]), "name": _cards_display_name(user)}
                )
                state["players"] = players
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
        except RuntimeError:
            return web.json_response({"ok": False, "error": "join_failed"}, status=200)
    return web.json_response({"ok": True, "lobby_id": lobby_id})


async def miniapp_cards_leave(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    async with pool.acquire() as conn:
        async with conn.transaction():
            lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
            if not lobby:
                return web.json_response({"ok": False, "error": "not_found"}, status=200)
            state = lobby.get("state") or {}
            players = state.get("players") or []
            stakes = state.get("stakes") or {}
            owner_id = int(lobby.get("owner_id") or 0)
            leaving = int(user["user_id"])
            players = [p for p in players if int(p.get("user_id", 0)) != leaving]
            state["players"] = players
            stake = stakes.pop(str(leaving), None)
            state["stakes"] = stakes
            if stake:
                if stake.get("type") == "balance":
                    await _refund_balance(conn, leaving, int(stake.get("amount", 0)))
                elif stake.get("type") == "sausage":
                    await add_inventory_item_safe(pool, leaving, str(stake.get("file", "")))
            if str(lobby.get("status") or "") == "open":
                if leaving == owner_id:
                    for uid, data in stakes.items():
                        if data.get("type") == "balance":
                            await _refund_balance(conn, int(uid), int(data.get("amount", 0)))
                        elif data.get("type") == "sausage":
                            await add_inventory_item_safe(pool, int(uid), str(data.get("file", "")))
                    await delete_game_lobby(pool, lobby_id, conn=conn)
                    return web.json_response({"ok": True, "closed": True})
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
                return web.json_response({"ok": True})

            if str(lobby.get("status") or "") == "active":
                for player in state.get("players", []):
                    if int(player.get("user_id", 0)) == leaving and not player.get("finished"):
                        return web.json_response({"ok": False, "error": "active"}, status=200)
                for player in state.get("players", []):
                    if int(player.get("user_id", 0)) == leaving:
                        player["finished"] = True
                apply_cards_timeout(state)
                active_players = [
                    p for p in state.get("players", []) if not p.get("finished")
                ]
                if len(active_players) <= 1:
                    winner_id = (
                        int(active_players[0].get("user_id"))
                        if active_players
                        else None
                    )
                    state["winner_id"] = winner_id
                    state["status"] = "finished"
                    bet_type = str(lobby.get("bet_type") or "balance")
                    bet_amount = int(lobby.get("bet_amount") or 0)
                    if winner_id:
                        if bet_type == "balance":
                            reward = bet_amount * max(1, len(stakes))
                            await _refund_balance(conn, int(winner_id), reward)
                        else:
                            for stake in stakes.values():
                                file_name = stake.get("file")
                                if file_name:
                                    await add_inventory_item_safe(
                                        pool, int(winner_id), str(file_name)
                                    )
                    state["settled"] = True
                    await update_game_lobby(
                        pool, lobby_id, state=state, status="finished", conn=conn
                    )
                    return web.json_response({"ok": True, "finished": True})
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
            return web.json_response({"ok": True})


async def miniapp_cards_start(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    async with pool.acquire() as conn:
        async with conn.transaction():
            lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
            if not lobby:
                return web.json_response({"ok": False, "error": "not_found"}, status=200)
            if int(lobby.get("owner_id") or 0) != int(user["user_id"]):
                return web.json_response({"ok": False, "error": "owner"}, status=200)
            if lobby.get("status") != "open":
                return web.json_response({"ok": False, "error": "started"}, status=200)
            state = lobby.get("state") or {}
            players = state.get("players") or []
            if len(players) < 2:
                return web.json_response({"ok": False, "error": "players"}, status=200)
            game_state = init_cards_game_state(
                players,
                int(lobby.get("deck_size") or 36),
                str(lobby.get("mode") or "classic"),
            )
            game_state["stakes"] = state.get("stakes") or {}
            await update_game_lobby(pool, lobby_id, state=game_state, status="active", conn=conn)
    return web.json_response({"ok": True, "lobby_id": lobby_id})


async def miniapp_cards_state(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    lobby_id = str(request.query.get("lobby_id", "") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    lobby = await get_game_lobby(pool, lobby_id)
    if not lobby:
        return web.json_response({"ok": False, "error": "not_found"}, status=200)
    state = lobby.get("state") or {}
    changed = False
    if str(lobby.get("status") or "") == "active":
        changed = apply_cards_timeout(state)
        if changed:
            await update_game_lobby(
                pool,
                lobby_id,
                state=state,
                status=str(state.get("status") or lobby.get("status")),
            )
    if str(state.get("status")) == "finished" and not state.get("settled"):
        winner_id = state.get("winner_id")
        stakes = state.get("stakes") or {}
        bet_type = str(lobby.get("bet_type") or "balance")
        bet_amount = int(lobby.get("bet_amount") or 0)
        async with pool.acquire() as conn:
            if winner_id:
                if bet_type == "balance":
                    reward = bet_amount * max(1, len(stakes))
                    await _refund_balance(conn, int(winner_id), reward)
                else:
                    for stake in stakes.values():
                        file_name = stake.get("file")
                        if file_name:
                            await add_inventory_item_safe(pool, int(winner_id), str(file_name))
        state["settled"] = True
        await update_game_lobby(
            pool,
            lobby_id,
            state=state,
            status="finished",
        )
    if str(lobby.get("status")) == "open":
        players = [
            {
                "user_id": int(p.get("user_id", 0)),
                "name": str(p.get("name") or ""),
            }
            for p in state.get("players", [])
        ]
        payload = {
            "lobby_id": lobby_id,
            "status": "open",
            "players": players,
            "owner_id": lobby.get("owner_id"),
            "bet_type": lobby.get("bet_type"),
            "bet_amount": lobby.get("bet_amount"),
            "mode": lobby.get("mode"),
            "deck_size": lobby.get("deck_size"),
        }
        return web.json_response({"ok": True, "state": payload})
    payload = serialize_cards_state(state, int(user["user_id"]))
    payload["lobby_id"] = lobby_id
    payload["status"] = lobby.get("status")
    payload["bet_type"] = lobby.get("bet_type")
    payload["bet_amount"] = lobby.get("bet_amount")
    payload["owner_id"] = lobby.get("owner_id")
    return web.json_response({"ok": True, "state": payload})


async def miniapp_cards_action(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    action = str(payload.get("action") or "")
    if not lobby_id or not action:
        return web.json_response({"ok": False, "error": "payload"}, status=200)
    async with pool.acquire() as conn:
        async with conn.transaction():
            lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
            if not lobby:
                return web.json_response({"ok": False, "error": "not_found"}, status=200)
            state = lobby.get("state") or {}
            if str(lobby.get("status") or "") == "active":
                if apply_cards_timeout(state):
                    await update_game_lobby(
                        pool,
                        lobby_id,
                        state=state,
                        status=str(state.get("status") or lobby.get("status")),
                        conn=conn,
                    )
            ok, error = apply_cards_action(state, int(user["user_id"]), action, payload)
            if not ok:
                return web.json_response({"ok": False, "error": error or "action"}, status=200)
            if state.get("status") == "finished" and not state.get("settled"):
                winner_id = state.get("winner_id")
                stakes = state.get("stakes") or {}
                bet_type = str(lobby.get("bet_type") or "balance")
                bet_amount = int(lobby.get("bet_amount") or 0)
                if winner_id:
                    if bet_type == "balance":
                        reward = bet_amount * max(1, len(stakes))
                        await _refund_balance(conn, int(winner_id), reward)
                    else:
                        for stake in stakes.values():
                            file_name = stake.get("file")
                            if file_name:
                                await add_inventory_item_safe(pool, int(winner_id), str(file_name))
                state["settled"] = True
                await update_game_lobby(
                    pool, lobby_id, state=state, status="finished", conn=conn
                )
            else:
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
    return web.json_response({"ok": True})


async def miniapp_chess_lobbies(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    lobbies = await list_game_lobbies(pool, CHESS_GAME)
    payload = [_chess_lobby_summary(lobby, int(user["user_id"])) for lobby in lobbies]
    current = next((item["lobby_id"] for item in payload if item.get("joined")), None)
    return web.json_response({"ok": True, "lobbies": payload, "current_lobby": current})


async def miniapp_chess_create(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    bet_type = str(payload.get("bet_type") or "balance")
    bet_amount = int(payload.get("bet_amount") or 0)
    item_id = str(payload.get("item_id") or "")
    if bet_type not in {"balance", "sausage"}:
        return web.json_response({"ok": False, "error": "bet_type"}, status=200)
    if bet_amount <= 0:
        return web.json_response({"ok": False, "error": "bet_amount"}, status=200)

    name = _cards_display_name(user)
    base_state = {"players": [{"user_id": int(user["user_id"]), "name": name}], "stakes": {}}

    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                if bet_type == "balance":
                    ok = await _reserve_balance(conn, int(user["user_id"]), bet_amount)
                    if not ok:
                        return web.json_response({"ok": False, "error": "funds"}, status=200)
                    base_state["stakes"][str(user["user_id"])] = {
                        "type": "balance",
                        "amount": bet_amount,
                    }
                else:
                    if not item_id:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    taken = await _reserve_inventory_item(conn, int(user["user_id"]), item_id)
                    if not taken:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    card = card_map.get(taken.get("file", ""))
                    if not card or card.price is None or int(card.price) < bet_amount:
                        await conn.execute(
                            "INSERT INTO inventory (item_id, user_id, file) VALUES ($1, $2, $3)",
                            str(taken["item_id"]),
                            int(user["user_id"]),
                            str(taken["file"]),
                        )
                        return web.json_response({"ok": False, "error": "item_price"}, status=200)
                    base_state["stakes"][str(user["user_id"])] = {
                        "type": "sausage",
                        "amount": bet_amount,
                        "file": card.file,
                    }
                lobby_id = await create_game_lobby(
                    pool,
                    game_type=CHESS_GAME,
                    mode="classic",
                    deck_size=8,
                    bet_type=bet_type,
                    bet_amount=bet_amount,
                    owner_id=int(user["user_id"]),
                    state=base_state,
                    conn=conn,
                )
                if not lobby_id:
                    raise RuntimeError("create_failed")
        except RuntimeError:
            return web.json_response({"ok": False, "error": "create_failed"}, status=200)

    return web.json_response({"ok": True, "lobby_id": lobby_id})


async def miniapp_chess_join(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    item_id = str(payload.get("item_id") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
                if not lobby or lobby.get("game_type") != CHESS_GAME:
                    return web.json_response({"ok": False, "error": "not_found"}, status=200)
                if lobby.get("status") != "open":
                    return web.json_response({"ok": False, "error": "closed"}, status=200)
                state = lobby.get("state") or {}
                players = state.get("players") or []
                if any(int(p.get("user_id", 0)) == int(user["user_id"]) for p in players):
                    return web.json_response({"ok": True, "lobby_id": lobby_id})
                if len(players) >= CHESS_MAX_PLAYERS:
                    return web.json_response({"ok": False, "error": "full"}, status=200)
                bet_type = str(lobby.get("bet_type") or "balance")
                bet_amount = int(lobby.get("bet_amount") or 0)
                if bet_type == "balance":
                    ok = await _reserve_balance(conn, int(user["user_id"]), bet_amount)
                    if not ok:
                        return web.json_response({"ok": False, "error": "funds"}, status=200)
                    state.setdefault("stakes", {})[str(user["user_id"])] = {
                        "type": "balance",
                        "amount": bet_amount,
                    }
                else:
                    if not item_id:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    taken = await _reserve_inventory_item(conn, int(user["user_id"]), item_id)
                    if not taken:
                        return web.json_response({"ok": False, "error": "item"}, status=200)
                    card = card_map.get(taken.get("file", ""))
                    if not card or card.price is None or int(card.price) < bet_amount:
                        await conn.execute(
                            "INSERT INTO inventory (item_id, user_id, file) VALUES ($1, $2, $3)",
                            str(taken["item_id"]),
                            int(user["user_id"]),
                            str(taken["file"]),
                        )
                        return web.json_response({"ok": False, "error": "item_price"}, status=200)
                    state.setdefault("stakes", {})[str(user["user_id"])] = {
                        "type": "sausage",
                        "amount": bet_amount,
                        "file": card.file,
                    }
                players.append(
                    {"user_id": int(user["user_id"]), "name": _cards_display_name(user)}
                )
                state["players"] = players
                if len(players) >= CHESS_MAX_PLAYERS:
                    game_state = init_chess_state(players)
                    game_state["stakes"] = state.get("stakes") or {}
                    await update_game_lobby(
                        pool, lobby_id, state=game_state, status="active", conn=conn
                    )
                else:
                    await update_game_lobby(pool, lobby_id, state=state, conn=conn)
        except RuntimeError:
            return web.json_response({"ok": False, "error": "join_failed"}, status=200)
    return web.json_response({"ok": True, "lobby_id": lobby_id})


async def miniapp_chess_leave(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    async with pool.acquire() as conn:
        async with conn.transaction():
            lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
            if not lobby:
                return web.json_response({"ok": False, "error": "not_found"}, status=200)
            state = lobby.get("state") or {}
            players = state.get("players") or []
            stakes = state.get("stakes") or {}
            owner_id = int(lobby.get("owner_id") or 0)
            leaving = int(user["user_id"])
            if str(lobby.get("status") or "") == "active":
                return web.json_response({"ok": False, "error": "active"}, status=200)
            players = [p for p in players if int(p.get("user_id", 0)) != leaving]
            state["players"] = players
            stake = stakes.pop(str(leaving), None)
            state["stakes"] = stakes
            if stake:
                if stake.get("type") == "balance":
                    await _refund_balance(conn, leaving, int(stake.get("amount", 0)))
                elif stake.get("type") == "sausage":
                    await add_inventory_item_safe(pool, leaving, str(stake.get("file", "")))
            if str(lobby.get("status") or "") == "open":
                if leaving == owner_id:
                    for uid, data in stakes.items():
                        if data.get("type") == "balance":
                            await _refund_balance(conn, int(uid), int(data.get("amount", 0)))
                        elif data.get("type") == "sausage":
                            await add_inventory_item_safe(pool, int(uid), str(data.get("file", "")))
                    await delete_game_lobby(pool, lobby_id, conn=conn)
                    return web.json_response({"ok": True, "closed": True})
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
                return web.json_response({"ok": True})
    return web.json_response({"ok": True})


async def miniapp_chess_state(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    lobby_id = str(request.query.get("lobby_id", "") or "")
    if not lobby_id:
        return web.json_response({"ok": False, "error": "lobby"}, status=200)
    lobby = await get_game_lobby(pool, lobby_id)
    if not lobby:
        return web.json_response({"ok": False, "error": "not_found"}, status=200)
    state = lobby.get("state") or {}
    changed = False
    if str(lobby.get("status") or "") == "active":
        changed = apply_chess_timeout(state)
        if changed:
            await update_game_lobby(
                pool,
                lobby_id,
                state=state,
                status=str(state.get("status") or lobby.get("status")),
            )
    if str(state.get("status")) == "finished" and not state.get("settled"):
        winner_id = state.get("winner_id")
        stakes = state.get("stakes") or {}
        bet_type = str(lobby.get("bet_type") or "balance")
        bet_amount = int(lobby.get("bet_amount") or 0)
        async with pool.acquire() as conn:
            if winner_id:
                if bet_type == "balance":
                    reward = bet_amount * max(1, len(stakes))
                    await _refund_balance(conn, int(winner_id), reward)
                else:
                    for stake in stakes.values():
                        file_name = stake.get("file")
                        if file_name:
                            await add_inventory_item_safe(pool, int(winner_id), str(file_name))
        state["settled"] = True
        await update_game_lobby(pool, lobby_id, state=state, status="finished")
    if str(lobby.get("status")) == "open":
        players = [
            {
                "user_id": int(p.get("user_id", 0)),
                "name": str(p.get("name") or ""),
            }
            for p in state.get("players", [])
        ]
        payload = {
            "lobby_id": lobby_id,
            "status": "open",
            "players": players,
            "owner_id": lobby.get("owner_id"),
            "bet_type": lobby.get("bet_type"),
            "bet_amount": lobby.get("bet_amount"),
        }
        return web.json_response({"ok": True, "state": payload})
    payload = serialize_chess_state(state)
    payload["lobby_id"] = lobby_id
    payload["status"] = lobby.get("status")
    payload["bet_type"] = lobby.get("bet_type")
    payload["bet_amount"] = lobby.get("bet_amount")
    payload["owner_id"] = lobby.get("owner_id")
    return web.json_response({"ok": True, "state": payload})


async def miniapp_chess_action(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    lobby_id = str(payload.get("lobby_id") or "")
    action = str(payload.get("action") or "")
    if not lobby_id or not action:
        return web.json_response({"ok": False, "error": "payload"}, status=200)
    async with pool.acquire() as conn:
        async with conn.transaction():
            lobby = await get_game_lobby(pool, lobby_id, for_update=True, conn=conn)
            if not lobby:
                return web.json_response({"ok": False, "error": "not_found"}, status=200)
            state = lobby.get("state") or {}
            if str(lobby.get("status") or "") == "active":
                if apply_chess_timeout(state):
                    await update_game_lobby(
                        pool,
                        lobby_id,
                        state=state,
                        status=str(state.get("status") or lobby.get("status")),
                        conn=conn,
                    )
            ok, error = apply_chess_action(state, int(user["user_id"]), action, payload)
            if not ok:
                return web.json_response({"ok": False, "error": error or "action"}, status=200)
            if state.get("status") == "finished" and not state.get("settled"):
                winner_id = state.get("winner_id")
                stakes = state.get("stakes") or {}
                bet_type = str(lobby.get("bet_type") or "balance")
                bet_amount = int(lobby.get("bet_amount") or 0)
                if winner_id:
                    if bet_type == "balance":
                        reward = bet_amount * max(1, len(stakes))
                        await _refund_balance(conn, int(winner_id), reward)
                    else:
                        for stake in stakes.values():
                            file_name = stake.get("file")
                            if file_name:
                                await add_inventory_item_safe(pool, int(winner_id), str(file_name))
                state["settled"] = True
                await update_game_lobby(pool, lobby_id, state=state, status="finished", conn=conn)
            else:
                await update_game_lobby(pool, lobby_id, state=state, conn=conn)
    return web.json_response({"ok": True})


async def miniapp_index(request: web.Request) -> web.Response:
    index_path = MINIAPP_DIR / "index.html"
    if not index_path.exists():
        raise web.HTTPNotFound()
    return web.FileResponse(index_path)


async def miniapp_state(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    return web.json_response({"ok": True, "state": _build_state(user)})


def _sanitize_filename(value: str) -> str:
    clean = Path(value).name
    return clean.replace("\\", "/")


async def miniapp_media(request: web.Request) -> web.Response:
    rarity = request.match_info.get("rarity", "")
    filename = _sanitize_filename(request.match_info.get("filename", ""))
    if rarity not in RARITY_DIRS:
        raise web.HTTPNotFound()
    path = SAUSAGE_DIR / RARITY_DIRS[rarity] / filename
    if not path.exists() or not path.is_file():
        raise web.HTTPNotFound()
    return web.FileResponse(path)


async def _fetch_user_for_update(conn, user_id: int) -> Dict[str, object]:
    row = await conn.fetchrow(
        "SELECT * FROM users WHERE user_id = $1 FOR UPDATE",
        int(user_id),
    )
    return dict(row) if row else {}


def _coerce_user_updates(updates: Dict[str, object]) -> Tuple[str, list]:
    keys = list(updates.keys())
    values = []
    for key in keys:
        value = updates[key]
        if key == "kazik_session" and isinstance(value, (dict, list)):
            value = json.dumps(value)
        values.append(value)
    assignments = ", ".join(f"{key} = ${index + 2}" for index, key in enumerate(keys))
    return assignments, values


async def _apply_user_updates(conn, user_id: int, updates: Dict[str, object]) -> None:
    if not updates:
        return
    assignments, values = _coerce_user_updates(updates)
    sql = f"UPDATE users SET {assignments}, updated_at = now() WHERE user_id = $1"
    await conn.execute(sql, int(user_id), *values)


async def _insert_inventory_item(conn, user_id: int, file_name: str) -> Optional[str]:
    from app.utils import make_item_id

    for _ in range(5):
        item_id = make_item_id(int(user_id))
        row = await conn.fetchrow(
            """
            INSERT INTO inventory (item_id, user_id, file)
            VALUES ($1, $2, $3)
            ON CONFLICT (item_id) DO NOTHING
            RETURNING item_id
            """,
            item_id,
            int(user_id),
            file_name,
        )
        if row:
            return item_id
    cards_logger.error(
        "Miniapp kazik insert failed. user_id=%s file=%s",
        user_id,
        file_name,
    )
    return None


async def miniapp_buy(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        spins = int(payload.get("spins", 0))
        cost = int(payload.get("cost", 0))
    except (TypeError, ValueError):
        return web.json_response({"ok": False, "error": "invalid"}, status=400)
    if not any(pack["spins"] == spins and pack["cost"] == cost for pack in KAZIK_BUY_PACKS):
        return web.json_response({"ok": False, "error": "invalid"}, status=400)

    pool = request.app.get("db_pool")
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    async with pool.acquire() as conn:
        async with conn.transaction():
            locked = await _fetch_user_for_update(conn, int(user["user_id"]))
            if not locked:
                return web.json_response({"ok": False, "error": "not_found"}, status=404)
            stars = int(locked.get("stars", 0) or 0)
            if stars < cost:
                return web.json_response({"ok": False, "error": "no_stars"}, status=200)
            bonus_spins = int(locked.get("kazik_bonus_spins", 0) or 0)
            updates = {
                "stars": stars - cost,
                "kazik_bonus_spins": bonus_spins + spins,
            }
            await _apply_user_updates(conn, int(user["user_id"]), updates)
            locked.update(updates)
    kazik_logger.info(
        "Miniapp kazik buy. user_id=%s spins=%s cost=%s stars_before=%s stars_after=%s",
        user["user_id"],
        spins,
        cost,
        stars,
        stars - cost,
    )
    return web.json_response(
        {
            "ok": True,
            "state": _build_state(locked),
            "message": f"+{spins} спинов за {cost}⭐",
        }
    )


async def miniapp_spin(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    by_rarity = request.app.get("cards_by_rarity", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)

    reward_card = None
    reward_saved = True
    item_id = None
    win_digit = None
    digits = []
    spent_free = False
    updates: Dict[str, object] = {}

    async with pool.acquire() as conn:
        async with conn.transaction():
            locked = await _fetch_user_for_update(conn, int(user["user_id"]))
            if not locked:
                return web.json_response({"ok": False, "error": "not_found"}, status=404)
            now = now_local()
            if kazik_should_reset(locked, now):
                updates["kazik_daily_used"] = 0
                updates["kazik_reset_started_at"] = None
                locked["kazik_daily_used"] = 0
                locked["kazik_reset_started_at"] = None

            bonus_spins = int(locked.get("kazik_bonus_spins", 0) or 0)
            free_rolls = int(locked.get("free_rolls", 0) or 0)
            daily_used = int(locked.get("kazik_daily_used", 0) or 0)
            daily_limit = kazik_free_spins_limit(locked)

            if bonus_spins > 0:
                bonus_spins -= 1
                updates["kazik_bonus_spins"] = bonus_spins
                spent_free = True
            elif free_rolls > 0:
                free_rolls -= 1
                updates["free_rolls"] = free_rolls
                spent_free = True
            elif daily_used < daily_limit:
                daily_used += 1
                updates["kazik_daily_used"] = daily_used
                spent_free = True

            if spent_free:
                if is_vip(locked):
                    if not locked.get("kazik_reset_started_at"):
                        updates["kazik_reset_started_at"] = now
                else:
                    updates["kazik_reset_started_at"] = now
            else:
                stars = int(locked.get("stars", 0) or 0)
                if stars < KAZIK_STAR_SPIN_COST:
                    return web.json_response({"ok": False, "error": "no_stars"}, status=200)
                updates["stars"] = stars - KAZIK_STAR_SPIN_COST
                paid_counter = int(locked.get("kazik_paid_counter", 0) or 0) + 1
                if paid_counter >= KAZIK_PAID_SPINS_FOR_BONUS:
                    batches = paid_counter // KAZIK_PAID_SPINS_FOR_BONUS
                    paid_counter = paid_counter % KAZIK_PAID_SPINS_FOR_BONUS
                    bonus_spins += batches * KAZIK_BONUS_SPINS_PER_BATCH
                    updates["kazik_bonus_spins"] = bonus_spins
                updates["kazik_paid_counter"] = paid_counter

            win_chance = _kazik_win_chance(locked)
            no_win_streak = int(locked.get("kazik_no_win_streak", 0) or 0)
            force_win = (
                KAZIK_GUARANTEE_SPINS > 0
                and no_win_streak >= KAZIK_GUARANTEE_SPINS - 1
            )
            digits = roll_kazik_digits(win_chance=1.0 if force_win else win_chance)
            win_digit = digits[0] if digits[0] == digits[1] == digits[2] else None

            if win_digit is not None:
                reward_card = _pick_kazik_reward_card(by_rarity, win_digit)
                if reward_card:
                    item_id = await _insert_inventory_item(
                        conn, int(user["user_id"]), reward_card.file
                    )
                    if not item_id:
                        reward_saved = False
                updates["kazik_no_win_streak"] = 0
            else:
                updates["kazik_no_win_streak"] = no_win_streak + 1

            await _apply_user_updates(conn, int(user["user_id"]), updates)
            locked.update(updates)

    kazik_logger.info(
        "Miniapp kazik spin. user_id=%s spent_free=%s win_digit=%s win=%s stars_delta=%s",
        user["user_id"],
        spent_free,
        win_digit,
        bool(win_digit),
        -KAZIK_STAR_SPIN_COST if not spent_free else 0,
    )

    reward_payload = None
    if win_digit is not None:
        if reward_card:
            rarity_label = RARITY_NAMES.get(reward_card.rarity, reward_card.rarity)
            base_url = MINIAPP_URL.rstrip("/") if MINIAPP_URL else "/miniapp"
            safe_file = quote(reward_card.file)
            media_url = f"{base_url}/media/{reward_card.rarity}/{safe_file}"
            media_type = "video" if Path(reward_card.file).suffix.lower() in {".mp4", ".webm"} else "image"
            reward_payload = {
                "status": "ok" if reward_saved else "save_failed",
                "name": card_display_name(reward_card),
                "rarity": reward_card.rarity,
                "rarity_label": rarity_label,
                "price": reward_card.price,
                "media_url": media_url,
                "media_type": media_type,
            }
        else:
            reward_payload = {"status": "missing"}

    return web.json_response(
        {
            "ok": True,
            "digits": digits,
            "win": bool(win_digit),
            "reward": reward_payload,
            "state": _build_state(locked),
        }
    )


async def miniapp_upgrade_inventory(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT item_id, file FROM inventory WHERE user_id = $1 ORDER BY created_at DESC",
            int(user["user_id"]),
        )
    items: list[Dict[str, object]] = []
    for row in rows:
        file_name = str(row.get("file") or "")
        card = card_map.get(file_name)
        if not _upgrade_allowed_card(card):
            continue
        items.append(
            {
                "id": str(row.get("item_id")),
                "file": card.file,
                "name": card_display_name(card),
                "rarity": card.rarity,
                "rarity_label": RARITY_NAMES.get(card.rarity, card.rarity),
                "price": int(card.price or 0),
                "media_url": _card_media_url(card),
            }
        )
    return web.json_response({"ok": True, "items": items})


async def miniapp_upgrade_targets(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    raw_items = payload.get("item_ids", []) if isinstance(payload, dict) else []
    item_ids = [str(item_id) for item_id in raw_items if item_id]
    item_ids = list(dict.fromkeys(item_ids))[:UPGRADE_MAX_ITEMS]
    try:
        min_chance = int(payload.get("filter", 75))
    except (TypeError, ValueError):
        min_chance = 75
    if min_chance not in UPGRADE_FILTERS:
        min_chance = 75
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    cards_by_rarity = request.app.get("cards_by_rarity", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    total_value = 0
    if item_ids:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT item_id, file
                FROM inventory
                WHERE user_id = $1 AND item_id = ANY($2::text[])
                """,
                int(user["user_id"]),
                item_ids,
            )
        if len(rows) != len(item_ids):
            return web.json_response(
                {"ok": False, "error": "items_missing"}, status=200
            )
        for row in rows:
            card = card_map.get(str(row.get("file") or ""))
            if not _upgrade_allowed_card(card):
                return web.json_response(
                    {"ok": False, "error": "invalid_items"}, status=200
                )
            total_value += int(card.price or 0)
    targets = _list_upgrade_targets(cards_by_rarity, total_value, min_chance)
    return web.json_response(
        {
            "ok": True,
            "total_value": total_value,
            "filter": min_chance,
            "targets": targets,
        }
    )


async def miniapp_upgrade_roll(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    raw_items = payload.get("item_ids", []) if isinstance(payload, dict) else []
    target_file = str(payload.get("target_file") or "")
    item_ids = [str(item_id) for item_id in raw_items if item_id]
    item_ids = list(dict.fromkeys(item_ids))[:UPGRADE_MAX_ITEMS]
    if not item_ids or not target_file:
        return web.json_response({"ok": False, "error": "missing"}, status=200)
    pool = request.app.get("db_pool")
    card_map = request.app.get("card_map", {})
    cards_by_rarity = request.app.get("cards_by_rarity", {})
    if not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    target_card = card_map.get(target_file)
    if not _upgrade_allowed_card(target_card):
        return web.json_response({"ok": False, "error": "invalid_target"}, status=200)
    target_value = int(target_card.price or 0)
    if target_value <= 0:
        return web.json_response({"ok": False, "error": "invalid_target"}, status=200)
    total_value = 0
    chance = 0.0
    success = False
    new_item_id = None
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                locked = await _fetch_user_for_update(conn, int(user["user_id"]))
                if not locked:
                    return web.json_response(
                        {"ok": False, "error": "not_found"}, status=404
                    )
                rows = await conn.fetch(
                    """
                    SELECT item_id, file
                    FROM inventory
                    WHERE user_id = $1 AND item_id = ANY($2::text[])
                    """,
                    int(user["user_id"]),
                    item_ids,
                )
                if len(rows) != len(item_ids):
                    return web.json_response(
                        {"ok": False, "error": "items_missing"}, status=200
                    )
                for row in rows:
                    card = card_map.get(str(row.get("file") or ""))
                    if not _upgrade_allowed_card(card):
                        return web.json_response(
                            {"ok": False, "error": "invalid_items"}, status=200
                        )
                    total_value += int(card.price or 0)
                if target_value <= total_value:
                    return web.json_response(
                        {"ok": False, "error": "invalid_target"}, status=200
                    )
                chance = _calculate_upgrade_chance(total_value, target_value)
                if chance <= 0:
                    return web.json_response(
                        {"ok": False, "error": "chance"}, status=200
                    )
                success = random.random() < chance
                deleted = await conn.fetch(
                    """
                    DELETE FROM inventory
                    WHERE user_id = $1 AND item_id = ANY($2::text[])
                    RETURNING item_id
                    """,
                    int(user["user_id"]),
                    item_ids,
                )
                if len(deleted) != len(item_ids):
                    return web.json_response(
                        {"ok": False, "error": "items_missing"}, status=200
                    )
                if success:
                    new_item_id = await _insert_inventory_item(
                        conn, int(user["user_id"]), target_card.file
                    )
                    if not new_item_id:
                        raise RuntimeError("upgrade_insert_failed")
    except RuntimeError as exc:
        if str(exc) == "upgrade_insert_failed":
            return web.json_response({"ok": False, "error": "insert_failed"}, status=500)
        raise

    return web.json_response(
        {
            "ok": True,
            "success": bool(success),
            "chance": int(round(chance * 100)),
            "total_value": total_value,
            "reward": {
                "file": target_card.file,
                "name": card_display_name(target_card),
                "rarity": target_card.rarity,
                "rarity_label": RARITY_NAMES.get(target_card.rarity, target_card.rarity),
                "price": int(target_card.price or 0),
                "media_url": _card_media_url(target_card),
            },
        }
    )


async def miniapp_open_stars(request: web.Request) -> web.Response:
    user = await _load_user(request)
    if not user:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    bot = request.app.get("bot")
    pool = request.app.get("db_pool")
    if not bot or not pool:
        return web.json_response({"ok": False, "error": "server"}, status=500)
    await send_stars_menu_to_user(bot, pool, int(user["user_id"]))
    return web.json_response({"ok": True})


def setup_miniapp(app: web.Application) -> None:
    app.router.add_get("/miniapp", miniapp_index)
    app.router.add_get("/miniapp/", miniapp_index)
    if STATIC_DIR.exists():
        app.router.add_static("/miniapp/static/", STATIC_DIR, show_index=False)
    app.router.add_get("/miniapp/media/{rarity}/{filename}", miniapp_media)
    app.router.add_get("/miniapp/api/state", miniapp_state)
    app.router.add_post("/miniapp/api/spin", miniapp_spin)
    app.router.add_post("/miniapp/api/buy", miniapp_buy)
    app.router.add_get("/miniapp/api/upgrade/inventory", miniapp_upgrade_inventory)
    app.router.add_post("/miniapp/api/upgrade/targets", miniapp_upgrade_targets)
    app.router.add_post("/miniapp/api/upgrade/roll", miniapp_upgrade_roll)
    app.router.add_get("/miniapp/api/cards/lobbies", miniapp_cards_lobbies)
    app.router.add_get("/miniapp/api/cards/inventory", miniapp_cards_inventory)
    app.router.add_post("/miniapp/api/cards/create", miniapp_cards_create)
    app.router.add_post("/miniapp/api/cards/join", miniapp_cards_join)
    app.router.add_post("/miniapp/api/cards/leave", miniapp_cards_leave)
    app.router.add_post("/miniapp/api/cards/start", miniapp_cards_start)
    app.router.add_get("/miniapp/api/cards/state", miniapp_cards_state)
    app.router.add_post("/miniapp/api/cards/action", miniapp_cards_action)
    app.router.add_get("/miniapp/api/chess/lobbies", miniapp_chess_lobbies)
    app.router.add_post("/miniapp/api/chess/create", miniapp_chess_create)
    app.router.add_post("/miniapp/api/chess/join", miniapp_chess_join)
    app.router.add_post("/miniapp/api/chess/leave", miniapp_chess_leave)
    app.router.add_get("/miniapp/api/chess/state", miniapp_chess_state)
    app.router.add_post("/miniapp/api/chess/action", miniapp_chess_action)
    app.router.add_post("/miniapp/api/open_stars", miniapp_open_stars)

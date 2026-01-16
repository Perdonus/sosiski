from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import asyncpg

from app.utils import make_item_id
from config import NEWBIE_START_BALANCE, NEWBIE_START_FREE_ROLLS


def _row_to_dict(row: Optional[asyncpg.Record]) -> Dict[str, Any]:
    return dict(row) if row else {}


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def _parse_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _coerce_date_value(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _normalize_user(row: Optional[asyncpg.Record]) -> Dict[str, Any]:
    data = _row_to_dict(row)
    if not data:
        return data
    if "kazik_session" in data:
        data["kazik_session"] = _parse_json_value(data.get("kazik_session"))
    if "contract_session" in data:
        data["contract_session"] = _parse_json_value(data.get("contract_session"))
    if "showcase_session" in data:
        data["showcase_session"] = _parse_json_value(data.get("showcase_session"))
    return data


def _normalize_lobby(row: Optional[asyncpg.Record]) -> Dict[str, Any]:
    data = _row_to_dict(row)
    if not data:
        return data
    state = data.get("state")
    if isinstance(state, str):
        try:
            data["state"] = json.loads(state)
        except Exception:
            data["state"] = {}
    return data


_cards_logger = logging.getLogger("cards")


async def get_or_create_user(
    pool: asyncpg.Pool,
    user_id: int,
    username: str,
    user_tag: str,
) -> Dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO users (user_id, username, user_tag, balance, free_rolls)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id)
            DO UPDATE SET username = EXCLUDED.username, user_tag = EXCLUDED.user_tag
            RETURNING *
            """,
            int(user_id),
            username or "",
            user_tag or "",
            int(NEWBIE_START_BALANCE),
            int(NEWBIE_START_FREE_ROLLS),
        )
    return _normalize_user(row)


async def get_user(pool: asyncpg.Pool, user_id: int) -> Dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", int(user_id))
    return _normalize_user(row)


async def update_user_fields(
    pool: asyncpg.Pool, user_id: int, fields: Dict[str, Any]
) -> None:
    if not fields:
        return
    keys = list(fields.keys())
    values = []
    for key in keys:
        val = fields[key]
        if key == "kazik_session":
            val = _coerce_json_value(val)
        elif key == "kazik_daily_date":
            val = _coerce_date_value(val)
        elif key in {"rolls_daily_date", "showcase_daily_date"}:
            val = _coerce_date_value(val)
        elif key in {"contract_session", "showcase_session"}:
            val = _coerce_json_value(val)
        values.append(val)
    assignments = ", ".join(f"{key} = ${index + 2}" for index, key in enumerate(keys))
    sql = f"UPDATE users SET {assignments}, updated_at = now() WHERE user_id = $1"
    async with pool.acquire() as conn:
        await conn.execute(sql, int(user_id), *values)


async def adjust_user_balance(
    pool: asyncpg.Pool, user_id: int, delta: int
) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET balance = balance + $2, updated_at = now()
            WHERE user_id = $1 AND balance + $2 >= 0
            RETURNING balance
            """,
            int(user_id),
            int(delta),
        )
    return int(row["balance"]) if row else None


async def adjust_user_stars(
    pool: asyncpg.Pool, user_id: int, delta: int
) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET stars = stars + $2, updated_at = now()
            WHERE user_id = $1 AND stars + $2 >= 0
            RETURNING stars
            """,
            int(user_id),
            int(delta),
        )
    return int(row["stars"]) if row else None


async def adjust_user_stars_donated(
    pool: asyncpg.Pool, user_id: int, delta: int
) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET stars_donated = stars_donated + $2, updated_at = now()
            WHERE user_id = $1 AND stars_donated + $2 >= 0
            RETURNING stars_donated
            """,
            int(user_id),
            int(delta),
        )
    return int(row["stars_donated"]) if row else None


async def adjust_user_free_rolls(
    pool: asyncpg.Pool, user_id: int, delta: int
) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE users
            SET free_rolls = GREATEST(0, free_rolls + $2), updated_at = now()
            WHERE user_id = $1
            RETURNING free_rolls
            """,
            int(user_id),
            int(delta),
        )
    return int(row["free_rolls"]) if row else None


async def add_inventory_item(
    pool: asyncpg.Pool, user_id: int, item_id: str, file_name: str
) -> bool:
    async with pool.acquire() as conn:
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
    if not row:
        _cards_logger.warning(
            "Inventory insert skipped (conflict). user_id=%s item_id=%s file=%s",
            user_id,
            item_id,
            file_name,
        )
        return False
    return True


async def add_inventory_item_safe(
    pool: asyncpg.Pool,
    user_id: int,
    file_name: str,
    *,
    attempts: int = 5,
) -> Optional[str]:
    tries = max(1, int(attempts))
    for _ in range(tries):
        item_id = make_item_id(int(user_id))
        inserted = await add_inventory_item(pool, int(user_id), item_id, str(file_name))
        if inserted:
            return item_id
    _cards_logger.error(
        "Failed to insert inventory item after retries. user_id=%s file=%s",
        user_id,
        file_name,
    )
    return None


async def remove_inventory_item(
    pool: asyncpg.Pool, user_id: int, item_id: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM inventory WHERE item_id = $1 AND user_id = $2",
            item_id,
            int(user_id),
        )


async def consume_inventory_items(
    pool: asyncpg.Pool, user_id: int, item_ids: Iterable[str]
) -> List[Dict[str, str]]:
    unique_ids = [str(item_id) for item_id in dict.fromkeys(item_ids) if item_id]
    if not unique_ids:
        return []
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                DELETE FROM inventory
                WHERE user_id = $1 AND item_id = ANY($2::text[])
                RETURNING item_id, file
                """,
                int(user_id),
                unique_ids,
            )
            if len(rows) != len(unique_ids):
                raise ValueError("Inventory items mismatch for consume.")
            return [{"id": row["item_id"], "file": row["file"]} for row in rows]


async def exchange_inventory_items(
    pool: asyncpg.Pool,
    user_id: int,
    item_ids: Iterable[str],
    new_file: str,
    *,
    attempts: int = 5,
) -> Tuple[str, List[Dict[str, str]]]:
    unique_ids = [str(item_id) for item_id in dict.fromkeys(item_ids) if item_id]
    if not unique_ids:
        raise ValueError("No items provided for exchange.")
    tries = max(1, int(attempts))
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                DELETE FROM inventory
                WHERE user_id = $1 AND item_id = ANY($2::text[])
                RETURNING item_id, file
                """,
                int(user_id),
                unique_ids,
            )
            if len(rows) != len(unique_ids):
                raise ValueError("Inventory items mismatch for exchange.")
            for _ in range(tries):
                item_id = make_item_id(int(user_id))
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO inventory (item_id, user_id, file)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (item_id) DO NOTHING
                    RETURNING item_id
                    """,
                    item_id,
                    int(user_id),
                    str(new_file),
                )
                if inserted:
                    return (
                        str(inserted["item_id"]),
                        [{"id": row["item_id"], "file": row["file"]} for row in rows],
                    )
            raise RuntimeError("Failed to insert exchange item.")


async def update_inventory_item_file(
    pool: asyncpg.Pool, user_id: int, item_id: str, file_name: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE inventory
            SET file = $3
            WHERE item_id = $1 AND user_id = $2
            """,
            str(item_id),
            int(user_id),
            str(file_name),
        )


async def update_inventory_item_file_if_current(
    pool: asyncpg.Pool,
    user_id: int,
    item_id: str,
    expected_file: str,
    new_file: str,
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE inventory
            SET file = $4
            WHERE item_id = $1 AND user_id = $2 AND file = $3
            RETURNING item_id
            """,
            str(item_id),
            int(user_id),
            str(expected_file),
            str(new_file),
        )
    return bool(row)


async def remove_inventory_item_if_current(
    pool: asyncpg.Pool,
    user_id: int,
    item_id: str,
    expected_file: str,
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM inventory
            WHERE item_id = $1 AND user_id = $2 AND file = $3
            RETURNING item_id
            """,
            str(item_id),
            int(user_id),
            str(expected_file),
        )
    return bool(row)


async def transfer_inventory_item(
    pool: asyncpg.Pool, item_id: str, from_user_id: int, to_user_id: int
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE inventory
            SET user_id = $3
            WHERE item_id = $1 AND user_id = $2
            """,
            str(item_id),
            int(from_user_id),
            int(to_user_id),
        )


async def list_inventory(pool: asyncpg.Pool, user_id: int) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT item_id, file FROM inventory WHERE user_id = $1 ORDER BY created_at",
            int(user_id),
        )
    return [{"id": row["item_id"], "item_id": row["item_id"], "file": row["file"]} for row in rows]


async def inventory_has_file(
    pool: asyncpg.Pool, user_id: int, file_name: str
) -> bool:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT 1 FROM inventory WHERE user_id = $1 AND file = $2 LIMIT 1",
            int(user_id),
            str(file_name),
        )
    return bool(value)


async def list_game_lobbies(
    pool: asyncpg.Pool,
    game_type: str,
    *,
    statuses: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    statuses = statuses or ["open", "active"]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT lobby_id, game_type, mode, deck_size, bet_type, bet_amount, owner_id,
                   status, state, created_at, updated_at
            FROM game_lobbies
            WHERE game_type = $1 AND status = ANY($2::text[])
            ORDER BY created_at
            """,
            str(game_type),
            list(statuses),
        )
    return [_normalize_lobby(row) for row in rows]


async def create_game_lobby(
    pool: asyncpg.Pool,
    *,
    game_type: str,
    mode: str,
    deck_size: int,
    bet_type: str,
    bet_amount: int,
    owner_id: int,
    state: Dict[str, Any],
    conn: Optional[asyncpg.Connection] = None,
) -> Optional[str]:
    lobby_id = make_item_id(int(owner_id))
    close_conn = False
    if conn is None:
        close_conn = True
        conn = await pool.acquire()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO game_lobbies (lobby_id, game_type, mode, deck_size, bet_type, bet_amount, owner_id, status, state)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING lobby_id
            """,
            str(lobby_id),
            str(game_type),
            str(mode),
            int(deck_size),
            str(bet_type),
            int(bet_amount),
            int(owner_id),
            "open",
            json.dumps(state, ensure_ascii=False),
        )
    finally:
        if close_conn and conn:
            await pool.release(conn)
    return str(row["lobby_id"]) if row else None


async def get_game_lobby(
    pool: asyncpg.Pool,
    lobby_id: str,
    *,
    for_update: bool = False,
    conn: Optional[asyncpg.Connection] = None,
) -> Dict[str, Any]:
    close_conn = False
    if conn is None:
        close_conn = True
        conn = await pool.acquire()
    try:
        clause = "FOR UPDATE" if for_update else ""
        row = await conn.fetchrow(
            f"""
            SELECT lobby_id, game_type, mode, deck_size, bet_type, bet_amount, owner_id,
                   status, state, created_at, updated_at
            FROM game_lobbies
            WHERE lobby_id = $1
            {clause}
            """,
            str(lobby_id),
        )
        return _normalize_lobby(row)
    finally:
        if close_conn and conn:
            await pool.release(conn)


async def update_game_lobby(
    pool: asyncpg.Pool,
    lobby_id: str,
    *,
    state: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
    conn: Optional[asyncpg.Connection] = None,
) -> None:
    close_conn = False
    if conn is None:
        close_conn = True
        conn = await pool.acquire()
    try:
        updates = []
        values: List[Any] = []
        if status is not None:
            updates.append("status = $%d" % (len(values) + 2))
            values.append(str(status))
        if state is not None:
            updates.append("state = $%d" % (len(values) + 2))
            values.append(json.dumps(state, ensure_ascii=False))
        if not updates:
            return
        updates.append("updated_at = now()")
        sql = f"UPDATE game_lobbies SET {', '.join(updates)} WHERE lobby_id = $1"
        await conn.execute(sql, str(lobby_id), *values)
    finally:
        if close_conn and conn:
            await pool.release(conn)


async def delete_game_lobby(
    pool: asyncpg.Pool, lobby_id: str, *, conn: Optional[asyncpg.Connection] = None
) -> None:
    close_conn = False
    if conn is None:
        close_conn = True
        conn = await pool.acquire()
    try:
        await conn.execute("DELETE FROM game_lobbies WHERE lobby_id = $1", str(lobby_id))
    finally:
        if close_conn and conn:
            await pool.release(conn)


async def create_showcase_card(
    pool: asyncpg.Pool,
    owner_id: int,
    rarity: str,
    effect_type: str,
    effect_value: float,
    effect_payload: Optional[Dict[str, Any]] = None,
    title: str = "",
    slot: Optional[int] = None,
) -> Optional[str]:
    for _ in range(5):
        card_id = make_item_id(int(owner_id))
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO showcase_cards
                    (card_id, owner_id, rarity, effect_type, effect_value, effect_payload, title, slot)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (card_id) DO NOTHING
                RETURNING card_id
                """,
                card_id,
                int(owner_id),
                str(rarity),
                str(effect_type),
                float(effect_value),
                _coerce_json_value(effect_payload),
                str(title),
                slot,
            )
        if row:
            return str(row["card_id"])
    return None


async def list_showcase_cards(
    pool: asyncpg.Pool, owner_id: int
) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM showcase_cards WHERE owner_id = $1 ORDER BY created_at",
            int(owner_id),
        )
    return [dict(row) for row in rows]


async def list_showcase_active_cards(
    pool: asyncpg.Pool, owner_id: int
) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM showcase_cards
            WHERE owner_id = $1 AND slot IS NOT NULL
            ORDER BY slot
            """,
            int(owner_id),
        )
    return [dict(row) for row in rows]


async def fetch_showcase_active_cards_grouped(
    pool: asyncpg.Pool,
) -> Dict[int, List[Dict[str, Any]]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT owner_id, rarity, effect_type, effect_value, effect_payload, title, slot
            FROM showcase_cards
            WHERE slot IS NOT NULL
            ORDER BY owner_id, slot
            """
        )
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        owner_id = int(row["owner_id"])
        grouped.setdefault(owner_id, []).append(dict(row))
    return grouped


async def clear_showcase_slot(pool: asyncpg.Pool, owner_id: int, slot: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE showcase_cards
            SET slot = NULL
            WHERE owner_id = $1 AND slot = $2
            """,
            int(owner_id),
            int(slot),
        )


async def set_showcase_card_slot(
    pool: asyncpg.Pool, owner_id: int, card_id: str, slot: int
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE showcase_cards
            SET slot = $3
            WHERE owner_id = $1 AND card_id = $2
            RETURNING card_id
            """,
            int(owner_id),
            str(card_id),
            int(slot),
        )
    return bool(row)


async def clear_showcase_card_slot(
    pool: asyncpg.Pool, owner_id: int, card_id: str
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE showcase_cards
            SET slot = NULL
            WHERE owner_id = $1 AND card_id = $2
            RETURNING card_id
            """,
            int(owner_id),
            str(card_id),
        )
    return bool(row)


async def list_showcase_market(pool: asyncpg.Pool) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT m.listing_id, m.card_id, m.seller_id, m.price, m.created_at,
                   c.rarity, c.effect_type, c.effect_value, c.effect_payload, c.title, c.slot
            FROM showcase_market m
            JOIN showcase_cards c ON c.card_id = m.card_id
            ORDER BY m.created_at
            """
        )
    return [dict(row) for row in rows]


async def create_showcase_listing(
    pool: asyncpg.Pool,
    seller_id: int,
    card_id: str,
    price: int,
) -> Optional[str]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            card = await conn.fetchrow(
                """
                SELECT card_id, slot
                FROM showcase_cards
                WHERE owner_id = $1 AND card_id = $2
                FOR UPDATE
                """,
                int(seller_id),
                str(card_id),
            )
            if not card:
                return None
            if card.get("slot") is not None:
                return None
            exists = await conn.fetchval(
                "SELECT 1 FROM showcase_market WHERE card_id = $1",
                str(card_id),
            )
            if exists:
                return None
            listing_id = make_item_id(int(seller_id))
            row = await conn.fetchrow(
                """
                INSERT INTO showcase_market (listing_id, card_id, seller_id, price)
                VALUES ($1, $2, $3, $4)
                RETURNING listing_id
                """,
                listing_id,
                str(card_id),
                int(seller_id),
                int(price),
            )
            return str(row["listing_id"]) if row else None


async def cancel_showcase_listing(
    pool: asyncpg.Pool, seller_id: int, listing_id: str
) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM showcase_market
            WHERE listing_id = $1 AND seller_id = $2
            RETURNING listing_id
            """,
            str(listing_id),
            int(seller_id),
        )
    return bool(row)


async def buy_showcase_listing(
    pool: asyncpg.Pool, buyer_id: int, listing_id: str
) -> Tuple[Optional[Dict[str, Any]], str]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT m.listing_id, m.card_id, m.seller_id, m.price,
                       c.rarity, c.effect_type, c.effect_value, c.effect_payload, c.title
                FROM showcase_market m
                JOIN showcase_cards c ON c.card_id = m.card_id
                WHERE m.listing_id = $1
                FOR UPDATE
                """,
                str(listing_id),
            )
            if not row:
                return None, "not_found"
            seller_id = int(row["seller_id"])
            if seller_id == int(buyer_id):
                return None, "self"
            price = int(row["price"])
            balance = await conn.fetchval(
                "SELECT balance FROM users WHERE user_id = $1 FOR UPDATE",
                int(buyer_id),
            )
            if balance is None or int(balance) < price:
                return None, "funds"
            await conn.execute(
                """
                UPDATE users
                SET balance = balance - $2, updated_at = now()
                WHERE user_id = $1
                """,
                int(buyer_id),
                price,
            )
            await conn.execute(
                """
                UPDATE users
                SET balance = balance + $2, updated_at = now()
                WHERE user_id = $1
                """,
                seller_id,
                price,
            )
            await conn.execute(
                """
                UPDATE showcase_cards
                SET owner_id = $2, slot = NULL
                WHERE card_id = $1
                """,
                str(row["card_id"]),
                int(buyer_id),
            )
            await conn.execute(
                "DELETE FROM showcase_market WHERE listing_id = $1",
                str(listing_id),
            )
            return dict(row), ""


async def count_users(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval("SELECT COUNT(*) FROM users")
    return _parse_int(value, 0)


async def fetch_all_users(pool: asyncpg.Pool) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users")
    return [_normalize_user(row) for row in rows]


async def upsert_broadcast_chat(
    pool: asyncpg.Pool,
    chat_id: int,
    chat_type: str,
    title: str = "",
    username: str = "",
    added_by: Optional[int] = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO broadcast_chats (chat_id, chat_type, title, username, added_by)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id)
            DO UPDATE SET chat_type = EXCLUDED.chat_type,
                          title = EXCLUDED.title,
                          username = EXCLUDED.username,
                          updated_at = now()
            """,
            int(chat_id),
            str(chat_type),
            str(title or ""),
            str(username or ""),
            int(added_by) if added_by is not None else None,
        )


async def fetch_broadcast_chats(
    pool: asyncpg.Pool,
    types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        if types:
            rows = await conn.fetch(
                """
                SELECT * FROM broadcast_chats
                WHERE chat_type = ANY($1::text[])
                """,
                [str(t) for t in types],
            )
        else:
            rows = await conn.fetch("SELECT * FROM broadcast_chats")
    return [dict(row) for row in rows]


async def delete_broadcast_chat(pool: asyncpg.Pool, chat_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM broadcast_chats WHERE chat_id = $1", int(chat_id)
        )


async def fetch_inventory_map(pool: asyncpg.Pool) -> Dict[int, List[Dict[str, Any]]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, item_id, file FROM inventory ORDER BY created_at"
        )
    result: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        uid = int(row["user_id"])
        result.setdefault(uid, []).append(
            {"id": row["item_id"], "file": row["file"]}
        )
    return result


async def fetch_inventory_counts(
    pool: asyncpg.Pool, user_id: int
) -> Dict[str, int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT file, COUNT(*) AS total FROM inventory WHERE user_id = $1 GROUP BY file",
            int(user_id),
        )
    return {str(row["file"]): int(row["total"]) for row in rows}


async def fetch_user_by_tag(pool: asyncpg.Pool, tag: str) -> Optional[Dict[str, Any]]:
    clean = str(tag or "").lstrip("@").strip().lower()
    if not clean:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM users WHERE LOWER(user_tag) = $1", clean
        )
    return dict(row) if row else None


async def update_last_reminder_bulk(
    pool: asyncpg.Pool, user_ids: List[int], ts
) -> None:
    if not user_ids:
        return
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET last_reminder_at = $2, updated_at = now()
            WHERE user_id = ANY($1::bigint[])
            """,
            [int(uid) for uid in user_ids],
            ts,
        )


async def create_trade(pool: asyncpg.Pool, trade: Dict[str, Any]) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trades (
                token, from_id, to_id, from_item_id, to_item_id,
                status, from_name, from_tag, to_name, to_tag
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10
            )
            ON CONFLICT (token) DO NOTHING
            """,
            str(trade.get("token")),
            int(trade.get("from_id")),
            int(trade.get("to_id")),
            trade.get("from_item_id"),
            trade.get("to_item_id"),
            str(trade.get("status") or "draft"),
            trade.get("from_name"),
            trade.get("from_tag"),
            trade.get("to_name"),
            trade.get("to_tag"),
        )


async def get_trade(pool: asyncpg.Pool, token: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM trades WHERE token = $1", str(token))
    return dict(row) if row else None


async def update_trade(pool: asyncpg.Pool, token: str, fields: Dict[str, Any]) -> None:
    if not fields:
        return
    keys = list(fields.keys())
    values = [fields[key] for key in keys]
    assignments = ", ".join(f"{key} = ${index + 2}" for index, key in enumerate(keys))
    sql = f"UPDATE trades SET {assignments} WHERE token = $1"
    async with pool.acquire() as conn:
        await conn.execute(sql, str(token), *values)


async def delete_trade(pool: asyncpg.Pool, token: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM trades WHERE token = $1", str(token))


async def get_exclusive_stock(
    pool: asyncpg.Pool, file_name: str
) -> Optional[Tuple[int, int]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT remaining, total FROM exclusive_stock WHERE file = $1",
            str(file_name),
        )
    if not row:
        return None
    return int(row["remaining"]), int(row["total"])


EXCLUSIVE_RESERVED_KV_KEY = "exclusive_reserved"


async def get_exclusive_reserved_map(pool: asyncpg.Pool) -> Dict[str, int]:
    data = await get_kv(pool, EXCLUSIVE_RESERVED_KV_KEY) or {}
    items = data.get("items", {})
    if not isinstance(items, dict):
        return {}
    reserved: Dict[str, int] = {}
    for key, value in items.items():
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        if count > 0:
            reserved[str(key)] = count
    return reserved


async def update_exclusive_reserved(
    pool: asyncpg.Pool,
    updates: Dict[str, int],
) -> Dict[str, int]:
    current = await get_exclusive_reserved_map(pool)
    for file_name, delta in updates.items():
        base = current.get(file_name, 0)
        try:
            delta_val = int(delta)
        except (TypeError, ValueError):
            delta_val = 0
        total = base + delta_val
        if total > 0:
            current[file_name] = total
        else:
            current.pop(file_name, None)
    await set_kv(pool, EXCLUSIVE_RESERVED_KV_KEY, {"items": current})
    return current


async def upsert_exclusive_stock(
    pool: asyncpg.Pool, file_name: str, total: int, remaining: int
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO exclusive_stock (file, total, remaining)
            VALUES ($1, $2, $3)
            ON CONFLICT (file) DO UPDATE SET total = EXCLUDED.total, remaining = EXCLUDED.remaining
            """,
            str(file_name),
            int(total),
            int(remaining),
        )


async def decrement_exclusive_stock(
    pool: asyncpg.Pool, file_name: str
) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE exclusive_stock
            SET remaining = remaining - 1
            WHERE file = $1 AND remaining > 0
            RETURNING remaining
            """,
            str(file_name),
        )
    return int(row["remaining"]) if row else None


async def sync_exclusive_stock(
    pool: asyncpg.Pool, exclusive_files: Iterable[str], limit: int
) -> None:
    files = [str(item) for item in exclusive_files]
    if not files:
        return
    reserved_map = await get_exclusive_reserved_map(pool)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT file, COUNT(*) AS total
            FROM inventory
            WHERE file = ANY($1::text[])
            GROUP BY file
            """,
            files,
        )
        owned = {str(row["file"]): int(row["total"]) for row in rows}
        for file_name in files:
            reserved = int(reserved_map.get(file_name, 0))
            remaining = max(0, int(limit) - owned.get(file_name, 0) - reserved)
            await conn.execute(
                """
                INSERT INTO exclusive_stock (file, total, remaining)
                VALUES ($1, $2, $3)
                ON CONFLICT (file) DO UPDATE SET total = EXCLUDED.total, remaining = EXCLUDED.remaining
                """,
                str(file_name),
                int(limit),
                int(remaining),
            )


async def get_kv(pool: asyncpg.Pool, key: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM kv_store WHERE key = $1", str(key))
    if not row:
        return None
    value = row.get("value")
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


async def set_kv(pool: asyncpg.Pool, key: str, value: Dict[str, Any]) -> None:
    async with pool.acquire() as conn:
        payload = _coerce_json_value(value)
        await conn.execute(
            """
            INSERT INTO kv_store (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            """,
            str(key),
            payload,
        )

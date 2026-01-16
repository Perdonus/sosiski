from __future__ import annotations

import json
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import asyncpg

from config import DB_PATH, DATABASE_URL


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    try:
        return date.fromisoformat(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None


def _ensure_item_id(raw: Any) -> str:
    if isinstance(raw, str) and raw.strip():
        return raw
    return f"it_{secrets.token_urlsafe(6)}"


async def create_pool() -> asyncpg.Pool:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=10)


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                user_tag TEXT NOT NULL DEFAULT '',
                balance INT NOT NULL DEFAULT 0,
                stars INT NOT NULL DEFAULT 0,
                stars_donated INT NOT NULL DEFAULT 0,
                last_roll_at TIMESTAMPTZ,
                last_kazik_at TIMESTAMPTZ,
                kazik_session JSONB,
                kazik_daily_date DATE,
                kazik_daily_used INT NOT NULL DEFAULT 0,
                kazik_bonus_spins INT NOT NULL DEFAULT 0,
                kazik_reset_started_at TIMESTAMPTZ,
                kazik_paid_counter INT NOT NULL DEFAULT 0,
                kazik_no_win_streak INT NOT NULL DEFAULT 0,
                rolls_daily_date DATE,
                rolls_daily_used INT NOT NULL DEFAULT 0,
                referred_by BIGINT,
                ref_activated BOOLEAN NOT NULL DEFAULT FALSE,
                ref_reward_count INT NOT NULL DEFAULT 0,
                vip_until TIMESTAMPTZ,
                vip BOOLEAN NOT NULL DEFAULT FALSE,
                is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                avatar_file_id TEXT,
                free_rolls INT NOT NULL DEFAULT 0,
                contract_session JSONB,
                showcase_session JSONB,
                showcase_daily_date DATE,
                last_reminder_at TIMESTAMPTZ,
                input_mode TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory (
                item_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                file TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS inventory_user_idx ON inventory(user_id);
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS kazik_reset_started_at TIMESTAMPTZ;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS stars_donated INT NOT NULL DEFAULT 0;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS kazik_paid_counter INT NOT NULL DEFAULT 0;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS kazik_no_win_streak INT NOT NULL DEFAULT 0;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS rolls_daily_date DATE;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS rolls_daily_used INT NOT NULL DEFAULT 0;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS ref_activated BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS ref_reward_count INT NOT NULL DEFAULT 0;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS avatar_file_id TEXT;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS contract_session JSONB;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS showcase_session JSONB;
            """
        )
        await conn.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS showcase_daily_date DATE;
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                token TEXT PRIMARY KEY,
                from_id BIGINT NOT NULL,
                to_id BIGINT NOT NULL,
                from_item_id TEXT,
                to_item_id TEXT,
                status TEXT NOT NULL,
                from_name TEXT,
                from_tag TEXT,
                to_name TEXT,
                to_tag TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exclusive_stock (
                file TEXT PRIMARY KEY,
                total INT NOT NULL,
                remaining INT NOT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broadcast_chats (
                chat_id BIGINT PRIMARY KEY,
                chat_type TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                added_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS game_lobbies (
                lobby_id TEXT PRIMARY KEY,
                game_type TEXT NOT NULL,
                mode TEXT NOT NULL,
                deck_size INT NOT NULL,
                bet_type TEXT NOT NULL,
                bet_amount INT NOT NULL,
                owner_id BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                state JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS game_lobbies_status_idx ON game_lobbies(status);
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS game_lobbies_type_idx ON game_lobbies(game_type);
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS showcase_cards (
                card_id TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                rarity TEXT NOT NULL,
                effect_type TEXT NOT NULL,
                effect_value DOUBLE PRECISION NOT NULL,
                effect_payload JSONB,
                title TEXT NOT NULL DEFAULT '',
                slot INT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS showcase_market (
                listing_id TEXT PRIMARY KEY,
                card_id TEXT NOT NULL REFERENCES showcase_cards(card_id) ON DELETE CASCADE,
                seller_id BIGINT NOT NULL,
                price INT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


async def migrate_from_json(pool: asyncpg.Pool, path: Optional[Path] = None) -> bool:
    if path is None:
        path = DB_PATH
    if not path.exists():
        return False
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    users = raw.get("users", {}) if isinstance(raw, dict) else {}
    trades = raw.get("trades", {}) if isinstance(raw, dict) else {}
    if not isinstance(users, dict) or not users:
        return False

    async with pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM users")
        if existing and int(existing) > 0:
            return False
        async with conn.transaction():
            for uid, data in users.items():
                if not isinstance(data, dict):
                    continue
                user_id = _parse_int(uid, 0)
                if user_id <= 0:
                    continue
                await conn.execute(
                    """
                    INSERT INTO users (
                        user_id, username, user_tag, balance, stars, stars_donated,
                        last_roll_at, last_kazik_at, kazik_session,
                        kazik_daily_date, kazik_daily_used, kazik_bonus_spins,
                        kazik_reset_started_at, kazik_paid_counter, kazik_no_win_streak,
                        referred_by, vip_until, vip, free_rolls,
                        last_reminder_at, input_mode
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, $8, $9,
                        $10, $11, $12, $13, $14, $15,
                        $16, $17, $18, $19,
                        $20, $21
                    )
                    """,
                    user_id,
                    str(data.get("username") or ""),
                    str(data.get("user_tag") or ""),
                    _parse_int(data.get("balance"), 0),
                    _parse_int(data.get("stars"), 0),
                    _parse_int(data.get("stars_donated"), 0),
                    _parse_iso(data.get("last_roll_at")),
                    _parse_iso(data.get("last_kazik_at")),
                    data.get("kazik_session"),
                    _parse_date(data.get("kazik_daily_date")),
                    _parse_int(data.get("kazik_daily_used"), 0),
                    _parse_int(data.get("kazik_bonus_spins"), 0),
                    _parse_iso(data.get("kazik_reset_started_at")),
                    _parse_int(data.get("kazik_paid_counter"), 0),
                    _parse_int(data.get("kazik_no_win_streak"), 0),
                    _parse_int(data.get("referred_by"), 0) or None,
                    _parse_iso(data.get("vip_until")),
                    _parse_bool(data.get("vip")),
                    _parse_int(data.get("free_rolls"), 0),
                    _parse_iso(data.get("last_reminder_at")),
                    data.get("input_mode"),
                )
                inventory = data.get("inventory", [])
                if isinstance(inventory, list):
                    for item in inventory:
                        if not isinstance(item, dict):
                            continue
                        file_name = item.get("file")
                        if not file_name:
                            continue
                        item_id = _ensure_item_id(item.get("id"))
                        await conn.execute(
                            """
                            INSERT INTO inventory (item_id, user_id, file)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (item_id) DO NOTHING
                            """,
                            item_id,
                            user_id,
                            str(file_name),
                        )
            if isinstance(trades, dict) and trades:
                for token, trade in trades.items():
                    if not isinstance(trade, dict):
                        continue
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
                        str(token),
                        _parse_int(trade.get("from_id"), 0),
                        _parse_int(trade.get("to_id"), 0),
                        trade.get("from_item_id"),
                        trade.get("to_item_id"),
                        str(trade.get("status") or "draft"),
                        trade.get("from_name"),
                        trade.get("from_tag"),
                        trade.get("to_name"),
                        trade.get("to_tag"),
                    )
    return True

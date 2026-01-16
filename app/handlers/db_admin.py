from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.ownership import remember_owner
from app.showcase import EFFECT_LABELS
from config import ADMIN_BROADCAST_USER_ID, RARITY_ORDER

router = Router()

PAGE_SIZE = 10

TABLES: Dict[str, Dict[str, object]] = {
    "users": {
        "label": "users",
        "pk": "user_id",
        "pk_type": "int",
        "label_columns": ["user_id", "username", "user_tag"],
        "columns": [
            "user_id",
            "username",
            "balance",
            "stars",
            "stars_donated",
            "vip",
            "vip_until",
            "is_admin",
            "avatar_file_id",
            "free_rolls",
            "last_roll_at",
            "last_kazik_at",
            "rolls_daily_date",
            "rolls_daily_used",
            "ref_activated",
            "ref_reward_count",
        ],
        "editable": [
            "username",
            "user_tag",
            "balance",
            "stars",
            "stars_donated",
            "vip",
            "vip_until",
            "is_admin",
            "avatar_file_id",
            "free_rolls",
            "last_roll_at",
            "last_kazik_at",
            "kazik_daily_used",
            "kazik_bonus_spins",
            "kazik_paid_counter",
            "kazik_no_win_streak",
            "kazik_reset_started_at",
            "kazik_daily_date",
            "kazik_session",
            "rolls_daily_date",
            "rolls_daily_used",
            "referred_by",
            "ref_activated",
            "ref_reward_count",
            "input_mode",
            "contract_session",
            "showcase_session",
            "showcase_daily_date",
        ],
    },
    "inventory": {
        "label": "inventory",
        "pk": "item_id",
        "pk_type": "text",
        "label_columns": ["item_id", "file"],
        "columns": ["item_id", "user_id", "file", "created_at"],
        "editable": ["user_id", "file"],
    },
    "trades": {
        "label": "trades",
        "pk": "token",
        "pk_type": "text",
        "label_columns": ["token", "status"],
        "columns": ["token", "from_id", "to_id", "status", "created_at"],
        "editable": [
            "from_id",
            "to_id",
            "from_item_id",
            "to_item_id",
            "status",
            "from_name",
            "from_tag",
            "to_name",
            "to_tag",
        ],
    },
    "exclusive_stock": {
        "label": "exclusive_stock",
        "pk": "file",
        "pk_type": "text",
        "label_columns": ["file", "remaining"],
        "columns": ["file", "remaining", "total"],
        "editable": ["remaining", "total"],
    },
    "kv_store": {
        "label": "kv_store",
        "pk": "key",
        "pk_type": "text",
        "label_columns": ["key"],
        "columns": ["key", "updated_at", "value"],
        "editable": ["value"],
    },
    "broadcast_chats": {
        "label": "broadcast_chats",
        "pk": "chat_id",
        "pk_type": "int",
        "label_columns": ["chat_id", "title"],
        "columns": ["chat_id", "chat_type", "title", "username", "updated_at"],
        "editable": ["chat_type", "title", "username"],
    },
    "showcase_cards": {
        "label": "showcase_cards",
        "pk": "card_id",
        "pk_type": "text",
        "label_columns": ["card_id", "rarity", "title"],
        "columns": [
            "card_id",
            "owner_id",
            "rarity",
            "effect_type",
            "effect_value",
            "title",
            "slot",
            "created_at",
        ],
        "editable": ["owner_id", "rarity", "effect_type", "effect_value", "title", "slot"],
    },
    "showcase_market": {
        "label": "showcase_market",
        "pk": "listing_id",
        "pk_type": "text",
        "label_columns": ["listing_id", "price"],
        "columns": ["listing_id", "card_id", "seller_id", "price", "created_at"],
        "editable": ["card_id", "seller_id", "price"],
    },
}

ENUM_HINTS: Dict[tuple[str, str], List[str]] = {
    ("trades", "status"): ["draft", "open", "accepting", "confirming"],
    ("broadcast_chats", "chat_type"): ["private", "group", "supergroup", "channel"],
    ("showcase_cards", "rarity"): list(RARITY_ORDER),
    ("showcase_cards", "effect_type"): sorted(EFFECT_LABELS.keys()),
    ("showcase_cards", "slot"): ["1", "2", "3", "null"],
    ("users", "input_mode"): ["support_message", "showcase_price", "stars_topup", "null"],
}

COLUMN_TYPES_CACHE: Dict[str, Dict[str, str]] = {}


class DbAdminState(StatesGroup):
    choosing_table = State()
    choosing_action = State()
    choosing_row = State()
    choosing_field = State()
    entering_value = State()
    confirming_delete = State()


def _is_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id == int(ADMIN_BROADCAST_USER_ID))


def _build_tables_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for table in TABLES.keys():
        rows.append([InlineKeyboardButton(text=table, callback_data=f"db_table|{table}")])
    rows.append([InlineKeyboardButton(text="Закрыть", callback_data="db_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Список", callback_data="db_action|list")],
            [InlineKeyboardButton(text="Изменить", callback_data="db_action|edit")],
            [InlineKeyboardButton(text="Удалить", callback_data="db_action|delete")],
            [InlineKeyboardButton(text="Назад", callback_data="db_action|back")],
        ]
    )


def _build_rows_keyboard(
    rows: List[Dict[str, Any]],
    table: str,
    action: str,
    offset: int,
    total: int,
) -> InlineKeyboardMarkup:
    meta = TABLES[table]
    pk = str(meta["pk"])
    label_columns = list(meta.get("label_columns") or [pk])
    keyboard: List[List[InlineKeyboardButton]] = []
    for row in rows:
        label = _format_row_label(row, label_columns)
        if len(label) > 60:
            label = label[:57] + "..."
        pk_value = str(row.get(pk, ""))
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"db_row|{action}|{table}|{pk_value}",
                )
            ]
        )
    nav_row: List[InlineKeyboardButton] = []
    if offset > 0:
        nav_row.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"db_page|rows|prev")
        )
    if offset + PAGE_SIZE < total:
        nav_row.append(
            InlineKeyboardButton(text="➡️", callback_data=f"db_page|rows|next")
        )
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append(
        [InlineKeyboardButton(text="Назад", callback_data="db_action|back")]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _build_fields_keyboard(table: str, pk_value: str) -> InlineKeyboardMarkup:
    meta = TABLES[table]
    editable = list(meta.get("editable") or [])
    rows: List[List[InlineKeyboardButton]] = []
    buffer: List[InlineKeyboardButton] = []
    for field in editable:
        buffer.append(
            InlineKeyboardButton(
                text=field, callback_data=f"db_field|{table}|{pk_value}|{field}"
            )
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="db_rows_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_delete_keyboard(table: str, pk_value: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Удалить", callback_data=f"db_delete|{table}|{pk_value}"
                )
            ],
            [InlineKeyboardButton(text="Назад", callback_data="db_rows_back")],
        ]
    )


def _parse_pk_value(table: str, pk_value: str) -> Any:
    pk_type = TABLES[table].get("pk_type")
    if pk_type == "int":
        return int(pk_value)
    return pk_value


def _format_value(value: Any, max_len: int = 80) -> str:
    if value is None:
        text = "NULL"
    elif isinstance(value, datetime):
        text = value.isoformat(sep=" ", timespec="seconds")
    elif isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _format_row_label(row: Dict[str, Any], label_columns: List[str]) -> str:
    parts = [f"{col}={_format_value(row.get(col), 20)}" for col in label_columns]
    return " | ".join(parts)


async def _get_column_types(db_pool, table: str) -> Dict[str, str]:
    if table in COLUMN_TYPES_CACHE:
        return COLUMN_TYPES_CACHE[table]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            """,
            table,
        )
    result = {}
    for row in rows:
        name = str(row["column_name"])
        data_type = str(row["data_type"] or row["udt_name"] or "")
        result[name] = data_type.lower()
    COLUMN_TYPES_CACHE[table] = result
    return result


def _type_hint(type_name: str) -> str:
    if not type_name:
        return "текст"
    if type_name in {"integer", "bigint", "smallint"}:
        return "целое число (пример: 1200)"
    if type_name in {"numeric", "real", "double precision"}:
        return "число (пример: 12.5)"
    if type_name in {"boolean"}:
        return "true/false"
    if type_name.startswith("timestamp"):
        return "YYYY-MM-DD HH:MM:SS"
    if type_name == "date":
        return "YYYY-MM-DD"
    if type_name in {"json", "jsonb"}:
        return "JSON (пример: {\"key\": 1})"
    return "текст"


def _parse_typed_value(raw: str, type_name: str) -> Any:
    lowered = raw.strip().lower()
    if lowered in {"null", "none"}:
        return None
    if type_name in {"boolean"}:
        if lowered in {"true", "1", "yes", "да"}:
            return True
        if lowered in {"false", "0", "no", "нет"}:
            return False
        raise ValueError("invalid_bool")
    if type_name in {"integer", "bigint", "smallint"}:
        return int(raw)
    if type_name in {"numeric", "real", "double precision"}:
        return float(raw)
    if type_name in {"json", "jsonb"}:
        return json.loads(raw)
    return raw


def _field_hint(table: str, field: str, type_name: str) -> str:
    lines = []
    allowed = ENUM_HINTS.get((table, field))
    if allowed:
        lines.append("Допустимые: " + ", ".join(allowed))
    lines.append(f"Формат: {_type_hint(type_name)}")
    lines.append("NULL: null")
    return "\n".join(lines)


async def _send_tables_menu(message: Message, owner_id: int) -> None:
    sent = await message.answer("Выбери таблицу:", reply_markup=_build_tables_keyboard())
    remember_owner(sent.chat.id, sent.message_id, owner_id)


async def _send_actions_menu(message: Message, table: str, owner_id: int) -> None:
    sent = await message.answer(
        f"Таблица: {table}\nВыбери действие:",
        reply_markup=_build_actions_keyboard(),
    )
    remember_owner(sent.chat.id, sent.message_id, owner_id)


async def _send_table_list(
    message: Message,
    db_pool,
    table: str,
    offset: int,
    owner_id: int,
) -> None:
    meta = TABLES[table]
    columns = meta["columns"]
    pk = meta["pk"]
    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        rows = await conn.fetch(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY {pk} LIMIT $1 OFFSET $2",
            PAGE_SIZE,
            offset,
        )
    lines = []
    for row in rows:
        parts = [f"{col}={_format_value(row.get(col))}" for col in columns]
        lines.append(" | ".join(parts))
    header = f"{table}: {offset + 1}-{min(offset + PAGE_SIZE, total)} из {total}"
    text = "\n".join([header, ""] + lines) if lines else f"{table}: пусто."
    nav_row: List[InlineKeyboardButton] = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data="db_page|list|prev"))
    if offset + PAGE_SIZE < total:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data="db_page|list|next"))
    keyboard: List[List[InlineKeyboardButton]] = []
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton(text="Назад", callback_data="db_action|back")])
    sent = await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    remember_owner(sent.chat.id, sent.message_id, owner_id)


async def _send_row_picker(
    message: Message,
    db_pool,
    table: str,
    offset: int,
    action: str,
    owner_id: int,
) -> None:
    meta = TABLES[table]
    pk = str(meta["pk"])
    label_columns = list(meta.get("label_columns") or [pk])
    columns = [pk] + [col for col in label_columns if col != pk]
    async with db_pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        rows = await conn.fetch(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY {pk} LIMIT $1 OFFSET $2",
            PAGE_SIZE,
            offset,
        )
    if not rows:
        sent = await message.answer("Записей нет.", reply_markup=_build_actions_keyboard())
        remember_owner(sent.chat.id, sent.message_id, owner_id)
        return
    sent = await message.answer(
        f"Выбери запись ({offset + 1}-{min(offset + PAGE_SIZE, total)} из {total}):",
        reply_markup=_build_rows_keyboard(rows, table, action, offset, total),
    )
    remember_owner(sent.chat.id, sent.message_id, owner_id)


async def _fetch_row(db_pool, table: str, pk_value: Any) -> Optional[Dict[str, Any]]:
    pk = str(TABLES[table]["pk"])
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {table} WHERE {pk} = $1 LIMIT 1",
            pk_value,
        )
    return dict(row) if row else None


@router.message(Command("bd"))
async def db_menu_command(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    await state.clear()
    await state.set_state(DbAdminState.choosing_table)
    await _send_tables_menu(message, message.from_user.id)


@router.callback_query(F.data == "db_close")
async def db_close_callback(query: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    await state.clear()
    if query.message:
        await query.message.answer("Закрыто.")
    await query.answer()


@router.callback_query(F.data.startswith("db_table|"))
async def db_table_callback(query: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, table = query.data.split("|", 1)
    if table not in TABLES:
        await query.answer("Неизвестная таблица.", show_alert=True)
        return
    await state.update_data(table=table, offset=0, action=None)
    await state.set_state(DbAdminState.choosing_action)
    await _send_actions_menu(query.message, table, query.from_user.id)
    await query.answer()


@router.callback_query(F.data.startswith("db_action|"))
async def db_action_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, action = query.data.split("|", 1)
    data = await state.get_data()
    table = data.get("table")
    if not table or table not in TABLES:
        await query.answer("Сначала выбери таблицу.", show_alert=True)
        return
    if action == "back":
        await state.set_state(DbAdminState.choosing_table)
        await state.update_data(table=None, offset=0, action=None)
        await _send_tables_menu(query.message, query.from_user.id)
        await query.answer()
        return
    if action == "list":
        await state.update_data(offset=0, action=action, view="list")
        await _send_table_list(query.message, db_pool, table, 0, query.from_user.id)
        await query.answer()
        return
    if action in {"edit", "delete"}:
        await state.update_data(offset=0, action=action, view="rows")
        await state.set_state(DbAdminState.choosing_row)
        await _send_row_picker(query.message, db_pool, table, 0, action, query.from_user.id)
        await query.answer()
        return
    await query.answer()


@router.callback_query(F.data.startswith("db_page|"))
async def db_page_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, mode, direction = query.data.split("|", 2)
    data = await state.get_data()
    table = data.get("table")
    offset = int(data.get("offset", 0) or 0)
    action = data.get("action") or "edit"
    if not table or table not in TABLES:
        await query.answer("Сначала выбери таблицу.", show_alert=True)
        return
    if direction == "next":
        offset += PAGE_SIZE
    elif direction == "prev":
        offset = max(0, offset - PAGE_SIZE)
    await state.update_data(offset=offset)
    if mode == "list":
        await _send_table_list(query.message, db_pool, table, offset, query.from_user.id)
    else:
        await _send_row_picker(query.message, db_pool, table, offset, str(action), query.from_user.id)
    await query.answer()


@router.callback_query(F.data == "db_rows_back")
async def db_rows_back_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    data = await state.get_data()
    table = data.get("table")
    action = data.get("action") or "edit"
    offset = int(data.get("offset", 0) or 0)
    if not table or table not in TABLES:
        await query.answer("Сначала выбери таблицу.", show_alert=True)
        return
    await state.set_state(DbAdminState.choosing_row)
    await _send_row_picker(query.message, db_pool, table, offset, str(action), query.from_user.id)
    await query.answer()


@router.callback_query(F.data.startswith("db_row|"))
async def db_row_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    parts = query.data.split("|", 3)
    if len(parts) < 4:
        await query.answer()
        return
    _, action, table, pk_raw = parts
    if table not in TABLES:
        await query.answer("Неизвестная таблица.", show_alert=True)
        return
    try:
        pk_value = _parse_pk_value(table, pk_raw)
    except ValueError:
        await query.answer("Неверный ключ.", show_alert=True)
        return
    row = await _fetch_row(db_pool, table, pk_value)
    if not row:
        await query.answer("Запись не найдена.", show_alert=True)
        return
    await state.update_data(pk_value=pk_value)
    if action == "delete":
        await state.set_state(DbAdminState.confirming_delete)
        sent = await query.message.answer(
            f"Удалить запись {TABLES[table]['pk']}={pk_raw}?",
            reply_markup=_build_delete_keyboard(table, pk_raw),
        )
        remember_owner(sent.chat.id, sent.message_id, query.from_user.id)
        await query.answer()
        return
    await state.set_state(DbAdminState.choosing_field)
    sent = await query.message.answer(
        "Выбери поле для редактирования:",
        reply_markup=_build_fields_keyboard(table, pk_raw),
    )
    remember_owner(sent.chat.id, sent.message_id, query.from_user.id)
    await query.answer()


@router.callback_query(F.data.startswith("db_field|"))
async def db_field_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    parts = query.data.split("|", 3)
    if len(parts) < 4:
        await query.answer()
        return
    _, table, pk_raw, field = parts
    data = await state.get_data()
    if table not in TABLES:
        await query.answer("Неизвестная таблица.", show_alert=True)
        return
    if field not in TABLES[table].get("editable", []):
        await query.answer("Поле нельзя менять.", show_alert=True)
        return
    types = await _get_column_types(db_pool, table)
    type_name = types.get(field, "")
    pk_value = data.get("pk_value")
    current_row = await _fetch_row(db_pool, table, pk_value)
    current_value = _format_value(current_row.get(field) if current_row else None)
    hint = _field_hint(table, field, type_name)
    await state.update_data(field=field, table=table)
    await state.set_state(DbAdminState.entering_value)
    await query.message.answer(
        "\n".join(
            [
                f"Поле: {field}",
                f"Текущее: {current_value}",
                hint,
                "Введи новое значение или «отмена».",
            ]
        )
    )
    await query.answer()


@router.callback_query(F.data.startswith("db_delete|"))
async def db_delete_callback(
    query: CallbackQuery,
    state: FSMContext,
    db_pool,
) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        return
    if not query.message:
        return
    _, table, pk_raw = query.data.split("|", 2)
    if table not in TABLES:
        await query.answer("Неизвестная таблица.", show_alert=True)
        return
    try:
        pk_value = _parse_pk_value(table, pk_raw)
    except ValueError:
        await query.answer("Неверный ключ.", show_alert=True)
        return
    pk = TABLES[table]["pk"]
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            f"DELETE FROM {table} WHERE {pk} = $1",
            pk_value,
        )
    deleted = result.split()[-1] if result else "0"
    await state.set_state(DbAdminState.choosing_action)
    if deleted == "0":
        await query.message.answer("Запись не найдена.")
    else:
        await query.message.answer("Запись удалена.")
    await _send_actions_menu(query.message, table, query.from_user.id)
    await query.answer()


@router.message(DbAdminState.entering_value)
async def db_enter_value(message: Message, state: FSMContext, db_pool) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return
    text = (message.text or "").strip()
    if text.lower() in {"отмена", "cancel"}:
        data = await state.get_data()
        table = data.get("table")
        pk_value = data.get("pk_value")
        if table and pk_value is not None:
            await state.set_state(DbAdminState.choosing_field)
            sent = await message.answer(
                "Выбери поле для редактирования:",
                reply_markup=_build_fields_keyboard(table, str(pk_value)),
            )
            remember_owner(sent.chat.id, sent.message_id, message.from_user.id)
        return
    data = await state.get_data()
    table = data.get("table")
    pk_value = data.get("pk_value")
    field = data.get("field")
    if not table or table not in TABLES or field is None:
        await message.answer("Сначала выбери таблицу.")
        return
    types = await _get_column_types(db_pool, table)
    type_name = types.get(field, "")
    allowed = ENUM_HINTS.get((table, field))
    lowered = text.strip().lower()
    if allowed and lowered not in {item.lower() for item in allowed} and lowered not in {
        "null",
        "none",
    }:
        await message.answer("Допустимые: " + ", ".join(allowed))
        return
    try:
        value = _parse_typed_value(text, type_name)
    except (ValueError, json.JSONDecodeError):
        await message.answer(f"Неверный формат. {_type_hint(type_name)}")
        return
    pk = TABLES[table]["pk"]
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            f"UPDATE {table} SET {field} = $2 WHERE {pk} = $1",
            pk_value,
            value,
        )
    updated = result.split()[-1] if result else "0"
    await state.set_state(DbAdminState.choosing_action)
    if updated == "0":
        await message.answer("Запись не найдена.")
    else:
        await message.answer("Запись обновлена.")
    await _send_actions_menu(message, table, message.from_user.id)

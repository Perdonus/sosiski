from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from config import (
    CONTRACT_REQUIRED_COUNT,
    MINIAPP_URL,
    PUBLIC_BOT_USERNAME,
    RARITY_NAMES,
    RARITY_ORDER,
    SHOP_RARITY_ORDER,
    SHOWCASE_CRAFT_COUNT,
    SHOWCASE_MAX_ACTIVE,
    STARS_TOPUP_AMOUNTS,
)


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Крутки", callback_data="roll_menu")],
            [InlineKeyboardButton(text="Игры", callback_data="cmd|games")],
            [InlineKeyboardButton(text="Сосиски", callback_data="sausages_menu")],
            [InlineKeyboardButton(text="Донат", callback_data="donate_menu")],
            [InlineKeyboardButton(text="Топ", callback_data="cmd|top")],
        ]
    )


def build_back_keyboard(callback_data: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=callback_data)]]
    )


def build_roll_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Обычная", callback_data="cmd|sosiska")],
            [InlineKeyboardButton(text="Назад", callback_data="menu")],
        ]
    )


def build_sausages_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Мои", callback_data="cmd|my"),
                InlineKeyboardButton(text="Купить", callback_data="cmd|shop"),
            ],
            [InlineKeyboardButton(text="Апгрейд", callback_data="cmd|upgrade_web")],
            [InlineKeyboardButton(text="Витрина", callback_data="cmd|showcase")],
            [InlineKeyboardButton(text="Трейд", callback_data="cmd|trade")],
            [InlineKeyboardButton(text="Назад", callback_data="menu")],
        ]
    )


def build_showcase_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Моя витрина", callback_data="showcase_view")],
            [InlineKeyboardButton(text="Мои карты", callback_data="showcase_cards")],
            [InlineKeyboardButton(text="Создать карту", callback_data="showcase_craft_menu")],
            [InlineKeyboardButton(text="Маркет", callback_data="showcase_market")],
            [InlineKeyboardButton(text="Назад", callback_data="sausages_menu")],
        ]
    )


def build_donate_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="VIP", callback_data="donate_vip"),
                InlineKeyboardButton(text="Звёзды", callback_data="donate_stars"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="menu")],
        ]
    )


def build_donate_stars_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Пополнить", callback_data="donate_stars_topup")],
            [InlineKeyboardButton(text="Назад", callback_data="menu")],
        ]
    )


def build_rarity_keyboard(
    prefix: str,
    include_menu: bool = True,
    rarities: Optional[List[str]] = None,
    counts: Optional[Dict[str, int]] = None,
    back_callback: str = "menu",
) -> InlineKeyboardMarkup:
    rows = []
    buffer = []
    if rarities is None:
        rarities = list(RARITY_ORDER)
    for rarity in rarities:
        label = RARITY_NAMES.get(rarity, rarity)
        if counts is not None:
            label = f"{label} ({counts.get(rarity, 0)})"
        buffer.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"{prefix}|{rarity}",
            )
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    if include_menu:
        rows.append([InlineKeyboardButton(text="Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_kazik_spin_keyboard(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data="kazik_spin")],
            [InlineKeyboardButton(text="Купить спины", callback_data="kazik_buy_menu")],
            [InlineKeyboardButton(text="Назад", callback_data="menu")],
        ]
    )


def build_kazik_buy_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 крутка за 1⭐", callback_data="kazik_buy|1|1"),
                InlineKeyboardButton(text="5 круток за 4⭐", callback_data="kazik_buy|5|4"),
            ],
            [
                InlineKeyboardButton(text="10 круток за 7⭐", callback_data="kazik_buy|10|7"),
                InlineKeyboardButton(text="15 круток за 11⭐", callback_data="kazik_buy|15|11"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="cmd|kazik")],
        ]
    )


def build_kazik_webapp_keyboard() -> InlineKeyboardMarkup:
    if MINIAPP_URL:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Открыть игры",
                        web_app=WebAppInfo(url=MINIAPP_URL),
                    )
                ]
            ]
        )
    username = (PUBLIC_BOT_USERNAME or "").lstrip("@")
    url = f"https://t.me/{username}" if username else "https://t.me"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть игры", url=url)]]
    )


def build_kazik_open_dm_keyboard() -> InlineKeyboardMarkup:
    username = (PUBLIC_BOT_USERNAME or "").lstrip("@")
    url = f"https://t.me/{username}" if username else "https://t.me"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть игры", url=url)]]
    )


def build_giveaway_date_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сегодня", callback_data="gw_date|today"),
                InlineKeyboardButton(text="Завтра", callback_data="gw_date|tomorrow"),
            ],
            [InlineKeyboardButton(text="Ввести дату", callback_data="gw_date|custom")],
            [InlineKeyboardButton(text="Отмена", callback_data="gw_cancel")],
        ]
    )


def build_giveaway_places_keyboard(
    selected_places: Optional[List[int]] = None,
) -> InlineKeyboardMarkup:
    selected = set(selected_places or [])
    rows: List[List[InlineKeyboardButton]] = []
    buffer: List[InlineKeyboardButton] = []
    for place in range(1, 11):
        label = f"{place} место"
        if place in selected:
            label = f"{label} ✅"
        buffer.append(
            InlineKeyboardButton(text=label, callback_data=f"gw_place|{place}")
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    rows.append(
        [
            InlineKeyboardButton(text="Готово", callback_data="gw_done"),
            InlineKeyboardButton(text="Отмена", callback_data="gw_cancel"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_giveaway_place_type_keyboard(place: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сосиски", callback_data=f"gw_type|{place}|card"),
                InlineKeyboardButton(text="Балик", callback_data=f"gw_type|{place}|balance"),
            ],
            [
                InlineKeyboardButton(text="Фри спины", callback_data=f"gw_type|{place}|free"),
                InlineKeyboardButton(text="Випка", callback_data=f"gw_type|{place}|vip"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="gw_back_main")],
        ]
    )


def build_giveaway_vip_duration_keyboard(place: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="3 дня", callback_data=f"gw_vip|{place}|3"),
                InlineKeyboardButton(text="1 неделя", callback_data=f"gw_vip|{place}|7"),
            ],
            [
                InlineKeyboardButton(text="2 недели", callback_data=f"gw_vip|{place}|14"),
                InlineKeyboardButton(text="1 месяц", callback_data=f"gw_vip|{place}|30"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data=f"gw_place|{place}")],
        ]
    )


def build_giveaway_card_nav_keyboard(
    place: int,
    rarity: str,
    index: int,
    total: int,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    text="<", callback_data=f"gw_card_nav|{place}|{rarity}|{prev_index}"
                ),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(
                    text=">", callback_data=f"gw_card_nav|{place}|{rarity}|{next_index}"
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="Выбрать", callback_data=f"gw_card_pick|{place}|{rarity}|{index}"
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"gw_rarity_menu|{place}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_giveaway_delete_keyboard(date_key: str, target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Удалить",
                    callback_data=f"gw_delete|{date_key}|{target}",
                ),
                InlineKeyboardButton(text="Отмена", callback_data="gw_delete_cancel"),
            ]
        ]
    )


def build_shop_menu_keyboard(
    counts: Optional[Dict[str, int]] = None,
) -> InlineKeyboardMarkup:
    return build_rarity_keyboard(
        "shop_rarity",
        include_menu=True,
        rarities=list(SHOP_RARITY_ORDER),
        counts=counts,
    )


def build_inventory_keyboard(
    rarity: str,
    index: int,
    total: int,
    item_id: str,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(text="<", callback_data=f"my_nav|{rarity}|{prev_index}"),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(text=">", callback_data=f"my_nav|{rarity}|{next_index}"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(text="Продать", callback_data=f"my_sell|{item_id}|{rarity}|{index}"),
            InlineKeyboardButton(
                text="Контракт",
                callback_data=f"my_upgrade|{item_id}|{rarity}|{index}",
            ),
        ]
    )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="my_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_shop_keyboard(
    rarity: str,
    index: int,
    total: int,
    *,
    allow_buy: bool = True,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(text="<", callback_data=f"shop_nav|{rarity}|{prev_index}"),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(text=">", callback_data=f"shop_nav|{rarity}|{next_index}"),
            ]
        )
    if allow_buy:
        rows.append(
            [InlineKeyboardButton(text="Купить", callback_data=f"shop_buy|{rarity}|{index}")]
        )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="shop_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_draw_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Продать", callback_data=f"draw_sell|{item_id}"),
                InlineKeyboardButton(
                    text="Контракт",
                    callback_data=f"draw_upgrade|{item_id}",
                ),
            ]
        ]
    )


def build_contract_keyboard(
    rarity: str,
    index: int,
    total: int,
    item_id: str,
    *,
    selected_count: int,
    selected: bool,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    text="<", callback_data=f"contract_nav|{rarity}|{prev_index}"
                ),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(
                    text=">", callback_data=f"contract_nav|{rarity}|{next_index}"
                ),
            ]
        )
    toggle_text = "Убрать" if selected else "Выбрать"
    rows.append(
        [
            InlineKeyboardButton(
                text=toggle_text, callback_data=f"contract_pick|{item_id}|{rarity}|{index}"
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=f"Сделать контракт ({selected_count}/{CONTRACT_REQUIRED_COUNT})",
                callback_data=f"contract_confirm|{rarity}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="Сбросить выбор", callback_data=f"contract_clear|{rarity}|{index}"
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="my_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_showcase_cards_keyboard(
    index: int,
    total: int,
    card_id: str,
    *,
    slot: Optional[int] = None,
    listing_id: Optional[str] = None,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(text="<", callback_data=f"showcase_cards_nav|{prev_index}"),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(text=">", callback_data=f"showcase_cards_nav|{next_index}"),
            ]
        )
    if slot is None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Поставить в витрину",
                    callback_data=f"showcase_slot_menu|{card_id}|{index}",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"Снять (слот {slot})",
                    callback_data=f"showcase_slot_clear|{card_id}|{index}",
                )
            ]
        )
    if listing_id:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Снять с маркета",
                    callback_data=f"showcase_unlist_card|{listing_id}|{index}",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Выставить на маркет",
                    callback_data=f"showcase_list|{card_id}|{index}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="showcase_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_showcase_slot_keyboard(
    card_id: str,
    occupied_slots: List[int],
    *,
    index: int,
) -> InlineKeyboardMarkup:
    occupied = set(occupied_slots)
    row: List[InlineKeyboardButton] = []
    rows: List[List[InlineKeyboardButton]] = []
    for slot in range(1, SHOWCASE_MAX_ACTIVE + 1):
        label = f"Слот {slot}"
        if slot in occupied:
            label = f"{label} (занят)"
        row.append(
            InlineKeyboardButton(
                text=label, callback_data=f"showcase_slot_set|{card_id}|{slot}|{index}"
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"showcase_cards_nav|{index}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_showcase_market_keyboard(
    index: int,
    total: int,
    listing_id: str,
    *,
    is_owner: bool,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(text="<", callback_data=f"showcase_market_nav|{prev_index}"),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(text=">", callback_data=f"showcase_market_nav|{next_index}"),
            ]
        )
    if is_owner:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Снять с маркета",
                    callback_data=f"showcase_unlist_market|{listing_id}|{index}",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Купить",
                    callback_data=f"showcase_buy|{listing_id}|{index}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="showcase_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_showcase_craft_keyboard(
    rarity: str,
    index: int,
    total: int,
    item_id: str,
    *,
    selected_count: int,
    selected: bool,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    text="<", callback_data=f"showcase_craft_nav|{rarity}|{prev_index}"
                ),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(
                    text=">", callback_data=f"showcase_craft_nav|{rarity}|{next_index}"
                ),
            ]
        )
    toggle_text = "Убрать" if selected else "Выбрать"
    rows.append(
        [
            InlineKeyboardButton(
                text=toggle_text,
                callback_data=f"showcase_craft_pick|{item_id}|{rarity}|{index}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=f"Создать карту ({selected_count}/{SHOWCASE_CRAFT_COUNT})",
                callback_data=f"showcase_craft_confirm|{rarity}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="Сбросить выбор", callback_data=f"showcase_craft_clear|{rarity}|{index}"
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="Назад", callback_data="showcase_craft_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_draw_sell_confirm_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, продать", callback_data=f"draw_sell_confirm|{item_id}"
                ),
                InlineKeyboardButton(
                    text="Отмена", callback_data=f"draw_sell_cancel|{item_id}"
                ),
            ]
        ]
    )


def build_my_sell_confirm_keyboard(
    item_id: str, rarity: str, index: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, продать",
                    callback_data=f"my_sell_confirm|{item_id}|{rarity}|{index}",
                ),
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"my_sell_cancel|{item_id}|{rarity}|{index}",
                ),
            ]
        ]
    )


def build_upgrade_confirm_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, повысить", callback_data=f"draw_upgrade_confirm|{item_id}"
                ),
                InlineKeyboardButton(
                    text="Отмена", callback_data=f"draw_upgrade_cancel|{item_id}"
                ),
            ]
        ]
    )


def build_my_upgrade_confirm_keyboard(
    item_id: str, rarity: str, index: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, повысить",
                    callback_data=f"my_upgrade_confirm|{item_id}|{rarity}|{index}",
                ),
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"my_upgrade_cancel|{item_id}|{rarity}|{index}",
                ),
            ]
        ]
    )


def build_stars_menu_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for amount in STARS_TOPUP_AMOUNTS:
        row.append(
            InlineKeyboardButton(text=f"{amount}⭐", callback_data=f"stars_buy|{amount}")
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="donate_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_vip_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Оплатить звёздами", callback_data="vip_buy_stars"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="donate_menu")],
        ]
    )


def build_trade_rarity_keyboard(token: str, role: str) -> InlineKeyboardMarkup:
    rows = []
    buffer: List[InlineKeyboardButton] = []
    for rarity in RARITY_ORDER:
        buffer.append(
            InlineKeyboardButton(
                text=RARITY_NAMES.get(rarity, rarity),
                callback_data=f"trade_rarity|{role}|{token}|{rarity}",
            )
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    if role == "offer":
        rows.append(
            [
                InlineKeyboardButton(
                    text="Отменить трейд", callback_data=f"trade_cancel|{token}"
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Отказаться", callback_data=f"trade_decline|{token}"
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="Ничего не выбирать",
                    callback_data=f"trade_accept_none|{token}",
                )
            ]
        )
    rows.append(
        [InlineKeyboardButton(text="Назад", callback_data=f"trade_rarity_menu|{role}|{token}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_trade_accept_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Принять трейд", callback_data=f"trade_accept|{token}")],
        ]
    )


def build_trade_confirm_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Подтвердить", callback_data=f"trade_confirm|{token}"),
                InlineKeyboardButton(text="Отмена", callback_data=f"trade_confirm_cancel|{token}"),
            ]
        ]
    )


def build_trade_item_keyboard(
    token: str,
    role: str,
    rarity: str,
    index: int,
    total: int,
    item_id: str,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    text="<", callback_data=f"trade_nav|{role}|{token}|{rarity}|{prev_index}"
                ),
                InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(
                    text=">", callback_data=f"trade_nav|{role}|{token}|{rarity}|{next_index}"
                ),
            ]
        )
    action_label = "Предложить трейд" if role == "offer" else "Обменять"
    rows.append(
        [
            InlineKeyboardButton(
                text=action_label,
                callback_data=f"trade_pick|{role}|{token}|{item_id}|{rarity}|{index}",
            )
        ]
    )
    rows.append(
        [InlineKeyboardButton(text="Назад", callback_data=f"trade_rarity_menu|{role}|{token}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)

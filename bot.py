import asyncio
import json
import logging
import os
import random
import secrets
import time
import fcntl
import html
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import deque
from io import BytesIO
from pathlib import Path
from typing import Callable, Deque, Dict, List, Optional, Tuple, Union

from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps, ImageStat, ImageSequence
from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAnimation,
    InputMediaPhoto,
    InputMediaVideo,
    LabeledPrice,
    Update,
)
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.ext import (
    ApplicationBuilder,
    BaseRateLimiter,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

from cards import (
    Card,
    calc_sale_price,
    card_currency,
    card_display_name,
    card_file_path,
    compute_default_drop_chances,
    filter_existing_cards,
    format_amount,
    format_card_price,
    format_card_sale_price,
    format_drop_chance,
    format_price,
    format_stars,
    load_drop_chances,
    merge_cards,
    parse_drop_chance,
    parse_price,
    pick_random_card,
    scan_card_files,
    build_card_index,
)
from config import *  # noqa: F403
from storage import (
    compute_leaderboard,
    compute_rank,
    ensure_user,
    find_inventory_item,
    find_user_by_tag,
    get_balance,
    get_cooldown_seconds,
    get_kazik_spin_cost,
    get_kazik_win_chance,
    get_star_balance,
    get_user_label,
    inventory_value,
    is_vip,
    load_db,
    make_inventory_item,
    now_utc,
    parse_iso,
    save_db,
    sync_exclusive_stock,
    total_wealth,
)

from font_setup import ensure_fonts, ensure_utf8


class SlidingWindowLimiter:
    def __init__(self, max_rate: int, time_period: float) -> None:
        self._max_rate = max_rate
        self._time_period = time_period
        self._timestamps: Deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self._max_rate <= 0 or self._time_period <= 0:
            return
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                while self._timestamps and now - self._timestamps[0] >= self._time_period:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._max_rate:
                    self._timestamps.append(now)
                    return
                sleep_for = self._time_period - (now - self._timestamps[0])
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                await asyncio.sleep(0)


class SimpleRateLimiter(BaseRateLimiter[int]):
    def __init__(
        self,
        overall_max_rate: int,
        overall_time_period: float,
        group_max_rate: int,
        group_time_period: float,
        max_retries: int,
        min_delay_sec: float = 0.0,
    ) -> None:
        self._overall_limiter = (
            SlidingWindowLimiter(overall_max_rate, overall_time_period)
            if overall_max_rate and overall_time_period
            else None
        )
        self._group_max_rate = group_max_rate
        self._group_time_period = group_time_period
        self._group_limiters: Dict[Union[str, int], SlidingWindowLimiter] = {}
        self._max_retries = max_retries
        self._retry_after_until = 0.0
        self._retry_after_lock = asyncio.Lock()
        self._min_delay = max(0.0, float(min_delay_sec))
        self._min_delay_lock = asyncio.Lock()
        self._min_delay_until = 0.0

    async def initialize(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    def _get_group_limiter(self, group_id: Union[str, int]) -> SlidingWindowLimiter:
        limiter = self._group_limiters.get(group_id)
        if limiter is None:
            limiter = SlidingWindowLimiter(
                self._group_max_rate, self._group_time_period
            )
            self._group_limiters[group_id] = limiter
        return limiter

    async def _wait_for_retry_after(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            async with self._retry_after_lock:
                wait_for = self._retry_after_until - loop.time()
            if wait_for <= 0:
                return
            await asyncio.sleep(wait_for)

    async def _register_retry_after(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        async with self._retry_after_lock:
            until = loop.time() + delay + 0.1
            if until > self._retry_after_until:
                self._retry_after_until = until

    async def _wait_for_min_delay(self) -> None:
        if self._min_delay <= 0:
            return
        loop = asyncio.get_running_loop()
        while True:
            async with self._min_delay_lock:
                now = loop.time()
                if now >= self._min_delay_until:
                    self._min_delay_until = now + self._min_delay
                    return
                wait_for = self._min_delay_until - now
            await asyncio.sleep(wait_for)

    async def process_request(
        self,
        callback: Callable,
        args: object,
        kwargs: Dict[str, object],
        endpoint: str,
        data: Dict[str, object],
        rate_limit_args: Optional[int],
    ) -> object:
        max_retries = (
            rate_limit_args if rate_limit_args is not None else self._max_retries
        )
        chat_id = data.get("chat_id")
        group_id: Union[str, int, bool] = False
        if chat_id is not None:
            chat_value: Union[str, int, object] = chat_id
            try:
                chat_value = int(chat_value)
            except (TypeError, ValueError):
                pass
            if (isinstance(chat_value, int) and chat_value < 0) or isinstance(
                chat_value, str
            ):
                group_id = chat_value

        for attempt in range(max_retries + 1):
            await self._wait_for_retry_after()
            if group_id and self._group_max_rate:
                await self._get_group_limiter(group_id).acquire()
            if chat_id is not None and self._overall_limiter:
                await self._overall_limiter.acquire()
            await self._wait_for_min_delay()
            try:
                return await callback(*args, **kwargs)
            except RetryAfter as exc:
                await self._register_retry_after(exc.retry_after)
                if attempt >= max_retries:
                    raise
        return None



def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours}\u0447")
    if minutes:
        parts.append(f"{minutes}\u043c")
    if secs or not parts:
        parts.append(f"{secs}\u0441")
    return " ".join(parts)


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


def greeting_by_time(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    hour = now.hour
    if 5 <= hour < 12:
        return "\u0414\u043e\u0431\u0440\u043e\u0435 \u0443\u0442\u0440\u043e"
    if 12 <= hour < 18:
        return "\u0414\u043e\u0431\u0440\u044b\u0439 \u0434\u0435\u043d\u044c"
    if 18 <= hour < 23:
        return "\u0414\u043e\u0431\u0440\u044b\u0439 \u0432\u0435\u0447\u0435\u0440"
    return "\u0414\u043e\u0431\u0440\u043e\u0439 \u043d\u043e\u0447\u0438"


def get_public_bot_username(context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> Optional[str]:
    raw = os.getenv("PUBLIC_BOT_USERNAME", "").strip()
    if raw:
        username = raw.lstrip("@").strip()
        return username or None
    if context is None:
        return None
    try:
        username = str(getattr(context.bot, "username", "") or "")
    except Exception:
        username = ""
    username = username.lstrip("@").strip()
    return username or None


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


def strike_text(text: str) -> str:
    return "".join(f"{char}\u0336" for char in text)


def get_exclusive_stock(db: Dict[str, object], card_file: str) -> Tuple[int, int]:
    stock = db.get("exclusive_stock", {})
    if isinstance(stock, dict):
        record = stock.get(card_file)
        if isinstance(record, dict):
            try:
                total = int(record.get("total", EXCLUSIVE_STOCK_LIMIT))
                remaining = int(record.get("remaining", 0))
                return remaining, total
            except (TypeError, ValueError):
                pass
    return EXCLUSIVE_STOCK_LIMIT, EXCLUSIVE_STOCK_LIMIT


def consume_exclusive_stock(db: Dict[str, object], card_file: str) -> bool:
    stock = db.setdefault("exclusive_stock", {})
    record = stock.get(card_file)
    if not isinstance(record, dict):
        remaining = EXCLUSIVE_STOCK_LIMIT
        total = EXCLUSIVE_STOCK_LIMIT
    else:
        try:
            remaining = int(record.get("remaining", 0))
        except (TypeError, ValueError):
            remaining = 0
        try:
            total = int(record.get("total", EXCLUSIVE_STOCK_LIMIT))
        except (TypeError, ValueError):
            total = EXCLUSIVE_STOCK_LIMIT
    if remaining <= 0:
        return False
    stock[card_file] = {"total": total, "remaining": remaining - 1}
    return True


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


def load_discount_data() -> Dict[str, object]:
    if not DISCOUNT_FILE.exists():
        return {"date": "", "items": [], "generated_at": None}
    try:
        data = json.loads(DISCOUNT_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"date": "", "items": [], "generated_at": None}
    if not isinstance(data, dict):
        return {"date": "", "items": [], "generated_at": None}
    data.setdefault("items", [])
    return data


def save_discount_data(data: Dict[str, object]) -> None:
    DISCOUNT_FILE.parent.mkdir(parents=True, exist_ok=True)
    DISCOUNT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def discount_day_key(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    return now.date().isoformat()


def build_discount_index(discounts: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    items = discounts.get("items", [])
    if not isinstance(items, list):
        return {}
    return {str(item.get("file")): item for item in items if item.get("file")}


def get_discount_item(
    discounts: Dict[str, object], card_file: str
) -> Optional[Dict[str, object]]:
    index = build_discount_index(discounts)
    return index.get(card_file)


def is_discount_active(item: Optional[Dict[str, object]]) -> bool:
    if not item:
        return False
    try:
        return int(item.get("remaining", 0)) > 0
    except (TypeError, ValueError):
        return False


def pick_weighted_cards(
    cards_by_rarity: Dict[str, List[Card]],
    count: int,
) -> List[Card]:
    weighted_pool: List[Tuple[Card, float]] = []
    for rarity, cards in cards_by_rarity.items():
        if rarity == "exclusive":
            continue
        weight = DISCOUNT_RARITY_WEIGHTS.get(rarity, 0)
        if weight <= 0:
            continue
        for card in cards:
            if card.price is None:
                continue
            weighted_pool.append((card, weight))
    selected: List[Card] = []
    pool = weighted_pool[:]
    for _ in range(min(count, len(pool))):
        total_weight = sum(weight for _, weight in pool)
        if total_weight <= 0:
            break
        pick = random.random() * total_weight
        cumulative = 0.0
        chosen_index = 0
        for index, (_, weight) in enumerate(pool):
            cumulative += weight
            if pick <= cumulative:
                chosen_index = index
                break
        card, _ = pool.pop(chosen_index)
        selected.append(card)
    return selected


def generate_discounts(cards_by_rarity: Dict[str, List[Card]]) -> Dict[str, object]:
    now = now_local()
    day_key = discount_day_key(now)
    items: List[Dict[str, object]] = []
    percent_min = min(DISCOUNT_PERCENT_MIN, DISCOUNT_PERCENT_MAX)
    percent_max = max(DISCOUNT_PERCENT_MIN, DISCOUNT_PERCENT_MAX)
    for card in pick_weighted_cards(cards_by_rarity, DISCOUNT_ITEMS_PER_DAY):
        percent = random.randint(percent_min, percent_max)
        original_price = int(card.price or 0)
        discount_price = int(round(original_price * (100 - percent) / 100))
        if discount_price >= original_price:
            discount_price = max(1, original_price - 1)
        quantity = DISCOUNT_QUANTITY_BY_RARITY.get(card.rarity, 0)
        items.append(
            {
                "file": card.file,
                "rarity": card.rarity,
                "percent": percent,
                "original_price": original_price,
                "discount_price": discount_price,
                "remaining": int(quantity),
                "initial": int(quantity),
            }
        )
    return {
        "date": day_key,
        "generated_at": now.isoformat(),
        "items": items,
    }


async def ensure_discounts(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, object]:
    lock = context.application.bot_data.setdefault("discount_lock", asyncio.Lock())
    async with lock:
        discounts = load_discount_data()
        today = discount_day_key()
        if discounts.get("date") != today:
            cards_by_rarity = context.application.bot_data["cards_by_rarity"]
            discounts = generate_discounts(cards_by_rarity)
            save_discount_data(discounts)
        context.application.bot_data["discounts"] = discounts
    return discounts


def load_giveaway_data() -> Dict[str, object]:
    if not GIVEAWAY_FILE.exists():
        return {}
    try:
        data = json.loads(GIVEAWAY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_giveaway_data(data: Dict[str, object]) -> None:
    GIVEAWAY_FILE.parent.mkdir(parents=True, exist_ok=True)
    GIVEAWAY_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def giveaway_day_key(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    day = now.date()
    if now.hour < GIVEAWAY_START_HOUR:
        day = day - timedelta(days=1)
    return day.isoformat()


def giveaway_phase(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    hour = now.hour
    if hour < GIVEAWAY_START_HOUR:
        return "idle"
    if hour < GIVEAWAY_SIGNUP_END_HOUR:
        return "open"
    if hour < GIVEAWAY_ANNOUNCE_HOUR:
        return "closed"
    return "announce"


def pick_giveaway_card(cards_by_rarity: Dict[str, List[Card]]) -> Optional[Card]:
    if GIVEAWAY_MIN_RARITY in RARITY_ORDER:
        min_index = RARITY_ORDER.index(GIVEAWAY_MIN_RARITY)
    else:
        min_index = RARITY_ORDER.index("epic")
    pool: List[Card] = []
    for rarity in RARITY_ORDER[min_index:]:
        if rarity == "exclusive":
            continue
        pool.extend(cards_by_rarity.get(rarity, []))
    if pool:
        return random.choice(pool)
    fallback = []
    for rarity in RARITY_ORDER:
        if rarity == "exclusive":
            continue
        fallback.extend(cards_by_rarity.get(rarity, []))
    if fallback:
        return random.choice(fallback)
    return None


def create_giveaway(cards_by_rarity: Dict[str, List[Card]]) -> Dict[str, object]:
    now = now_local()
    prize_card = pick_giveaway_card(cards_by_rarity)
    data: Dict[str, object] = {
        "date": giveaway_day_key(now),
        "created_at": now.isoformat(),
        "status": "open",
        "entries": [],
        "winners": {},
        "start_announced": False,
    }
    if prize_card:
        data["prize_card_file"] = prize_card.file
        data["prize_card_rarity"] = prize_card.rarity
    return data


async def ensure_giveaway(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, object]:
    lock = context.application.bot_data.setdefault("giveaway_lock", asyncio.Lock())
    async with lock:
        giveaway = load_giveaway_data()
        today = giveaway_day_key()
        if giveaway.get("date") != today:
            cards_by_rarity = context.application.bot_data["cards_by_rarity"]
            giveaway = create_giveaway(cards_by_rarity)
            save_giveaway_data(giveaway)
        context.application.bot_data["giveaway"] = giveaway
    return giveaway


def format_giveaway_prize(
    giveaway: Dict[str, object],
    card_map: Dict[str, Card],
) -> str:
    prize_file = giveaway.get("prize_card_file")
    if not prize_file:
        return "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
    card = card_map.get(prize_file)
    if not card:
        return "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
    return card_display_name(card)


async def announce_giveaway_start(
    context: ContextTypes.DEFAULT_TYPE,
    giveaway: Dict[str, object],
) -> None:
    lock = context.application.bot_data.setdefault("giveaway_lock", asyncio.Lock())
    async with lock:
        current = load_giveaway_data() or giveaway
        if current.get("date") != giveaway_day_key():
            return
        if current.get("start_announced"):
            return
        current["start_announced"] = True
        current["start_announced_at"] = now_local().isoformat()
        save_giveaway_data(current)
    giveaway = current

    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    prize_file = giveaway.get("prize_card_file")
    prize_card = card_map.get(prize_file) if prize_file else None
    prize_name = card_display_name(prize_card) if prize_card else "сосиска"
    prize_rarity = (
        RARITY_NAMES.get(prize_card.rarity, prize_card.rarity)
        if prize_card
        else ""
    )
    balance_prizes = list(GIVEAWAY_BALANCE_PRIZES)
    third_prize = balance_prizes[2] if len(balance_prizes) > 2 else 500
    fourth_prize = balance_prizes[1] if len(balance_prizes) > 1 else 250
    fifth_prize = balance_prizes[0] if len(balance_prizes) > 0 else 100
    lines = [
        "\u0420\u043e\u0437\u044b\u0433\u0440\u044b\u0448 \u0434\u043d\u044f",
        f"\u041f\u0440\u0438\u0437 1 \u043c\u0435\u0441\u0442\u0430: {prize_name}"
        + (f" ({prize_rarity})" if prize_rarity else ""),
        "2 \u043c\u0435\u0441\u0442\u043e: \u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u0430\u044f \u043a\u0440\u0443\u0442\u043a\u0430",
        f"3 \u043c\u0435\u0441\u0442\u043e: {third_prize}\u0440",
        f"4 \u043c\u0435\u0441\u0442\u043e: {fourth_prize}\u0440",
        f"5 \u043c\u0435\u0441\u0442\u043e: {fifth_prize}\u0440",
        "\u0423\u0447\u0430\u0441\u0442\u0438\u0435: /rozigrish",
    ]
    text = "\n".join(lines)
    for uid in db.get("users", {}):
        try:
            await context.bot.send_message(chat_id=int(uid), text=text)
        except Exception:
            continue


async def announce_giveaway(
    context: ContextTypes.DEFAULT_TYPE,
    giveaway: Dict[str, object],
) -> None:
    winners: Dict[str, str] = {}
    all_entries: List[str] = []
    lock = context.application.bot_data.setdefault("giveaway_lock", asyncio.Lock())
    async with lock:
        current = load_giveaway_data() or giveaway
        if current.get("status") == "announced":
            return
        if current.get("date") != giveaway_day_key():
            return
        entries = current.get("entries", [])
        if not isinstance(entries, list):
            entries = []
        unique_entries = list(dict.fromkeys(str(uid) for uid in entries))
        if not unique_entries:
            current["status"] = "announced"
            current["announced_at"] = now_local().isoformat()
            save_giveaway_data(current)
            return
        all_entries = unique_entries
        winners_count = min(GIVEAWAY_WINNERS, len(unique_entries))
        winners_list = random.sample(unique_entries, winners_count)
        winners = {str(place): uid for place, uid in enumerate(winners_list, start=1)}
        current["winners"] = winners
        current["status"] = "announced"
        current["announced_at"] = now_local().isoformat()
        save_giveaway_data(current)
    giveaway = current
    if not winners:
        return

    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    prize_file = giveaway.get("prize_card_file")
    prize_card = card_map.get(prize_file) if prize_file else None

    balance_prizes = list(GIVEAWAY_BALANCE_PRIZES)
    third_prize = balance_prizes[2] if len(balance_prizes) > 2 else 500
    fourth_prize = balance_prizes[1] if len(balance_prizes) > 1 else 250
    fifth_prize = balance_prizes[0] if len(balance_prizes) > 0 else 100

    for place in sorted(winners, key=lambda value: int(value)):
        uid = winners[place]
        user = db.get("users", {}).get(uid)
        if not user:
            continue
        if place == "1":
            if prize_card:
                user.setdefault("inventory", []).append(
                    make_inventory_item(prize_card.file)
                )
        elif place == "2":
            user["free_rolls"] = int(user.get("free_rolls", 0)) + GIVEAWAY_FREE_ROLLS
        elif place == "3":
            user["balance"] = int(user.get("balance", 0)) + third_prize
        elif place == "4":
            user["balance"] = int(user.get("balance", 0)) + fourth_prize
        elif place == "5":
            user["balance"] = int(user.get("balance", 0)) + fifth_prize

    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)

    first_uid = winners.get("1")
    first_label = get_user_label_by_id(db, first_uid) if first_uid else "\u041d\u0435\u0442 \u043f\u043e\u0431\u0435\u0434\u0438\u0442\u0435\u043b\u044f"
    first_lines = [
        "\u0420\u043e\u0437\u044b\u0433\u0440\u044b\u0448 \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d!",
        f"\ud83e\udd47 1 \u043c\u0435\u0441\u0442\u043e: {first_label}",
    ]
    if prize_card:
        first_lines.append(card_display_name(prize_card))
        first_lines.append(
            f"\u0420\u0435\u0434\u043a\u043e\u0441\u0442\u044c: {RARITY_NAMES.get(prize_card.rarity, prize_card.rarity)}"
        )
    first_text = "\n".join(first_lines)

    other_lines = []
    for place in ("2", "3", "4", "5"):
        uid = winners.get(place)
        if not uid:
            continue
        label = get_user_label_by_id(db, uid)
        if place == "2":
            reward = "\u0431\u0435\u0441\u043f\u043b\u0430\u0442\u043d\u0430\u044f \u043a\u0440\u0443\u0442\u043a\u0430"
        elif place == "3":
            reward = f"{third_prize}\u0440"
        elif place == "4":
            reward = f"{fourth_prize}\u0440"
        else:
            reward = f"{fifth_prize}\u0440"
        other_lines.append(f"{place} \u043c\u0435\u0441\u0442\u043e: {label} \u2014 {reward}")
    other_text = "\n".join(other_lines)

    prize_path = get_card_media_path(prize_card) if prize_card else None
    for uid in all_entries:
        try:
            if prize_path and prize_path.exists():
                with prize_path.open("rb") as photo:
                    await context.bot.send_photo(
                        chat_id=int(uid), photo=photo, caption=first_text
                    )
            else:
                await context.bot.send_message(chat_id=int(uid), text=first_text)
            if other_text:
                await context.bot.send_message(chat_id=int(uid), text=other_text)
        except Exception:
            continue

async def safe_answer_callback(
    query,
    text: Optional[str] = None,
    show_alert: bool = False,
) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except TimedOut:
        return
    except NetworkError:
        return
    except BadRequest as exc:
        message = str(exc).lower()
        if "query is too old" in message or "query id is invalid" in message:
            return
        raise


def build_stars_payload(amount: int) -> str:
    return f"stars_topup:{amount}"


def parse_stars_payload(payload: str) -> Optional[int]:
    if not payload:
        return None
    if not payload.startswith("stars_topup:"):
        return None
    raw = payload.split(":", 1)[1]
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None




def apply_pressed_by(text: str, tg_user) -> str:
    return text or ""


def get_user_label_by_id(db: Dict[str, object], uid: str) -> str:
    user = db.get("users", {}).get(uid, {})
    tag = str(user.get("user_tag") or "").strip()
    if tag:
        return f"@{tag}"
    name = str(user.get("username") or "").strip()
    if name:
        return name
    return str(uid)


def message_owner_key(message) -> Optional[Tuple[int, int]]:
    if not message or not message.chat or message.message_id is None:
        return None
    return message.chat.id, message.message_id


def get_message_owner(bot_data: Dict[str, object], message) -> Optional[int]:
    key = message_owner_key(message)
    if not key:
        return None
    owners = bot_data.get("message_owners", {})
    return owners.get(key)


def set_message_owner(
    bot_data: Dict[str, object], message, user_id: Optional[int]
) -> None:
    if user_id is None:
        return
    key = message_owner_key(message)
    if not key:
        return
    owners = bot_data.setdefault("message_owners", {})
    owners[key] = int(user_id)


def build_draw_caption(user_label: str, card: Card) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    return "\n".join(
        [
            f"{user_label}, \u0432\u0430\u043c \u0432\u044b\u043f\u0430\u043b\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430!",
            f"{format_card_label(card)} - {price_text}",
        ]
    )


def build_upgrade_warning_caption(user_label: str, card: Card) -> str:
    base = build_draw_caption(user_label, card)
    return "\n".join(
        [
            "\u0412\u044b \u0443\u0432\u0435\u0440\u0435\u043d\u044b? \u0412\u044b \u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u043e\u0442\u0435\u0440\u044f\u0442\u044c \u043a\u0430\u0440\u0442\u0443.",
            base,
        ]
    )


def build_upgrade_success_caption(user_label: str, card: Card) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    return "\n".join(
        [
            f"{user_label}, \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c \u043f\u043e\u0432\u044b\u0448\u0435\u043d\u0430 (1)!",
            f"{format_card_label(card)} - {price_text}",
        ]
    )


def build_upgrade_fail_caption(user_label: str) -> str:
    return "\n".join(
        [
            f"{user_label}, \u043d\u0435 \u043f\u043e\u0432\u0435\u0437\u043b\u043e (0).",
            "\u0421\u043e\u0441\u0438\u0441\u043a\u0430 \u043f\u043e\u0442\u0435\u0440\u044f\u043d\u0430.",
        ]
    )


def roll_kazik_digits(
    win_chance: float = KAZIK_WIN_CHANCE,
    win_weights: Optional[Dict[int, float]] = None,
) -> List[int]:
    if win_weights is None:
        win_weights = KAZIK_WIN_WEIGHTS
    if random.random() < win_chance:
        winner = random.choices(
            list(win_weights.keys()),
            weights=list(win_weights.values()),
            k=1,
        )[0]
        return [winner, winner, winner]
    digits = [random.choice(KAZIK_DIGITS) for _ in range(3)]
    while digits[0] == digits[1] == digits[2]:
        digits = [random.choice(KAZIK_DIGITS) for _ in range(3)]
    return digits


def kazik_reward_rarities(digit: int) -> List[str]:
    if digit == 1:
        return ["dno", "common", "uncommon"]
    if digit == 2:
        return ["uncommon", "rare", "epic"]
    return ["legendary", "platinum", "meme"]


def pick_kazik_reward_card(
    by_rarity: Dict[str, List[Card]],
    digit: int,
    *,
    allow_exclusive: bool = False,
) -> Optional[Card]:
    available_by_rarity = filter_existing_cards(by_rarity)
    if allow_exclusive and digit == 3 and random.random() < VIP_KAZIK_EXCLUSIVE_CHANCE:
        exclusive_pool = available_by_rarity.get("exclusive", [])
        if exclusive_pool:
            return random.choice(exclusive_pool)
    pool: List[Card] = []
    for rarity in kazik_reward_rarities(digit):
        pool.extend(available_by_rarity.get(rarity, []))
    if not pool:
        return None
    return random.choice(pool)


def build_kazik_text_line(digits: List[int], revealed: int) -> str:
    parts = []
    for index in range(3):
        if index < revealed and index < len(digits):
            parts.append(str(digits[index]))
        else:
            parts.append("?")
    return " | ".join(parts)


def get_next_rarity(rarity: str, *, allow_exclusive: bool = False) -> Optional[str]:
    if rarity not in RARITY_ORDER:
        return None
    index = RARITY_ORDER.index(rarity)
    for next_rarity in RARITY_ORDER[index + 1 :]:
        if next_rarity == "exclusive" and not allow_exclusive:
            continue
        return next_rarity
    return None


def truncate_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def font_line_height(font: ImageFont.FreeTypeFont) -> int:
    try:
        ascent, descent = font.getmetrics()
        return ascent + descent
    except Exception:
        bbox = font.getbbox("Hg")
        return bbox[3] - bbox[1]


def fit_text_to_width(
    text: str, max_width: int, font: ImageFont.FreeTypeFont, draw: ImageDraw.ImageDraw
) -> str:
    if not text:
        return ""
    if draw.textlength(text, font=font) <= max_width:
        return text
    ellipsis = "..."
    trimmed = text
    while trimmed and draw.textlength(trimmed + ellipsis, font=font) > max_width:
        trimmed = trimmed[:-1]
    return trimmed + ellipsis if trimmed else ellipsis


def load_truetype_font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    if hasattr(ImageFont, "LAYOUT_RAQM"):
        return ImageFont.truetype(
            str(path), size=size, layout_engine=ImageFont.LAYOUT_RAQM
        )
    return ImageFont.truetype(str(path), size=size)

def pick_font_from_candidates(
    size: int, candidates: List[Path]
) -> ImageFont.FreeTypeFont:
    for font_path in candidates:
        if font_path.exists():
            try:
                return load_truetype_font(font_path, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def collect_font_candidates() -> List[Path]:
    env_paths = []
    env_single = os.getenv("SOSISKI_FONT_PATH", "").strip()
    if env_single:
        env_paths.append(Path(env_single))
    env_multi = os.getenv("SOSISKI_FONT_PATHS", "").strip()
    if env_multi:
        env_paths.extend(
            Path(part.strip())
            for part in env_multi.split(";")
            if part.strip()
        )
    return env_paths + FONT_CANDIDATES


def contains_cjk(text: str) -> bool:
    for char in text:
        code = ord(char)
        if (
            0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0x3040 <= code <= 0x30FF
            or 0xAC00 <= code <= 0xD7AF
        ):
            return True
    return False


def contains_symbol(text: str) -> bool:
    for char in text:
        code = ord(char)
        if (
            0x2600 <= code <= 0x26FF
            or 0x2700 <= code <= 0x27BF
            or 0x2190 <= code <= 0x21FF
            or 0x2300 <= code <= 0x23FF
            or 0x2500 <= code <= 0x25FF
            or 0x2B00 <= code <= 0x2BFF
            or 0x1F000 <= code <= 0x1FAFF
            or 0x1F300 <= code <= 0x1FAFF
            or 0x1F1E6 <= code <= 0x1F1FF
            or 0x1F3FB <= code <= 0x1F3FF
            or 0xFE00 <= code <= 0xFE0F
        ):
            return True
    return False


def pick_font(size: int) -> ImageFont.FreeTypeFont:
    return pick_font_from_candidates(size, collect_font_candidates())


def pick_font_bundle(
    size: int,
) -> Tuple[
    ImageFont.FreeTypeFont,
    ImageFont.FreeTypeFont,
    ImageFont.FreeTypeFont,
]:
    candidates = collect_font_candidates()
    base_font = pick_font_from_candidates(size, candidates)
    cjk = [path for path in candidates if path.name in CJK_FONT_NAMES]
    cjk_font = pick_font_from_candidates(size, cjk + candidates)
    sym = [path for path in candidates if path.name in SYMBOL_FONT_NAMES]
    preferred = [
        "NotoColorEmoji.ttf",
        "NotoEmoji-Regular.ttf",
        "NotoSansSymbols2-Regular.ttf",
        "Symbola.ttf",
    ]
    preferred_index = {name: index for index, name in enumerate(preferred)}
    sym.sort(key=lambda path: preferred_index.get(path.name, len(preferred)))
    symbol_font = pick_font_from_candidates(size, sym + candidates)
    return base_font, cjk_font, symbol_font


def text_length_mixed(
    text: str,
    draw: ImageDraw.ImageDraw,
    base_font: ImageFont.FreeTypeFont,
    cjk_font: ImageFont.FreeTypeFont,
    symbol_font: ImageFont.FreeTypeFont,
) -> float:
    length = 0.0
    for char in text:
        font = base_font
        if contains_cjk(char):
            font = cjk_font
        elif contains_symbol(char):
            font = symbol_font
        length += draw.textlength(char, font=font)
    return length


def fit_text_to_width_mixed(
    text: str,
    max_width: int,
    draw: ImageDraw.ImageDraw,
    base_font: ImageFont.FreeTypeFont,
    cjk_font: ImageFont.FreeTypeFont,
    symbol_font: ImageFont.FreeTypeFont,
) -> str:
    if not text:
        return ""
    if text_length_mixed(text, draw, base_font, cjk_font, symbol_font) <= max_width:
        return text
    ellipsis = "..."
    trimmed = text
    while trimmed:
        candidate = trimmed + ellipsis
        if (
            text_length_mixed(candidate, draw, base_font, cjk_font, symbol_font)
            <= max_width
        ):
            return candidate
        trimmed = trimmed[:-1]
    return ellipsis


def draw_text_mixed(
    draw: ImageDraw.ImageDraw,
    position: Tuple[int, int],
    text: str,
    base_font: ImageFont.FreeTypeFont,
    cjk_font: ImageFont.FreeTypeFont,
    symbol_font: ImageFont.FreeTypeFont,
    fill: Tuple[int, int, int, int],
) -> None:
    x, y = position
    for char in text:
        font = base_font
        if contains_cjk(char):
            font = cjk_font
        elif contains_symbol(char):
            font = symbol_font
        draw.text((x, y), char, font=font, fill=fill)
        x += draw.textlength(char, font=font)


def pick_font_for_text(text: str, size: int) -> ImageFont.FreeTypeFont:
    candidates = collect_font_candidates()
    if contains_cjk(text):
        cjk = [path for path in candidates if path.name in CJK_FONT_NAMES]
        return pick_font_from_candidates(size, cjk + candidates)
    if contains_symbol(text):
        sym = [path for path in candidates if path.name in SYMBOL_FONT_NAMES]
        return pick_font_from_candidates(size, sym + candidates)
    return pick_font_from_candidates(size, candidates)


async def fetch_user_avatar(bot, user_id: int) -> Optional[bytes]:
    try:
        photos = await bot.get_user_profile_photos(user_id=user_id, limit=1)
    except Exception:
        return None
    if not photos or photos.total_count == 0:
        return None
    try:
        file = await bot.get_file(photos.photos[0][-1].file_id)
        data = await file.download_as_bytearray()
    except Exception:
        return None
    return bytes(data)


def get_avatar_cache(bot_data: Dict[str, object]) -> Dict[int, Tuple[float, bytes]]:
    cache = bot_data.setdefault("avatar_cache", {})
    return cache


async def fetch_user_avatar_cached(
    bot,
    user_id: int,
    cache: Dict[int, Tuple[float, bytes]],
    ttl_sec: int = AVATAR_CACHE_TTL_SEC,
) -> Optional[bytes]:
    now = time.monotonic()
    cached = cache.get(user_id)
    if cached and cached[0] > now:
        return cached[1]
    data = await fetch_user_avatar(bot, user_id)
    if data:
        cache[user_id] = (now + ttl_sec, data)
    return data


_logo_template: Optional[Image.Image] = None


def load_logo_template() -> Optional[Image.Image]:
    global _logo_template
    if _logo_template is not None:
        return _logo_template
    path = BASE_DIR / os.getenv("LOGO_FILE", "logo.webp")
    if not path.exists():
        return None
    try:
        _logo_template = Image.open(path).convert("RGBA")
    except Exception:
        _logo_template = None
    return _logo_template


def pick_logo_colors(image: Image.Image, box: Tuple[int, int, int, int]) -> Tuple[
    Tuple[int, int, int, int], Tuple[int, int, int, int]
]:
    try:
        region = image.crop(box).convert("RGB")
        r, g, b = ImageStat.Stat(region).mean
        luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0
        if luminance >= 0.6:
            fg = (0, 0, 0, 255)
        elif luminance <= 0.4:
            fg = (255, 255, 255, 255)
        else:
            contrast_white = 1.05 / (luminance + 0.05)
            contrast_black = (luminance + 0.05) / 0.05
            fg = (255, 255, 255, 255) if contrast_white >= contrast_black else (0, 0, 0, 255)
    except Exception:
        fg = (255, 255, 255, 255)
    shadow = (0, 0, 0, 255) if fg[0] > 0 else (255, 255, 255, 255)
    return fg, shadow


def build_logo_stamp(
    logo: Image.Image,
    size: int,
    fg: Tuple[int, int, int, int],
    shadow: Tuple[int, int, int, int],
) -> Image.Image:
    logo_img = ImageOps.contain(logo, (size, size), method=Image.LANCZOS)
    alpha = logo_img.getchannel("A")
    fg_logo = Image.new("RGBA", logo_img.size, fg)
    fg_logo.putalpha(alpha)
    shadow_logo = Image.new("RGBA", logo_img.size, shadow)
    shadow_alpha = alpha.point(lambda a: int(a * 0.7))
    shadow_logo.putalpha(shadow_alpha)
    shadow_logo = shadow_logo.filter(ImageFilter.GaussianBlur(radius=3))

    stamp = Image.new("RGBA", logo_img.size, (0, 0, 0, 0))
    stamp.alpha_composite(shadow_logo, (2, 2))
    stamp.alpha_composite(fg_logo, (0, 0))
    return stamp


def apply_corner_logo(image: Image.Image) -> None:
    logo = load_logo_template()
    if logo is None:
        return
    if image.mode != "RGBA":
        image_rgba = image.convert("RGBA")
        image.paste(image_rgba)

    width, height = image.size
    size = max(26, int(min(width, height) * 0.09))
    logo_img = ImageOps.contain(logo, (size, size), method=Image.LANCZOS)
    margin = max(14, size // 3)
    x = max(0, width - margin - logo_img.width)
    y = margin
    box = (x, y, x + logo_img.width, y + logo_img.height)
    fg, shadow = pick_logo_colors(image, box)
    stamp = build_logo_stamp(logo, size, fg, shadow)
    image.alpha_composite(stamp, (x, y))


def ensure_exclusive_cache_dir() -> Path:
    cache_dir = PHOTO_CACHE_DIR / "exclusive"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def exclusive_cache_path(source: Path) -> Path:
    cache_version = os.getenv("IMAGE_CACHE_VERSION", "v2").strip() or "v2"
    filename = f"{source.stem}_wm_{cache_version}{source.suffix.lower()}"
    return ensure_exclusive_cache_dir() / filename


def build_logo_stamp_for_image(image: Image.Image) -> Tuple[Image.Image, Tuple[int, int]]:
    logo = load_logo_template()
    if logo is None:
        raise RuntimeError("Logo not found")
    width, height = image.size
    size = max(26, int(min(width, height) * 0.09))
    margin = max(14, size // 3)
    x = max(0, width - margin - size)
    y = margin
    box = (x, y, x + size, y + size)
    fg, shadow = pick_logo_colors(image, box)
    stamp = build_logo_stamp(logo, size, fg, shadow)
    return stamp, (x, y)


def watermark_exclusive_image(source: Path, target: Path) -> bool:
    try:
        image = Image.open(source).convert("RGBA")
        stamp, position = build_logo_stamp_for_image(image)
        image.alpha_composite(stamp, position)
        target.parent.mkdir(parents=True, exist_ok=True)
        suffix = target.suffix.lower()
        if suffix == ".webp":
            image.save(target, format="WEBP")
        elif suffix == ".png":
            image.save(target, format="PNG")
        else:
            image.convert("RGB").save(target)
        return True
    except Exception:
        return False


def watermark_exclusive_gif(source: Path, target: Path) -> bool:
    try:
        base = Image.open(source)
        frames = []
        durations = []
        for frame in ImageSequence.Iterator(base):
            frame_rgba = frame.convert("RGBA")
            stamp, position = build_logo_stamp_for_image(frame_rgba)
            frame_rgba.alpha_composite(stamp, position)
            frames.append(frame_rgba)
            durations.append(frame.info.get("duration", base.info.get("duration", 100)))
        if not frames:
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        frames[0].save(
            target,
            save_all=True,
            append_images=frames[1:],
            loop=base.info.get("loop", 0),
            duration=durations,
            disposal=base.info.get("disposal", 2),
            optimize=False,
        )
        return True
    except Exception:
        return False


def watermark_exclusive_video(source: Path, target: Path) -> bool:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            sample_path = tmp_dir_path / "sample.png"
            logo_path = tmp_dir_path / "logo.png"
            subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(source),
                    "-frames:v",
                    "1",
                    str(sample_path),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            sample = Image.open(sample_path).convert("RGBA")
            stamp, position = build_logo_stamp_for_image(sample)
            stamp.save(logo_path, format="PNG")

            x, y = position
            filters = f"overlay={x}:{y}"
            target.parent.mkdir(parents=True, exist_ok=True)
            codec_args = []
            if target.suffix.lower() == ".webm":
                codec_args = [
                    "-c:v",
                    "libvpx-vp9",
                    "-b:v",
                    "0",
                    "-crf",
                    "32",
                    "-c:a",
                    "libopus",
                ]
            else:
                codec_args = [
                    "-c:v",
                    "libx264",
                    "-crf",
                    "23",
                    "-preset",
                    "veryfast",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:a",
                    "aac",
                ]
            subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(source),
                    "-i",
                    str(logo_path),
                    "-filter_complex",
                    filters,
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a?",
                    *codec_args,
                    str(target),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
        return True
    except Exception:
        return False


def get_exclusive_media_path(source: Path) -> Path:
    if not source.exists():
        return source
    target = exclusive_cache_path(source)
    if target.exists() and target.stat().st_size > 0:
        return target
    suffix = source.suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        if watermark_exclusive_image(source, target):
            return target
    elif suffix == ".gif":
        if watermark_exclusive_gif(source, target):
            return target
    elif suffix in {".mp4", ".webm"}:
        if watermark_exclusive_video(source, target):
            return target
    return source


def get_card_media_path(card: Card) -> Path:
    path = card_file_path(card)
    if card.rarity == "exclusive":
        return get_exclusive_media_path(path)
    return path


def build_profile_image(
    display_name: str,
    rank: int,
    total_users: int,
    total_value: int,
    balance: int,
    stars: int,
    vip: bool,
    avatar_bytes: Optional[bytes],
) -> BytesIO:
    width, height = 900, 500
    if avatar_bytes:
        avatar = Image.open(BytesIO(avatar_bytes)).convert("RGB")
        base = ImageOps.fit(avatar, (width, height), method=Image.LANCZOS)
        base = base.filter(ImageFilter.GaussianBlur(radius=12))
    else:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 24 + int(60 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.55)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 160),
    )
    base.alpha_composite(overlay)

    avatar_size = int(plate_h * 0.7)
    avatar_x = plate_x + 40
    avatar_y = plate_y + (plate_h - avatar_size) // 2
    if avatar_bytes:
        avatar_img = Image.open(BytesIO(avatar_bytes)).convert("RGB")
        avatar_img = ImageOps.fit(
            avatar_img, (avatar_size, avatar_size), method=Image.LANCZOS
        )
    else:
        avatar_img = Image.new("RGB", (avatar_size, avatar_size), "#2d2d2d")
    mask = Image.new("L", (avatar_size, avatar_size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, avatar_size, avatar_size), fill=255)
    base.paste(avatar_img, (avatar_x, avatar_y), mask)
    if vip:
        border = 6
        draw = ImageDraw.Draw(base)
        draw.ellipse(
            (
                avatar_x - border // 2,
                avatar_y - border // 2,
                avatar_x + avatar_size + border // 2,
                avatar_y + avatar_size + border // 2,
            ),
            outline=(255, 215, 0, 255),
            width=border,
        )

    text_x = avatar_x + avatar_size + 54
    display_text = str(display_name or "")
    title_base, title_cjk, title_sym = pick_font_bundle(PROFILE_TITLE_SIZE)
    info_base, info_cjk, info_sym = pick_font_bundle(PROFILE_INFO_SIZE)
    draw = ImageDraw.Draw(base)
    max_name_width = plate_x + plate_w - 40 - text_x
    name_text = fit_text_to_width_mixed(
        display_text, max_name_width, draw, title_base, title_cjk, title_sym
    )
    name_height = max(
        font_line_height(title_base),
        font_line_height(title_cjk),
        font_line_height(title_sym),
    )
    info_height = max(
        font_line_height(info_base),
        font_line_height(info_cjk),
        font_line_height(info_sym),
    )
    line_gap = max(8, int(info_height * 0.25))
    total_text_h = name_height + line_gap + 4 * info_height + 3 * line_gap
    text_y = plate_y + max(0, (plate_h - total_text_h) // 2)

    name_color = (255, 215, 0, 255) if vip else (255, 255, 255, 255)
    draw_text_mixed(
        draw,
        (text_x, text_y),
        name_text,
        title_base,
        title_cjk,
        title_sym,
        name_color,
    )
    if vip:
        vip_font = pick_font(int(PROFILE_TITLE_SIZE * 0.6))
        name_width = text_length_mixed(
            name_text, draw, title_base, title_cjk, title_sym
        )
        vip_x = text_x + name_width + 12
        vip_y = text_y + int(name_height * 0.2)
        draw.text(
            (vip_x, vip_y),
            "VIP",
            font=vip_font,
            fill=(255, 215, 0, 255),
        )
    info_color = (220, 220, 220, 255)
    current_y = text_y + name_height + line_gap
    draw_text_mixed(
        draw,
        (text_x, current_y),
        f"\u041c\u0435\u0441\u0442\u043e \u0432 \u0442\u043e\u043f\u0435: {rank}/{total_users}",
        info_base,
        info_cjk,
        info_sym,
        info_color,
    )
    current_y += info_height + line_gap
    draw_text_mixed(
        draw,
        (text_x, current_y),
        f"\u041e\u0431\u0449\u0430\u044f \u0446\u0435\u043d\u0430 \u0441\u043e\u0441\u0438\u0441\u043e\u043a: {total_value}",
        info_base,
        info_cjk,
        info_sym,
        info_color,
    )
    current_y += info_height + line_gap
    draw_text_mixed(
        draw,
        (text_x, current_y),
        f"\u0411\u0430\u043b\u0430\u043d\u0441: {balance} \u0440\u0443\u0431.",
        info_base,
        info_cjk,
        info_sym,
        info_color,
    )
    current_y += info_height + line_gap
    draw_text_mixed(
        draw,
        (text_x, current_y),
        f"\u0417\u0432\u0435\u0437\u0434\u044b: {format_stars(stars)}",
        info_base,
        info_cjk,
        info_sym,
        info_color,
    )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_leaderboard_image(
    entries: List[Tuple[str, int, Optional[bytes], bool]],
    total_users: int,
) -> BytesIO:
    width = 900
    title_font = pick_font(LEADERBOARD_TITLE_SIZE)
    sub_font = pick_font(LEADERBOARD_SUBTITLE_SIZE)
    entry_font = pick_font(LEADERBOARD_ENTRY_SIZE)
    row_base, row_cjk, row_sym = pick_font_bundle(LEADERBOARD_ENTRY_SIZE)

    title_height = font_line_height(title_font)
    sub_height = font_line_height(sub_font)
    entry_height = font_line_height(entry_font)
    rows_count = max(1, len(entries))
    row_height = (
        max(LEADERBOARD_AVATAR_SIZE, entry_height) if entries else entry_height
    )
    header_height = title_height + LEADERBOARD_HEADER_GAP + sub_height
    rows_height = rows_count * row_height + (rows_count - 1) * LEADERBOARD_ROW_GAP
    content_height = header_height + LEADERBOARD_HEADER_TO_ROWS_GAP + rows_height
    plate_h = content_height + LEADERBOARD_PLATE_PADDING * 2
    height = plate_h + LEADERBOARD_OUTER_MARGIN * 2

    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=16))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 18 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_x = (width - plate_w) // 2
    plate_y = LEADERBOARD_OUTER_MARGIN
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=36,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_x = plate_x + LEADERBOARD_PLATE_PADDING
    title_y = plate_y + LEADERBOARD_PLATE_PADDING
    draw.text(
        (title_x, title_y),
        "\u0422\u043e\u043f \u0438\u0433\u0440\u043e\u043a\u043e\u0432",
        font=title_font,
        fill=(255, 255, 255, 255),
    )
    sub_y = title_y + title_height + LEADERBOARD_HEADER_GAP
    draw.text(
        (title_x, sub_y),
        f"\u0412\u0441\u0435\u0433\u043e \u0438\u0433\u0440\u043e\u043a\u043e\u0432: {total_users}",
        font=sub_font,
        fill=(210, 210, 210, 255),
    )

    text_left = plate_x + LEADERBOARD_PLATE_PADDING
    text_right = plate_x + plate_w - LEADERBOARD_PLATE_PADDING
    y = sub_y + sub_height + LEADERBOARD_HEADER_TO_ROWS_GAP

    if not entries:
        draw.text(
            (text_left, y),
            "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u0438\u0433\u0440\u043e\u043a\u043e\u0432.",
            font=entry_font,
            fill=(210, 210, 210, 255),
        )
    else:
        for index, (name, total, avatar_bytes, vip) in enumerate(
            entries, start=1
        ):
            row_top = y
            avatar_x = text_left
            avatar_y = row_top + (row_height - LEADERBOARD_AVATAR_SIZE) // 2
            if avatar_bytes:
                try:
                    avatar_img = Image.open(BytesIO(avatar_bytes)).convert("RGB")
                    avatar_img = ImageOps.fit(
                        avatar_img,
                        (LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE),
                        method=Image.LANCZOS,
                    )
                except Exception:
                    avatar_img = Image.new(
                        "RGB",
                        (LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE),
                        "#2d2d2d",
                    )
            else:
                avatar_img = Image.new(
                    "RGB", (LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE), "#2d2d2d"
                )
            mask = Image.new("L", (LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE), 0)
            ImageDraw.Draw(mask).ellipse(
                (0, 0, LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE), fill=255
            )
            base.paste(avatar_img, (avatar_x, avatar_y), mask)
            if vip:
                border = 5
                draw.ellipse(
                    (
                        avatar_x - border // 2,
                        avatar_y - border // 2,
                        avatar_x + LEADERBOARD_AVATAR_SIZE + border // 2,
                        avatar_y + LEADERBOARD_AVATAR_SIZE + border // 2,
                    ),
                    outline=(255, 215, 0, 255),
                    width=border,
                )

            name_x = avatar_x + LEADERBOARD_AVATAR_SIZE + 20
            value_text = f"{total} \u0440\u0443\u0431."
            value_box = draw.textbbox((0, 0), value_text, font=row_base)
            value_width = value_box[2] - value_box[0]
            value_x = text_right - value_width
            prefix = f"{index}. "
            prefix_width = draw.textlength(prefix, font=row_base)
            vip_tag = "VIP" if vip else ""
            vip_font = pick_font(int(LEADERBOARD_ENTRY_SIZE * 0.6))
            vip_width = (
                draw.textlength(vip_tag, font=vip_font) + 12 if vip else 0
            )
            name_max_width = max(
                0, value_x - 16 - vip_width - (name_x + prefix_width)
            )
            display_name = fit_text_to_width_mixed(
                name or "\u0411\u0435\u0437 \u0438\u043c\u0435\u043d\u0438",
                name_max_width,
                draw,
                row_base,
                row_cjk,
                row_sym,
            )
            text_y = row_top + (row_height - entry_height) // 2
            name_color = (255, 215, 0, 255) if vip else (255, 255, 255, 255)
            draw.text(
                (name_x, text_y),
                prefix,
                font=row_base,
                fill=name_color,
            )
            draw_text_mixed(
                draw,
                (int(name_x + prefix_width), text_y),
                display_name,
                row_base,
                row_cjk,
                row_sym,
                name_color,
            )
            if vip:
                name_width = text_length_mixed(
                    display_name, draw, row_base, row_cjk, row_sym
                )
                vip_x = name_x + prefix_width + name_width + 8
                vip_y = text_y + int(entry_height * 0.12)
                draw.text(
                    (vip_x, vip_y),
                    vip_tag,
                    font=vip_font,
                    fill=(255, 215, 0, 255),
                )
            draw.text(
                (value_x, text_y),
                value_text,
                font=row_base,
                fill=(210, 210, 210, 255),
            )
            y += row_height + LEADERBOARD_ROW_GAP

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_menu_image(title: str, subtitle: Optional[str] = None) -> BytesIO:
    width, height = MENU_IMAGE_WIDTH, MENU_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.6)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_font = pick_font_for_text(title, MENU_TITLE_SIZE)
    subtitle_font = pick_font(MENU_SUBTITLE_SIZE)
    title_box = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_box[2] - title_box[0]
    title_height = title_box[3] - title_box[1]
    title_x = plate_x + (plate_w - title_width) // 2
    if subtitle:
        title_y = plate_y + 60
        subtitle_text = subtitle
        subtitle_box = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
        subtitle_width = subtitle_box[2] - subtitle_box[0]
        subtitle_x = plate_x + (plate_w - subtitle_width) // 2
        subtitle_y = title_y + (title_box[3] - title_box[1]) + 24
        draw.text(
            (title_x, title_y),
            title,
            font=title_font,
            fill=(255, 255, 255, 255),
        )
        draw.text(
            (subtitle_x, subtitle_y),
            subtitle_text,
            font=subtitle_font,
            fill=(210, 210, 210, 255),
        )
    else:
        title_y = plate_y + (plate_h - title_height) // 2 - title_box[1]
        draw.text(
            (title_x, title_y),
            title,
            font=title_font,
            fill=(255, 255, 255, 255),
        )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_kazik_title_image(title: str, subtitle: Optional[str] = None) -> BytesIO:
    width, height = KAZIK_IMAGE_WIDTH, KAZIK_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.65)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_font = pick_font_for_text(title, KAZIK_TITLE_SIZE)
    subtitle_font = pick_font_for_text(subtitle or "", KAZIK_SUBTITLE_SIZE)
    max_text_width = int(plate_w * 0.9)
    title_text = fit_text_to_width(title, max_text_width, title_font, draw)
    subtitle_text = (
        fit_text_to_width(subtitle, max_text_width, subtitle_font, draw)
        if subtitle
        else None
    )

    title_box = draw.textbbox((0, 0), title_text, font=title_font)
    title_w = title_box[2] - title_box[0]
    title_h = title_box[3] - title_box[1]
    if subtitle_text:
        subtitle_box = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
        subtitle_w = subtitle_box[2] - subtitle_box[0]
        subtitle_h = subtitle_box[3] - subtitle_box[1]
        gap = 16
        block_h = title_h + gap + subtitle_h
        start_y = plate_y + (plate_h - block_h) // 2
        title_x = plate_x + (plate_w - title_w) // 2 - title_box[0]
        title_y = start_y - title_box[1]
        subtitle_x = plate_x + (plate_w - subtitle_w) // 2 - subtitle_box[0]
        subtitle_y = start_y + title_h + gap - subtitle_box[1]
        draw.text(
            (title_x, title_y),
            title_text,
            font=title_font,
            fill=(255, 255, 255, 255),
        )
        draw.text(
            (subtitle_x, subtitle_y),
            subtitle_text,
            font=subtitle_font,
            fill=(210, 210, 210, 255),
        )
    else:
        title_x = plate_x + (plate_w - title_w) // 2 - title_box[0]
        title_y = plate_y + (plate_h - title_h) // 2 - title_box[1]
        draw.text(
            (title_x, title_y),
            title_text,
            font=title_font,
            fill=(255, 255, 255, 255),
        )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_kazik_spin_image(
    digits: List[int],
    revealed: int,
    title: Optional[str] = None,
) -> BytesIO:
    width, height = KAZIK_IMAGE_WIDTH, KAZIK_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.65)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    if title and revealed <= 0:
        title_font = pick_font_for_text(title, KAZIK_TITLE_SIZE)
        title_box = draw.textbbox((0, 0), title, font=title_font)
        title_w = title_box[2] - title_box[0]
        title_h = title_box[3] - title_box[1]
        title_x = plate_x + (plate_w - title_w) // 2 - title_box[0]
        title_y = plate_y + (plate_h - title_h) // 2 - title_box[1]
        draw.text(
            (title_x, title_y),
            title,
            font=title_font,
            fill=(255, 255, 255, 255),
        )
        apply_corner_logo(base)
        output = BytesIO()
        base.convert("RGB").save(output, format="JPEG", quality=92)
        output.seek(0)
        return output

    title_offset = 0
    if title:
        title_font = pick_font_for_text(title, KAZIK_SUBTITLE_SIZE)
        title_box = draw.textbbox((0, 0), title, font=title_font)
        title_w = title_box[2] - title_box[0]
        title_h = title_box[3] - title_box[1]
        title_x = plate_x + (plate_w - title_w) // 2 - title_box[0]
        title_y = plate_y + 18 - title_box[1]
        draw.text(
            (title_x, title_y),
            title,
            font=title_font,
            fill=(220, 220, 220, 255),
        )
        title_offset = int(title_h * 0.4) + 12
    slot_w = int((plate_w - 2 * KAZIK_SLOT_GAP) / 3)
    slot_h = int(plate_h * 0.6)
    slot_y = plate_y + (plate_h - slot_h) // 2 + title_offset
    digit_font = pick_font(KAZIK_DIGIT_SIZE)

    for index in range(3):
        slot_x = plate_x + index * (slot_w + KAZIK_SLOT_GAP)
        draw.rounded_rectangle(
            (slot_x, slot_y, slot_x + slot_w, slot_y + slot_h),
            radius=KAZIK_SLOT_RADIUS,
            fill=(15, 15, 15, 210),
        )
        digit_layer = Image.new("RGBA", (slot_w, slot_h), (0, 0, 0, 0))
        digit_draw = ImageDraw.Draw(digit_layer)
        digit_value = (
            digits[index] if index < revealed else random.choice(KAZIK_DIGITS)
        )
        digit_text = str(digit_value)
        text_box = digit_draw.textbbox((0, 0), digit_text, font=digit_font)
        text_w = text_box[2] - text_box[0]
        text_h = text_box[3] - text_box[1]
        text_x = (slot_w - text_w) // 2 - text_box[0]
        text_y = (slot_h - text_h) // 2 - text_box[1]
        digit_draw.text(
            (text_x, text_y),
            digit_text,
            font=digit_font,
            fill=(255, 255, 255, 230),
        )
        if index >= revealed:
            digit_layer = digit_layer.filter(ImageFilter.GaussianBlur(radius=6))
        base.alpha_composite(digit_layer, (slot_x, slot_y))

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def ensure_photo_cache_dir() -> None:
    PHOTO_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def ensure_cached_image(path: Path, builder: Callable[[], BytesIO]) -> Path:
    if not path.exists() or path.stat().st_size == 0:
        ensure_photo_cache_dir()
        image = builder()
        path.write_bytes(image.getvalue())
    return path


def get_cached_menu_image(
    key: str, title: str, subtitle: Optional[str]
) -> Path:
    cache_version = os.getenv("IMAGE_CACHE_VERSION", "v2").strip() or "v2"
    filename = f"menu_{key}_{cache_version}.jpg"
    path = PHOTO_CACHE_DIR / filename
    return ensure_cached_image(path, lambda: build_menu_image(title, subtitle))


def get_cached_kazik_title_image() -> Path:
    cache_version = os.getenv("IMAGE_CACHE_VERSION", "v2").strip() or "v2"
    path = PHOTO_CACHE_DIR / f"kazik_title_{cache_version}.jpg"
    return ensure_cached_image(
        path, lambda: build_kazik_title_image("\u041a\u0430\u0437\u0438\u043d\u043e")
    )


def get_cached_kazik_result_image(
    win: bool, digits: List[int]
) -> Path:
    digits_slug = "-".join(str(digit) for digit in digits)
    suffix = "win" if win else "lose"
    title = "\u0412\u044b\u0438\u0433\u0440\u044b\u0448!" if win else "\u041f\u0440\u043e\u0438\u0433\u0440\u044b\u0448"
    subtitle = f"\u0412\u044b\u043f\u0430\u043b\u043e: {build_kazik_text_line(digits, 3)}"
    cache_version = os.getenv("IMAGE_CACHE_VERSION", "v2").strip() or "v2"
    filename = f"kazik_{suffix}_{digits_slug}_{cache_version}.jpg"
    path = PHOTO_CACHE_DIR / filename
    return ensure_cached_image(
        path, lambda: build_kazik_title_image(title, subtitle)
    )


async def send_or_edit_photo(
    message,
    photo,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup],
    prefer_edit: bool,
    *,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
    owner_id: Optional[int] = None,
    parse_mode: Optional[str] = None,
) -> None:
    animation_extensions = {".gif"}
    video_extensions = {".mp4", ".webm"}

    def rewind_if_possible(payload) -> None:
        try:
            seeker = getattr(payload, "seek", None)
            if callable(seeker):
                seeker(0)
        except Exception:
            return

    def is_payload_empty(payload) -> bool:
        try:
            buffer = getattr(payload, "getbuffer", None)
            if callable(buffer):
                return buffer().nbytes == 0
            tell = getattr(payload, "tell", None)
            seek = getattr(payload, "seek", None)
            if callable(tell) and callable(seek):
                current = tell()
                seek(0, 2)
                size = tell()
                seek(current)
                return size == 0
        except Exception:
            return False
        return False

    name = getattr(photo, "name", "") or ""
    ext = Path(str(name)).suffix.lower()
    if ext in animation_extensions:
        kind = "animation"
        send_kwargs = {
            "animation": photo,
            "caption": caption,
            "reply_markup": reply_markup,
            "parse_mode": parse_mode,
        }
    elif ext in video_extensions:
        kind = "video"
        send_kwargs = {
            "video": photo,
            "caption": caption,
            "reply_markup": reply_markup,
            "parse_mode": parse_mode,
        }
    else:
        kind = "photo"
        send_kwargs = {
            "photo": photo,
            "caption": caption,
            "reply_markup": reply_markup,
            "parse_mode": parse_mode,
        }

    if is_payload_empty(photo):
        if prefer_edit:
            await edit_message_text(
                message,
                caption,
                reply_markup,
                parse_mode=parse_mode,
            )
        else:
            await message.reply_text(
                caption,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
        return

    target_message = message
    if prefer_edit:
        try:
            rewind_if_possible(photo)
            if kind == "animation":
                input_media = InputMediaAnimation(
                    media=photo, caption=caption, parse_mode=parse_mode
                )
            elif kind == "video":
                input_media = InputMediaVideo(
                    media=photo, caption=caption, parse_mode=parse_mode
                )
            else:
                input_media = InputMediaPhoto(
                    media=photo, caption=caption, parse_mode=parse_mode
                )
            target_message = await message.edit_media(
                input_media,
                reply_markup=reply_markup,
            )
        except Exception:
            rewind_if_possible(photo)
            if kind == "animation":
                target_message = await message.reply_animation(**send_kwargs)
            elif kind == "video":
                target_message = await message.reply_video(**send_kwargs)
            else:
                target_message = await message.reply_photo(**send_kwargs)
    else:
        rewind_if_possible(photo)
        if kind == "animation":
            target_message = await message.reply_animation(**send_kwargs)
        elif kind == "video":
            target_message = await message.reply_video(**send_kwargs)
        else:
            target_message = await message.reply_photo(**send_kwargs)
    if context and reply_markup:
        set_message_owner(context.application.bot_data, target_message, owner_id)
    return target_message


async def edit_message_text(
    message,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup],
    *,
    parse_mode: Optional[str] = None,
) -> None:
    try:
        await message.edit_caption(
            caption=text, reply_markup=reply_markup, parse_mode=parse_mode
        )
        return
    except Exception:
        pass
    await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041a\u0440\u0443\u0442\u043a\u0430",
                    callback_data="roll_menu",
                )
            ],
            [
                InlineKeyboardButton(
                    "\u0421\u043e\u0441\u0438\u0441\u043a\u0438",
                    callback_data="sausages_menu",
                )
            ],
            [
                InlineKeyboardButton(
                    "\u0414\u043e\u043d\u0430\u0442",
                    callback_data="donate_menu",
                ),
            ],
            [
                InlineKeyboardButton(
                    "\u0422\u043e\u043f",
                    callback_data="cmd|top",
                )
            ],
        ]
    )


def build_roll_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041e\u0431\u044b\u0447\u043d\u0430\u044f",
                    callback_data="cmd|sosiska",
                ),
            ],
            [
                InlineKeyboardButton(
                    "\u041d\u0430\u0437\u0430\u0434",
                    callback_data="menu",
                )
            ],
        ]
    )


def build_sausages_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041c\u043e\u0438",
                    callback_data="cmd|my",
                ),
                InlineKeyboardButton(
                    "\u041a\u0443\u043f\u0438\u0442\u044c",
                    callback_data="cmd|shop",
                ),
            ],
            [
                InlineKeyboardButton(
                    "\u0422\u0440\u0435\u0439\u0434",
                    callback_data="cmd|trade",
                )
            ],
            [
                InlineKeyboardButton(
                    "\u041d\u0430\u0437\u0430\u0434",
                    callback_data="menu",
                )
            ],
        ]
    )


def build_donate_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("VIP", callback_data="donate_vip"),
                InlineKeyboardButton(
                    "\u0417\u0432\u0451\u0437\u0434\u044b", callback_data="donate_stars"
                ),
            ],
            [InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")],
        ]
    )


def build_donate_stars_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041f\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c",
                    callback_data="donate_stars_topup",
                )
            ],
            [InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")],
        ]
    )


def build_rarity_keyboard(
    prefix: str,
    include_menu: bool = True,
    rarities: Optional[List[str]] = None,
) -> InlineKeyboardMarkup:
    rows = []
    buffer = []
    if rarities is None:
        rarities = RARITY_ORDER
    for rarity in rarities:
        buffer.append(
            InlineKeyboardButton(
                RARITY_NAMES[rarity], callback_data=f"{prefix}|{rarity}"
            )
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    if include_menu:
        rows.append([
            InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")
        ])
    return InlineKeyboardMarkup(rows)


def build_shop_menu_keyboard() -> InlineKeyboardMarkup:
    rarities = list(RARITY_ORDER)
    base = build_rarity_keyboard(
        "shop_rarity",
        include_menu=False,
        rarities=rarities,
    )
    rows = [list(row) for row in base.inline_keyboard]
    rows.append(
        [
            InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def build_inventory_keyboard(
    rarity: str,
    index: int,
    total: int,
    item_id: str,
) -> InlineKeyboardMarkup:
    rows = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton("<", callback_data=f"my_nav|{rarity}|{prev_index}"),
                InlineKeyboardButton(f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(">", callback_data=f"my_nav|{rarity}|{next_index}"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "\u041f\u0440\u043e\u0434\u0430\u0442\u044c",
                callback_data=f"my_sell|{item_id}|{rarity}|{index}",
            ),
            InlineKeyboardButton(
                "\u041f\u043e\u0432\u044b\u0441\u0438\u0442\u044c \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c",
                callback_data=f"my_upgrade|{item_id}|{rarity}|{index}",
            ),
        ]
    )
    rows.append([
        InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="my_menu")
    ])
    return InlineKeyboardMarkup(rows)


def build_shop_keyboard(
    rarity: str, index: int, total: int, *, allow_buy: bool = True
) -> InlineKeyboardMarkup:
    rows = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    "<", callback_data=f"shop_nav|{rarity}|{prev_index}"
                ),
                InlineKeyboardButton(f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(">", callback_data=f"shop_nav|{rarity}|{next_index}"),
            ]
        )
    if allow_buy:
        rows.append(
            [
                InlineKeyboardButton(
                    "\u041a\u0443\u043f\u0438\u0442\u044c",
                    callback_data=f"shop_buy|{rarity}|{index}",
                )
            ]
        )
    rows.append([
        InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="shop_menu")
    ])
    return InlineKeyboardMarkup(rows)


def build_draw_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041f\u0440\u043e\u0434\u0430\u0442\u044c",
                    callback_data=f"draw_sell|{item_id}",
                ),
                InlineKeyboardButton(
                    "\u041f\u043e\u0432\u044b\u0441\u0438\u0442\u044c \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c",
                    callback_data=f"draw_upgrade|{item_id}",
                ),
            ]
        ]
    )


def build_draw_sell_confirm_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u0414\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c",
                    callback_data=f"draw_sell_confirm|{item_id}",
                ),
                InlineKeyboardButton(
                    "\u041e\u0442\u043c\u0435\u043d\u0430",
                    callback_data=f"draw_sell_cancel|{item_id}",
                ),
            ]
        ]
    )


def build_my_sell_confirm_keyboard(
    item_id: str, rarity: str, index: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u0414\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c",
                    callback_data=f"my_sell_confirm|{item_id}|{rarity}|{index}",
                ),
                InlineKeyboardButton(
                    "\u041e\u0442\u043c\u0435\u043d\u0430",
                    callback_data=f"my_sell_cancel|{item_id}|{rarity}|{index}",
                ),
            ]
        ]
    )


def build_upgrade_confirm_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u0414\u0430, \u043f\u043e\u0432\u044b\u0441\u0438\u0442\u044c",
                    callback_data=f"draw_upgrade_confirm|{item_id}",
                ),
                InlineKeyboardButton(
                    "\u041e\u0442\u043c\u0435\u043d\u0430",
                    callback_data=f"draw_upgrade_cancel|{item_id}",
                ),
            ]
        ]
    )


def build_my_upgrade_confirm_keyboard(
    item_id: str, rarity: str, index: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u0414\u0430, \u043f\u043e\u0432\u044b\u0441\u0438\u0442\u044c",
                    callback_data=f"my_upgrade_confirm|{item_id}|{rarity}|{index}",
                ),
                InlineKeyboardButton(
                    "\u041e\u0442\u043c\u0435\u043d\u0430",
                    callback_data=f"my_upgrade_cancel|{item_id}|{rarity}|{index}",
                ),
            ]
        ]
    )


def build_gift_keyboard(token: str) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(
            str(index),
            callback_data=f"gift_pick|{token}|{index}",
        )
        for index in range(1, GIFT_BUTTONS + 1)
    ]
    return InlineKeyboardMarkup([buttons])


def build_kazik_spin_keyboard(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data="kazik_spin")]]
    )


def kazik_daily_key(now: Optional[datetime] = None) -> str:
    if now is None:
        now = now_local()
    return now.date().isoformat()


def kazik_free_spins_limit(user: Dict[str, object]) -> int:
    return 10 if is_vip(user) else 1


def kazik_free_spins_left(user: Dict[str, object], now: Optional[datetime] = None) -> int:
    if now is None:
        now = now_local()
    bonus = int(user.get("kazik_bonus_spins", 0) or 0)
    used = int(user.get("kazik_daily_used", 0) or 0)
    if str(user.get("kazik_daily_date") or "") != kazik_daily_key(now):
        used = 0
    limit = kazik_free_spins_limit(user)
    return bonus + max(0, limit - used)


def kazik_spin_button_label(user: Dict[str, object]) -> str:
    free_left = kazik_free_spins_left(user)
    if free_left > 0:
        return f"\u041f\u043e\u043a\u0440\u0443\u0442\u0438\u0442\u044c (FREE: {free_left})"
    return f"\u041f\u043e\u043a\u0440\u0443\u0442\u0438\u0442\u044c \u0437\u0430 {KAZIK_STAR_SPIN_COST}\u2b50"


def build_stars_menu_keyboard(user: Dict[str, object]) -> InlineKeyboardMarkup:
    rows = []
    buffer = []
    for amount in STARS_TOPUP_AMOUNTS:
        buffer.append(
            InlineKeyboardButton(
                f"\u2b50 {amount} \u0437\u0432\u0451\u0437\u0434",
                callback_data=f"stars_buy|{amount}",
            )
        )
        if len(buffer) == 2:
            rows.append(buffer)
            buffer = []
    if buffer:
        rows.append(buffer)
    rows.append(
        [InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")]
    )
    return InlineKeyboardMarkup(rows)


def build_skidki_keyboard(total: int) -> InlineKeyboardMarkup:
    rows = []
    for idx in range(1, total + 1):
        rows.append(
            [
                InlineKeyboardButton(
                    f"\u041f\u043e\u0441\u043c\u043e\u0442\u0440\u0435\u0442\u044c {idx}",
                    callback_data=f"discount_view|{idx}",
                )
            ]
        )
    rows.append(
        [InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")]
    )
    return InlineKeyboardMarkup(rows)


def build_discount_view_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="skidki_menu")]]
    )


def build_vip_menu_keyboard(user: Dict[str, object]) -> InlineKeyboardMarkup:
    rows = []
    label = "\u041f\u0440\u043e\u0434\u043b\u0438\u0442\u044c VIP" if is_vip(user) else "\u041a\u0443\u043f\u0438\u0442\u044c VIP"
    rows.append([InlineKeyboardButton(label, callback_data="noop")])
    rows.append(
        [
            InlineKeyboardButton(
                f"\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u0431\u0430\u043b\u0430\u043d\u0441\u043e\u043c ({VIP_COST_RUB}\u0440)",
                callback_data="vip_buy_balance",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                f"\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u0437\u0432\u0451\u0437\u0434\u0430\u043c\u0438 ({VIP_COST_STARS}\u2b50)",
                callback_data="vip_buy_stars",
            )
        ]
    )
    rows.append([InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def build_vip_reward_keyboard(index: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton("<", callback_data=f"vip_reward_nav|{prev_index}"),
                InlineKeyboardButton(f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(">", callback_data=f"vip_reward_nav|{next_index}"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "\u0412\u044b\u0431\u0440\u0430\u0442\u044c",
                callback_data=f"vip_reward_pick|{index}",
            )
        ]
    )
    rows.append(
        [InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="cmd|vip")]
    )
    return InlineKeyboardMarkup(rows)


def build_trade_rarity_keyboard(token: str, role: str) -> InlineKeyboardMarkup:
    rows = []
    buffer = []
    for rarity in RARITY_ORDER:
        buffer.append(
            InlineKeyboardButton(
                RARITY_NAMES[rarity], callback_data=f"trade_rarity|{role}|{token}|{rarity}"
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
                    "\u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c \u0442\u0440\u0435\u0439\u0434",
                    callback_data=f"trade_cancel|{token}",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    "\u041e\u0442\u043a\u0430\u0437\u0430\u0442\u044c\u0441\u044f",
                    callback_data=f"trade_decline|{token}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    "\u041d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u0432\u044b\u0431\u0438\u0440\u0430\u0442\u044c",
                    callback_data=f"trade_accept_none|{token}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


def build_trade_accept_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041f\u0440\u0438\u043d\u044f\u0442\u044c \u0442\u0440\u0435\u0439\u0434",
                    callback_data=f"trade_accept_btn|{token}",
                )
            ]
        ]
    )


def trade_user_label(trade: Dict[str, object], role: str) -> str:
    tag = trade.get(f"{role}_tag")
    name = trade.get(f"{role}_name")
    if tag:
        return f"@{tag}"
    if name:
        return str(name)
    user_id = trade.get(f"{role}_id")
    return str(user_id) if user_id else "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c"


def build_trade_confirm_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u041f\u0440\u0438\u043d\u044f\u0442\u044c",
                    callback_data=f"trade_confirm|{token}",
                ),
                InlineKeyboardButton(
                    "\u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c",
                    callback_data=f"trade_confirm_cancel|{token}",
                ),
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
    rows = []
    if total > 1:
        prev_index = (index - 1) % total
        next_index = (index + 1) % total
        rows.append(
            [
                InlineKeyboardButton(
                    "<",
                    callback_data=f"trade_nav|{role}|{token}|{rarity}|{prev_index}",
                ),
                InlineKeyboardButton(f"{index + 1}/{total}", callback_data="noop"),
                InlineKeyboardButton(
                    ">",
                    callback_data=f"trade_nav|{role}|{token}|{rarity}|{next_index}",
                ),
            ]
        )
    action_label = (
        "\u041f\u0440\u0435\u0434\u043b\u043e\u0436\u0438\u0442\u044c \u0442\u0440\u0435\u0439\u0434"
        if role == "offer"
        else "\u041e\u0431\u043c\u0435\u043d\u044f\u0442\u044c"
    )
    rows.append(
        [
            InlineKeyboardButton(
                action_label,
                callback_data=f"trade_pick|{role}|{token}|{item_id}|{rarity}|{index}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                "\u041d\u0430\u0437\u0430\u0434",
                callback_data=f"trade_rarity_menu|{role}|{token}",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)

async def send_main_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    user = ensure_user(db, tg_user)
    rank, total_users = compute_rank(db, card_map, str(tg_user.id))
    total_value = inventory_value(user, card_map)
    balance = get_balance(user)
    stars = get_star_balance(user)
    vip = is_vip(user)
    try:
        avatar_cache = get_avatar_cache(context.application.bot_data)
        avatar_bytes = await asyncio.wait_for(
            fetch_user_avatar_cached(context.bot, tg_user.id, avatar_cache),
            timeout=8,
        )
    except Exception:
        avatar_bytes = None
    profile_image = build_profile_image(
        tg_user.full_name,
        rank,
        total_users,
        total_value,
        balance,
        stars,
        vip,
        avatar_bytes,
    )
    user_label = get_user_label(tg_user)
    caption = apply_pressed_by(
        f"{greeting_by_time()}, {user_label}!\n\u041c\u0435\u043d\u044e \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f",
        pressed_by,
    )
    await send_or_edit_photo(
        message,
        profile_image,
        caption,
        build_main_menu_keyboard(),
        prefer_edit=bool(update.callback_query),
        context=context,
        owner_id=tg_user.id,
    )


async def roll_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if pressed_by is None:
        pressed_by = tg_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    cooldown = get_cooldown_seconds(user)
    last_roll = parse_iso(user.get("last_roll_at"))
    roll_left = 0
    if last_roll:
        diff = now_utc() - last_roll
        roll_left = max(0, cooldown - int(diff.total_seconds()))
    now = now_local()
    free_left = kazik_free_spins_left(user, now)
    reset_in = max(
        0,
        int(
            (
                (now + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                - now
            ).total_seconds()
        ),
    )
    roll_line = (
        f"\u0414\u043e \u0441\u043b\u0435\u0434. \u043a\u0440\u0443\u0442\u043a\u0438: {format_duration(roll_left)}"
        if roll_left > 0
        else "\u0414\u043e \u0441\u043b\u0435\u0434. \u043a\u0440\u0443\u0442\u043a\u0438: \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e"
    )
    caption_lines = [
        "\u041a\u0440\u0443\u0442\u043a\u0430",
        roll_line,
        f"\u041a\u0430\u0437\u0438\u043a \u0444\u0440\u0438-\u0441\u043f\u0438\u043d\u043e\u0432: {free_left}",
        f"\u0421\u0431\u0440\u043e\u0441 \u041a\u0430\u0437\u0438\u043a\u0430: {format_duration(reset_in)}",
        f"\u041a\u0430\u0437\u0438\u043a \u043f\u043e\u0441\u043b\u0435 \u0444\u0440\u0438: {KAZIK_STAR_SPIN_COST}\u2b50",
    ]
    menu_path = get_cached_menu_image(
        "roll",
        "\u041a\u0440\u0443\u0442\u043a\u0430",
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0436\u0438\u043c",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            apply_pressed_by("\n".join(caption_lines), pressed_by),
            build_roll_menu_keyboard(),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def sausages_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if pressed_by is None:
        pressed_by = tg_user
    caption = apply_pressed_by(
        "\u0421\u043e\u0441\u0438\u0441\u043a\u0438",
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "sausages",
        "\u0421\u043e\u0441\u0438\u0441\u043a\u0438",
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u043c\u0435\u043d\u044e",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_sausages_menu_keyboard(),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def donate_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    stars = get_star_balance(user)
    now = now_utc()
    vip_until = parse_iso(user.get("vip_until"))
    if vip_until and vip_until > now:
        left = int((vip_until - now).total_seconds())
        status = f"VIP (\u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {format_duration(left)})"
    else:
        status = "\u043d\u0435\u0442"
    caption = apply_pressed_by(
        "\n".join(
            [
                "\u0414\u043e\u043d\u0430\u0442",
                f"\u0421\u0442\u0430\u0442\u0443\u0441 VIP: {status}",
                f"\u0417\u0432\u0451\u0437\u0434 \u043d\u0430 \u0431\u0430\u043b\u0430\u043d\u0441\u0435: {format_stars(stars)}",
            ]
        ),
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "donate",
        "\u0414\u043e\u043d\u0430\u0442",
        "VIP \u0438 \u0417\u0432\u0451\u0437\u0434\u044b",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_donate_menu_keyboard(),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def donate_stars_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    stars = get_star_balance(user)
    caption = apply_pressed_by(
        "\n".join(
            [
                f"\u0417\u0432\u0451\u0437\u0434 \u043d\u0430 \u0431\u0430\u043b\u0430\u043d\u0441\u0435: {format_stars(stars)}",
                "\u041d\u0430\u0436\u043c\u0438 \u00ab\u041f\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c\u00bb \u0438 \u0432\u0432\u0435\u0434\u0438 \u043a\u043e\u043b-\u0432\u043e \u0437\u0432\u0451\u0437\u0434 (\u043c\u0438\u043d. 25).",
            ]
        ),
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "donate_stars",
        "\u0417\u0432\u0451\u0437\u0434\u044b",
        "\u041f\u043e\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0435",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_donate_stars_keyboard(),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return

    db = context.application.bot_data["db"]
    users = db.setdefault("users", {})
    user_id = str(tg_user.id)
    is_new_user = user_id not in users
    user = ensure_user(db, tg_user)

    note_lines = []
    changed = is_new_user

    if message.chat and message.chat.type == "private" and is_new_user:
        payload = context.args[0] if getattr(context, "args", None) else ""
        referrer_id = parse_referrer_id(payload)
        if (
            referrer_id
            and referrer_id != user_id
            and not user.get("referred_by")
            and isinstance(users.get(referrer_id), dict)
        ):
            referrer = users[referrer_id]
            user["referred_by"] = referrer_id
            user["kazik_bonus_spins"] = int(user.get("kazik_bonus_spins", 0) or 0) + 1
            referrer["kazik_bonus_spins"] = (
                int(referrer.get("kazik_bonus_spins", 0) or 0) + 1
            )
            changed = True
            note_lines.append(
                "\u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u0440\u0435\u0444\u0435\u0440\u0430\u043b: +1 \u0444\u0440\u0438 \u0441\u043f\u0438\u043d \u0432 \u041a\u0430\u0437\u0438\u043a\u0435."
            )
            try:
                await context.bot.send_message(
                    chat_id=int(referrer_id),
                    text=(
                        "\u041f\u043e \u0442\u0432\u043e\u0435\u0439 \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u0437\u0430\u0448\u0451\u043b "
                        f"{get_user_label(tg_user)}. +1 \u0444\u0440\u0438 \u0441\u043f\u0438\u043d \u0432 \u041a\u0430\u0437\u0438\u043a\u0435."
                    ),
                )
            except Exception:
                pass

    if changed:
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)

    await send_main_menu(update, context)
    if note_lines and message.chat and message.chat.type == "private":
        await message.reply_text("\n".join(note_lines))


async def ref_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return
    if message.chat and message.chat.type != "private":
        return

    username = get_public_bot_username(context)
    if not username:
        await message.reply_text(
            "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0438\u0442\u044c username \u0431\u043e\u0442\u0430. \u0423\u043a\u0430\u0436\u0438 PUBLIC_BOT_USERNAME \u0432 .env."
        )
        return

    link = f"https://t.me/{username}?start=ref_{tg_user.id}"
    await message.reply_text(
        "\n".join(
            [
                "\u0422\u0432\u043e\u044f \u0440\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430:",
                link,
                "",
                "\u0415\u0441\u043b\u0438 \u043a\u0442\u043e-\u0442\u043e \u0437\u0430\u0439\u0434\u0451\u0442 \u043f\u043e \u043d\u0435\u0439, \u0442\u043e \u0432\u044b \u043e\u0431\u0430 \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 +1 \u0444\u0440\u0438 \u0441\u043f\u0438\u043d \u0432 \u041a\u0430\u0437\u0438\u043a\u0435.",
            ]
        ),
        disable_web_page_preview=True,
    )


async def broadcast_text_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return
    admin_raw = os.getenv("ADMIN_BROADCAST_USER_ID", "6603471853").strip() or "6603471853"
    try:
        admin_id = int(admin_raw)
    except ValueError:
        admin_id = 6603471853
    if tg_user.id != admin_id:
        return
    raw_text = message.text or ""
    text = raw_text.partition(" ")[2].strip()
    if not text:
        await message.reply_text("\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: /text <\u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435>")
        return
    db = context.application.bot_data["db"]
    users = db.get("users", {})
    if not isinstance(users, dict) or not users:
        await message.reply_text("\u041d\u0435\u0442 \u044e\u0437\u0435\u0440\u043e\u0432 \u0432 \u0431\u0430\u0437\u0435.")
        return
    user_ids = list(users.keys())
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=int(uid), text=text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.04)
    await message.reply_text(f"\u0413\u043e\u0442\u043e\u0432\u043e. \u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e: {sent}, \u043e\u0448\u0438\u0431\u043e\u043a: {failed}.")


async def text_input_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user or not message.text:
        return
    if message.chat and message.chat.type != "private":
        return

    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    mode = str(user.get("input_mode") or "")
    if mode != "stars_topup":
        return

    text = message.text.strip()
    lowered = text.lower()
    if lowered in {"\u043e\u0442\u043c\u0435\u043d\u0430", "cancel"}:
        user["input_mode"] = None
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await message.reply_text("\u041e\u0442\u043c\u0435\u043d\u0435\u043d\u043e.")
        return

    try:
        amount = int(text)
    except ValueError:
        await message.reply_text(
            "\u0412\u0432\u0435\u0434\u0438 \u0447\u0438\u0441\u043b\u043e (\u043c\u0438\u043d\u0438\u043c\u0443\u043c 25) \u0438\u043b\u0438 \u00ab\u043e\u0442\u043c\u0435\u043d\u0430\u00bb."
        )
        return
    if amount < 25:
        await message.reply_text("\u041c\u0438\u043d\u0438\u043c\u0443\u043c 25.")
        return

    user["input_mode"] = None
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    await send_stars_invoice(message, amount)


async def sosiska_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    by_rarity = context.application.bot_data["cards_by_rarity"]
    drop_chances = context.application.bot_data["drop_chances"]
    user = ensure_user(db, tg_user)
    if not is_vip(user):
        drop_chances = boost_drop_chances(
            drop_chances, NON_VIP_DROP_NERF_RARITIES, NON_VIP_DROP_CHANCE_MULTIPLIER
        )

    free_rolls = int(user.get("free_rolls", 0))
    use_free_roll = free_rolls > 0
    if not use_free_roll:
        cooldown = get_cooldown_seconds(user)
        last_roll = parse_iso(user.get("last_roll_at"))
        if last_roll:
            diff = now_utc() - last_roll
            if diff.total_seconds() < cooldown:
                left = cooldown - int(diff.total_seconds())
                await message.reply_text(
                    apply_pressed_by(
                        f"\u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0430\u044f \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0447\u0435\u0440\u0435\u0437 {format_duration(left)}.",
                        pressed_by,
                    ),
                )
                return

    available_by_rarity = filter_existing_cards(by_rarity)
    card = pick_random_card(available_by_rarity, drop_chances)
    if not card:
        await message.reply_text(
            apply_pressed_by(
                "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a. \u0414\u043e\u0431\u0430\u0432\u044c \u0444\u043e\u0442\u043e \u0432 \u043f\u0430\u043f\u043a\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0435\u0439.",
                pressed_by,
            )
        )
        return

    path = get_card_media_path(card)
    if not path.exists():
        await message.reply_text(
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            )
        )
        return

    item = make_inventory_item(card.file)
    user["inventory"].append(item)
    user["last_roll_at"] = now_utc().isoformat()
    if use_free_roll:
        user["free_rolls"] = max(0, free_rolls - 1)

    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)

    user_label = get_user_label(tg_user)
    caption = apply_pressed_by(build_draw_caption(user_label, card), pressed_by)
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_draw_keyboard(item["id"]),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        ) 


async def my_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    caption = apply_pressed_by(
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c:",
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "my",
        "\u041c\u043e\u0438 \u0441\u043e\u0441\u0438\u0441\u043a\u0438",
        None,
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_rarity_keyboard("my_rarity"),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def shop_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    caption = apply_pressed_by(
        "\u041c\u0430\u0433\u0430\u0437\u0438\u043d. \u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c:",
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "shop",
        "\u041c\u0430\u0433\u0430\u0437\u0438\u043d",
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_shop_menu_keyboard(),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def skidki_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    discounts = await ensure_discounts(context)
    items = discounts.get("items", [])
    if not isinstance(items, list) or not items:
        await message.reply_text(
            apply_pressed_by(
                "\u0421\u0435\u0439\u0447\u0430\u0441 \u0430\u043a\u0446\u0438\u0439 \u043d\u0435\u0442.",
                pressed_by,
            )
        )
        return
    lines = []
    for index, item in enumerate(items, start=1):
        filename = str(item.get("file") or "")
        card = card_map.get(filename)
        rarity = item.get("rarity") or (card.rarity if card else "common")
        title = card_display_name(card) if card else filename
        original_price = int(item.get("original_price", 0))
        discounted = int(item.get("discount_price", 0))
        remaining = int(item.get("remaining", 0))
        status = (
            f"\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: {remaining}"
            if remaining > 0
            else "(\u0440\u0430\u0441\u043a\u0443\u043f\u0438\u043b\u0438)"
        )
        if card:
            label = escape_html(format_card_label(card))
            new_price = format_short_amount(discounted, card_currency(card))
            old_price = format_short_amount(original_price, card_currency(card))
        else:
            label = escape_html(f"({RARITY_NAMES.get(rarity, rarity)}) {title}")
            new_price = format_short_amount(discounted, "rub")
            old_price = format_short_amount(original_price, "rub")
        line = f"{index}. {label} - {format_price_with_old_html(new_price, old_price, italic_old=True)} \u2014 {status}"
        lines.append(line)
    caption = apply_pressed_by("\n".join(lines), pressed_by)
    menu_path = get_cached_menu_image(
        "skidki",
        "\u0421\u043a\u0438\u0434\u043a\u0438",
        "\u0410\u043a\u0446\u0438\u0438 \u0434\u043d\u044f",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_skidki_keyboard(len(items)),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
            parse_mode=ParseMode.HTML,
        )


async def gift_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)

    last_play = parse_iso(user.get("last_kazik_at"))
    if last_play:
        diff = now_utc() - last_play
        if diff.total_seconds() < GIFT_COOLDOWN_SEC:
            left = GIFT_COOLDOWN_SEC - int(diff.total_seconds())
            await message.reply_text(
                apply_pressed_by(
                    f"\u041f\u043e\u0434\u0430\u0440\u043e\u043a \u0431\u0443\u0434\u0435\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0447\u0435\u0440\u0435\u0437 {format_duration(left)}.",
                    pressed_by,
                )
            )
            return

    token = secrets.token_urlsafe(6)
    session = {
        "token": token,
        "win_index": random.randint(1, GIFT_BUTTONS),
        "used": False,
        "created_at": now_utc().isoformat(),
    }
    user["kazik_session"] = session
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)

    sent = await message.reply_text(
        apply_pressed_by(
            "\u041f\u043e\u0434\u0430\u0440\u043e\u043a: \u0432\u044b\u0431\u0435\u0440\u0438 \u043a\u043d\u043e\u043f\u043a\u0443.",
            pressed_by,
        ),
        reply_markup=build_gift_keyboard(token),
    )
    set_message_owner(context.application.bot_data, sent, tg_user.id)


async def kazik_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    now = now_local()
    free_left = kazik_free_spins_left(user, now)
    reset_in = max(
        0,
        int(
            (
                (now + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                - now
            ).total_seconds()
        ),
    )
    lines = [
        f"\u0424\u0440\u0438 \u0441\u043f\u0438\u043d\u043e\u0432: {free_left}",
        f"\u0421\u0431\u0440\u043e\u0441 \u0447\u0435\u0440\u0435\u0437: {format_duration(reset_in)}",
        f"\u041f\u043e\u0441\u043b\u0435 \u0444\u0440\u0438: {KAZIK_STAR_SPIN_COST}\u2b50",
    ]
    caption = apply_pressed_by("\n".join(lines), pressed_by)
    image_path = get_cached_kazik_title_image()
    label = kazik_spin_button_label(user)
    with image_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_kazik_spin_keyboard(label),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def stars_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
    note: Optional[str] = None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    stars = get_star_balance(user)
    lines = []
    if note:
        lines.append(note)
    lines.append(f"\u0417\u0432\u0451\u0437\u0434 \u043d\u0430 \u0431\u0430\u043b\u0430\u043d\u0441\u0435: {format_stars(stars)}")
    caption = apply_pressed_by("\n".join(lines), pressed_by)
    menu_path = get_cached_menu_image(
        "stars",
        "\u0417\u0432\u0451\u0437\u0434\u044b",
        "\u041f\u043e\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0435 \u0431\u0430\u043b\u0430\u043d\u0441\u0430",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_stars_menu_keyboard(user),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def vip_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
    note: Optional[str] = None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    lines = []
    if note:
        lines.append(note)
    now = now_utc()
    vip_until = parse_iso(user.get("vip_until"))
    if vip_until and vip_until > now:
        left = int((vip_until - now).total_seconds())
        lines.append(
            f"\u0421\u0442\u0430\u0442\u0443\u0441: VIP (\u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {format_duration(left)})"
        )
    else:
        lines.append("\u0421\u0442\u0430\u0442\u0443\u0441: \u043d\u0435\u0442 VIP")
    lines.extend(
        [
            "VIP \u0434\u0430\u0451\u0442:",
            "- \u0411\u044b\u0441\u0442\u0440\u0435\u0435 \u043e\u0442\u043a\u0430\u0442 \u043a\u0440\u0443\u0442\u043a\u0438",
            "- \u0411\u043e\u043b\u044c\u0448\u0435 \u0444\u0440\u0438-\u0441\u043f\u0438\u043d\u043e\u0432 \u0432 \u041a\u0430\u0437\u0438\u043a\u0435",
            "- \u041f\u043e\u0432\u044b\u0448\u0435\u043d\u043d\u044b\u0439 \u0448\u0430\u043d\u0441 \u043d\u0430 \u0440\u0435\u0434\u043a\u0438\u0435 \u0441\u043e\u0441\u0438\u0441\u043a\u0438",
            f"\u0421\u0440\u043e\u043a: {VIP_DURATION_DAYS} \u0434\u043d\u0435\u0439",
        ]
    )
    lines.append(
        f"\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c: {VIP_COST_RUB}\u0440 \u0438\u043b\u0438 {VIP_COST_STARS}\u2b50"
    )
    caption = apply_pressed_by("\n".join(lines), pressed_by)
    menu_path = get_cached_menu_image(
        "vip",
        "VIP",
        "\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0430",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_vip_menu_keyboard(user),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def rozigrish_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    if message and getattr(message, "chat", None) and message.chat.type != "private":
        await message.reply_text(
            apply_pressed_by(
                "\u0420\u043e\u0437\u044b\u0433\u0440\u044b\u0448 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u043b\u0438\u0447\u043a\u0435 \u0441 \u0431\u043e\u0442\u043e\u043c.",
                pressed_by,
            )
        )
        return
    now = now_local()
    giveaway = await ensure_giveaway(context)
    phase = giveaway_phase(now)
    if phase == "announce" and giveaway.get("status") != "announced":
        await announce_giveaway(context, giveaway)
        giveaway = load_giveaway_data()
    if phase == "idle":
        await message.reply_text(
            apply_pressed_by(
                "\u0420\u043e\u0437\u044b\u0433\u0440\u044b\u0448 \u0435\u0449\u0451 \u043d\u0435 \u043d\u0430\u0447\u0430\u043b\u0441\u044f.",
                pressed_by,
            )
        )
        return
    if phase == "closed":
        await message.reply_text(
            apply_pressed_by(
                "\u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044f \u0437\u0430\u043a\u0440\u044b\u0442\u0430.",
                pressed_by,
            )
        )
        return
    if phase == "announce":
        status_text = (
            "\u0418\u0442\u043e\u0433\u0438 \u0443\u0436\u0435 \u043e\u0433\u043b\u0430\u0448\u0435\u043d\u044b."
            if giveaway.get("status") == "announced"
            else "\u0418\u0434\u0451\u0442 \u043f\u043e\u0434\u0432\u0435\u0434\u0435\u043d\u0438\u0435 \u0438\u0442\u043e\u0433\u043e\u0432."
        )
        await message.reply_text(apply_pressed_by(status_text, pressed_by))
        return

    lock = context.application.bot_data.setdefault("giveaway_lock", asyncio.Lock())
    added = False
    async with lock:
        giveaway = load_giveaway_data() or giveaway
        if giveaway.get("date") != giveaway_day_key(now):
            giveaway = create_giveaway(context.application.bot_data["cards_by_rarity"])
        entries = giveaway.setdefault("entries", [])
        uid = str(tg_user.id)
        if uid not in entries:
            entries.append(uid)
            added = True
        giveaway["status"] = "open"
        save_giveaway_data(giveaway)
    if added:
        reply = "\u0422\u044b \u0443\u0447\u0430\u0441\u0442\u0432\u0443\u0435\u0448\u044c \u0432 \u0440\u043e\u0437\u044b\u0433\u0440\u044b\u0448\u0435!"
    else:
        reply = "\u0422\u044b \u0443\u0436\u0435 \u0443\u0447\u0430\u0441\u0442\u0432\u0443\u0435\u0448\u044c \u0432 \u0440\u043e\u0437\u044b\u0433\u0440\u044b\u0448\u0435!"
    await message.reply_text(apply_pressed_by(reply, pressed_by))


async def send_stars_invoice(message, amount: int) -> None:
    payload = build_stars_payload(amount)
    provider_token = os.getenv("STARS_PROVIDER_TOKEN", "").strip()
    await message.reply_invoice(
        title=f"{amount} \u0437\u0432\u0451\u0437\u0434",
        description="\u041f\u043e\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0435 \u0431\u0430\u043b\u0430\u043d\u0441\u0430 \u0437\u0432\u0451\u0437\u0434.",
        payload=payload,
        provider_token=provider_token,
        currency=STARS_CURRENCY,
        prices=[LabeledPrice(label=f"{amount} \u2b50", amount=amount)],
    )


async def vip_reward_menu_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    by_rarity = context.application.bot_data["cards_by_rarity"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    if user.get("vip_reward_pending"):
        user["vip_reward_pending"] = False
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await message.reply_text(
            apply_pressed_by(
                "VIP \u043d\u0430\u0433\u0440\u0430\u0434\u044b \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u044b. \u042d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u044b \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0435.",
                pressed_by,
            )
        )
        return
    if not user.get("vip_reward_pending"):
        await message.reply_text(
            apply_pressed_by(
                "\u0423 \u0442\u0435\u0431\u044f \u043d\u0435\u0442 \u043d\u0430\u0433\u0440\u0430\u0434\u044b VIP.",
                pressed_by,
            )
        )
        return
    cards = filter_existing_cards(by_rarity).get("exclusive", [])
    if not cards:
        await message.reply_text(
            apply_pressed_by(
                "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u043d\u044b\u0445 \u0441\u043e\u0441\u0438\u0441\u043e\u043a.",
                pressed_by,
            )
        )
        return
    await show_vip_reward_card(
        message,
        cards,
        0,
        pressed_by=pressed_by,
        prefer_edit=bool(update.callback_query),
        context=context,
    )


async def show_vip_reward_card(
    message,
    cards: List[Card],
    index: int,
    pressed_by=None,
    prefer_edit: bool = True,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    caption = apply_pressed_by(
        "\n".join(
            [
                "VIP \u043d\u0430\u0433\u0440\u0430\u0434\u0430: \u0432\u044b\u0431\u0435\u0440\u0438 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u043d\u0443\u044e \u0441\u043e\u0441\u0438\u0441\u043a\u0443.",
                f"{card_display_name(card)}",
                f"\u0420\u0435\u0434\u043a\u043e\u0441\u0442\u044c: {RARITY_NAMES[card.rarity]} | \u0426\u0435\u043d\u0430: {format_card_price(card)}",
                f"{index + 1}/{len(cards)}",
            ]
        ),
        pressed_by,
    )
    keyboard = build_vip_reward_keyboard(index, len(cards))
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=prefer_edit,
            context=context,
            owner_id=pressed_by.id if pressed_by else None,
        )


async def edit_vip_reward_card(
    message,
    cards: List[Card],
    index: int,
    pressed_by=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    await show_vip_reward_card(
        message,
        cards,
        index,
        pressed_by=pressed_by,
        prefer_edit=True,
        context=context,
    )


async def vip_purchase_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    await vip_purchase_with_stars(update, context, pressed_by=pressed_by)


def compute_vip_until(user: Dict[str, object], now: datetime) -> datetime:
    current = parse_iso(user.get("vip_until"))
    base = current if current and current > now else now
    return base + timedelta(days=VIP_DURATION_DAYS)


async def vip_purchase_with_stars(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    stars = get_star_balance(user)
    if stars < VIP_COST_STARS:
        await message.reply_text(
            apply_pressed_by(
                f"\u041d\u0443\u0436\u043d\u043e {VIP_COST_STARS}\u2b50 \u0434\u043b\u044f VIP.",
                pressed_by,
            ),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u0417\u0432\u0451\u0437\u0434\u044b", callback_data="donate_stars")]]
            ),
        )
        return
    now = now_utc()
    user["stars"] = stars - VIP_COST_STARS
    user["vip_until"] = compute_vip_until(user, now).isoformat()
    user["vip"] = True
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    left = int((parse_iso(user.get("vip_until")) - now).total_seconds())
    await message.reply_text(
        apply_pressed_by(
            f"\u2705 VIP \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u043e\u0432\u0430\u043d! \u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {format_duration(left)}.",
            pressed_by,
        )
    )
    await vip_menu_command(update, context, pressed_by=pressed_by)


async def vip_purchase_with_balance(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    balance = get_balance(user)
    if balance < VIP_COST_RUB:
        await message.reply_text(
            apply_pressed_by(
                f"\u041d\u0443\u0436\u043d\u043e {VIP_COST_RUB}\u0440 \u043d\u0430 \u0431\u0430\u043b\u0430\u043d\u0441\u0435 \u0434\u043b\u044f VIP.",
                pressed_by,
            )
        )
        return
    now = now_utc()
    user["balance"] = balance - VIP_COST_RUB
    user["vip_until"] = compute_vip_until(user, now).isoformat()
    user["vip"] = True
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    left = int((parse_iso(user.get("vip_until")) - now).total_seconds())
    await message.reply_text(
        apply_pressed_by(
            f"\u2705 VIP \u0430\u043a\u0442\u0438\u0432\u0438\u0440\u043e\u0432\u0430\u043d! \u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {format_duration(left)}.",
            pressed_by,
        )
    )
    await vip_menu_command(update, context, pressed_by=pressed_by)


async def trade_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    if update.callback_query or not context.args:
        await message.reply_text(
            apply_pressed_by(
                "\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: /trade @username",
                pressed_by,
            )
        )
        return
    target_raw = context.args[0]
    target = find_user_by_tag(db, target_raw)
    if not target:
        await message.reply_text(
            apply_pressed_by(
                "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d. \u041f\u0443\u0441\u0442\u044c \u043e\u043d \u043d\u0430\u043f\u0438\u0448\u0435\u0442 \u0431\u043e\u0442\u0443 /start.",
                pressed_by,
            )
        )
        return
    target_id, target_data = target
    if target_id == str(tg_user.id):
        await message.reply_text(
            apply_pressed_by(
                "\u041d\u0435\u043b\u044c\u0437\u044f \u0442\u0440\u0435\u0439\u0434\u0438\u0442\u044c \u0441 \u0441\u0430\u043c\u0438\u043c \u0441\u043e\u0431\u043e\u0439.",
                pressed_by,
            )
        )
        return
    if not user.get("inventory"):
        await message.reply_text(
            apply_pressed_by(
                "\u0423 \u0442\u0435\u0431\u044f \u043d\u0435\u0442 \u0441\u043e\u0441\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u0442\u0440\u0435\u0439\u0434\u0430.",
                pressed_by,
            )
        )
        return
    token = secrets.token_urlsafe(6)
    trade = {
        "token": token,
        "from_id": str(tg_user.id),
        "from_name": user.get("username", ""),
        "from_tag": user.get("user_tag", ""),
        "from_item_id": None,
        "to_id": target_id,
        "to_name": target_data.get("username", ""),
        "to_tag": target_data.get("user_tag", ""),
        "to_item_id": None,
        "status": "draft",
        "created_at": now_utc().isoformat(),
    }
    db.setdefault("trades", {})[token] = trade
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    target_label = (
        f"@{trade['to_tag']}" if trade.get("to_tag") else trade.get("to_name")
    )
    caption = apply_pressed_by(
        "\n".join(
            [
                f"\u0422\u0440\u0435\u0439\u0434 \u0434\u043b\u044f {target_label}",
                "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c \u0434\u043b\u044f \u043f\u0440\u0435\u0434\u043b\u043e\u0436\u0435\u043d\u0438\u044f.",
            ]
        ),
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "trade",
        "\u0422\u0440\u0435\u0439\u0434",
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_trade_rarity_keyboard(token, "offer"),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def trade_accept_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    user = ensure_user(db, tg_user)
    if pressed_by is None:
        pressed_by = tg_user
    if not context.args:
        await message.reply_text(
            apply_pressed_by(
                "\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439: /trade_accept <\u043a\u043e\u0434>",
                pressed_by,
            )
        )
        return
    token = context.args[0].strip()
    trade = db.get("trades", {}).get(token)
    if not trade or trade.get("status") != "open":
        await message.reply_text(
            apply_pressed_by(
                "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0438\u043b\u0438 \u0437\u0430\u043a\u0440\u044b\u0442.",
                pressed_by,
            )
        )
        return
    if trade.get("from_id") == str(tg_user.id):
        await message.reply_text(
            apply_pressed_by(
                "\u041d\u0435\u043b\u044c\u0437\u044f \u043f\u0440\u0438\u043d\u044f\u0442\u044c \u0441\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                pressed_by,
            )
        )
        return
    if trade.get("to_id") and trade.get("to_id") != str(tg_user.id):
        await message.reply_text(
            apply_pressed_by(
                "\u042d\u0442\u043e\u0442 \u0442\u0440\u0435\u0439\u0434 \u043f\u0440\u0435\u0434\u043d\u0430\u0437\u043d\u0430\u0447\u0435\u043d \u0434\u0440\u0443\u0433\u043e\u043c\u0443 \u0438\u0433\u0440\u043e\u043a\u0443.",
                pressed_by,
            )
        )
        return
    if not trade.get("to_id"):
        trade["to_id"] = str(tg_user.id)
    trade["status"] = "accepting"
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    from_id = trade.get("from_id")
    from_user = db.get("users", {}).get(from_id, {})
    offered_item = find_inventory_item(from_user, trade.get("from_item_id", ""))
    offered_card = card_map.get(offered_item.get("file")) if offered_item else None
    offer_text = (
        f"\u0422\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0448\u044c: {card_display_name(offered_card)}"
        if offered_card
        else "\u0422\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0448\u044c \u0441\u043e\u0441\u0438\u0441\u043a\u0443 \u0438\u0437 \u0442\u0440\u0435\u0439\u0434\u0430."
    )
    caption = apply_pressed_by(
        "\n".join(
            [
                f"\u0422\u0440\u0435\u0439\u0434 \u043e\u0442: {trade.get('from_name') or from_id}",
                offer_text,
                "\u0412\u044b\u0431\u0435\u0440\u0438 \u0441\u0432\u043e\u044e \u0441\u043e\u0441\u0438\u0441\u043a\u0443 \u0434\u043b\u044f \u043e\u0431\u043c\u0435\u043d\u0430 \u0438\u043b\u0438 \u043d\u0430\u0436\u043c\u0438 \u00ab\u041d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u0432\u044b\u0431\u0438\u0440\u0430\u0442\u044c\u00bb.",
            ]
        ),
        pressed_by,
    )
    menu_path = get_cached_menu_image(
        "trade_accept",
        "\u0422\u0440\u0435\u0439\u0434",
        "\u0412\u044b\u0431\u0435\u0440\u0438 \u0441\u043e\u0441\u0438\u0441\u043a\u0443",
    )
    with menu_path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            build_trade_rarity_keyboard(token, "accept"),
            prefer_edit=bool(update.callback_query),
            context=context,
            owner_id=tg_user.id,
        )


async def show_trade_card(
    message,
    user: Dict[str, object],
    card_map: Dict[str, Card],
    token: str,
    role: str,
    rarity: str,
    index: int,
    pressed_by=None,
    prefer_edit: bool = True,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    items = filter_inventory_by_rarity(user, card_map, rarity)
    if not items:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0423 \u0442\u0435\u0431\u044f \u043d\u0435\u0442 \u0441\u043e\u0441\u0438\u0441\u043e\u043a \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438.",
                pressed_by,
            ),
            build_trade_rarity_keyboard(token, role),
        )
        return
    index = max(0, min(index, len(items) - 1))
    item = items[index]
    card = card_map.get(item.get("file"))
    if not card:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435.",
                pressed_by,
            ),
            None,
        )
        return
    caption = apply_pressed_by(
        build_inventory_caption(card, index, len(items)), pressed_by
    )
    keyboard = build_trade_item_keyboard(
        token, role, rarity, index, len(items), item["id"]
    )
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=prefer_edit,
            context=context,
            owner_id=pressed_by.id if pressed_by else None,
        )


async def edit_trade_card(
    message,
    user: Dict[str, object],
    card_map: Dict[str, Card],
    token: str,
    role: str,
    rarity: str,
    index: int,
    pressed_by=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    await show_trade_card(
        message,
        user,
        card_map,
        token,
        role,
        rarity,
        index,
        pressed_by=pressed_by,
        prefer_edit=True,
        context=context,
    )


async def top_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    pressed_by=None,
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    ensure_user(db, tg_user)
    entries, total_users = compute_leaderboard(db, card_map, TOP_LIMIT)
    avatar_cache = get_avatar_cache(context.application.bot_data)
    async def fetch_avatar_safe(uid: str) -> Optional[bytes]:
        try:
            user_id = int(uid)
        except (TypeError, ValueError):
            return None
        try:
            return await asyncio.wait_for(
                fetch_user_avatar_cached(context.bot, user_id, avatar_cache),
                timeout=8,
            )
        except Exception:
            return None

    avatars = await asyncio.gather(
        *(fetch_avatar_safe(uid) for uid, _, _, _ in entries)
    )
    leaderboard_entries = [
        (name, total, avatar_bytes, vip)
        for (_, name, total, vip), avatar_bytes in zip(entries, avatars)
    ]
    leaderboard_image = build_leaderboard_image(leaderboard_entries, total_users)
    await send_or_edit_photo(
        message,
        leaderboard_image,
        apply_pressed_by(
            "\u0422\u043e\u043f \u0438\u0433\u0440\u043e\u043a\u043e\u0432",
            pressed_by,
        ),
        InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="menu")]]
        ),
        prefer_edit=bool(update.callback_query),
        context=context,
        owner_id=tg_user.id,
    )


def build_inventory_caption(card: Card, index: int, total: int) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    sale_text = format_short_amount(calc_sale_price(card), card_currency(card))
    return "\n".join(
        [
            f"{format_card_label(card)} - {price_text}",
            f"\u0426\u0435\u043d\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438: {sale_text}",
            f"{index + 1}/{total}",
        ]
    )


def build_shop_caption(
    card: Card,
    index: int,
    total: int,
    discount: Optional[Dict[str, object]] = None,
    exclusive_stock: Optional[Tuple[int, int]] = None,
) -> str:
    price_text = format_short_amount(card.price, card_currency(card))
    label = escape_html(format_card_label(card))
    lines = []
    if discount and is_discount_active(discount):
        percent = int(discount.get("percent", 0))
        discounted = int(discount.get("discount_price", card.price or 0))
        remaining = int(discount.get("remaining", 0))
        new_price = format_short_amount(discounted, card_currency(card))
        old_price = format_short_amount(card.price, card_currency(card))
        lines.append(f"{label} - {format_price_with_old_html(new_price, old_price, italic_old=True)}")
        lines.append(f"\u0410\u041a\u0426\u0418\u042f -{percent}%")
        lines.append(f"\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: {remaining}")
    else:
        lines.append(f"{label} - {escape_html(price_text)}")
    if exclusive_stock:
        remaining, total_stock = exclusive_stock
        lines.append(f"\u0422\u0438\u0440\u0430\u0436: {remaining}/{total_stock}")
    lines.append(f"{index + 1}/{total}")
    return "\n".join(lines)


def build_discount_caption(
    card: Card,
    index: int,
    total: int,
    discount: Dict[str, object],
) -> str:
    percent = int(discount.get("percent", 0))
    discounted = int(discount.get("discount_price", card.price or 0))
    new_price = format_short_amount(discounted, card_currency(card))
    old_price = format_short_amount(card.price, card_currency(card))
    remaining = int(discount.get("remaining", 0))
    status = (
        f"\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c: {remaining}"
        if remaining > 0
        else "\u0420\u0430\u0441\u043a\u0443\u043f\u0438\u043b\u0438"
    )
    label = escape_html(format_card_label(card))
    return "\n".join(
        [
            f"{label} - {format_price_with_old_html(new_price, old_price, italic_old=True)}",
            f"\u0410\u041a\u0426\u0418\u042f -{percent}%",
            status,
            f"{index + 1}/{total}",
        ]
    )


def filter_inventory_by_rarity(
    user: Dict[str, object],
    card_map: Dict[str, Card],
    rarity: str,
) -> List[Dict[str, object]]:
    items = []
    for item in user.get("inventory", []):
        filename = item.get("file")
        if not filename:
            continue
        card = card_map.get(filename)
        if card and card.rarity == rarity:
            items.append(item)
    return items


async def show_inventory_card(
    message,
    user: Dict[str, object],
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    pressed_by=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    items = filter_inventory_by_rarity(user, card_map, rarity)
    if not items:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0423 \u0442\u0435\u0431\u044f \u043d\u0435\u0442 \u0441\u043e\u0441\u0438\u0441\u043e\u043a \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438.",
                pressed_by,
            ),
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="my_menu")]]
            ),
        )
        return
    index = max(0, min(index, len(items) - 1))
    item = items[index]
    card = card_map.get(item["file"])
    if not card:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435.",
                pressed_by,
            ),
            None,
        )
        return
    caption = apply_pressed_by(
        build_inventory_caption(card, index, len(items)), pressed_by
    )
    keyboard = build_inventory_keyboard(rarity, index, len(items), item["id"])
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=True,
            context=context,
            owner_id=pressed_by.id if pressed_by else None,
        )


async def edit_inventory_card(
    message,
    user: Dict[str, object],
    card_map: Dict[str, Card],
    rarity: str,
    index: int,
    pressed_by=None,
) -> None:
    items = filter_inventory_by_rarity(user, card_map, rarity)
    if not items:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0423 \u0442\u0435\u0431\u044f \u043d\u0435\u0442 \u0441\u043e\u0441\u0438\u0441\u043e\u043a \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438.",
                pressed_by,
            ),
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="my_menu")]]
            ),
        )
        return
    index = max(0, min(index, len(items) - 1))
    item = items[index]
    card = card_map.get(item["file"])
    if not card:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430 \u0432 \u0431\u0430\u0437\u0435.",
                pressed_by,
            ),
            None,
        )
        return
    caption = apply_pressed_by(
        build_inventory_caption(card, index, len(items)), pressed_by
    )
    keyboard = build_inventory_keyboard(rarity, index, len(items), item["id"])
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=True,
        )


async def show_shop_card(
    message,
    rarity: str,
    index: int,
    cards: List[Card],
    pressed_by=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    if not cards:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0412 \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438 \u043f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a.",
                pressed_by,
            ),
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="shop_menu")]]
            ),
        )
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    discount = None
    exclusive_stock = None
    if context is not None:
        discounts = await ensure_discounts(context)
        discount = get_discount_item(discounts, card.file)
        if card.rarity == "exclusive":
            db = context.application.bot_data.get("db", {})
            exclusive_stock = get_exclusive_stock(db, card.file)
    caption = apply_pressed_by(
        build_shop_caption(
            card, index, len(cards), discount=discount, exclusive_stock=exclusive_stock
        ),
        pressed_by,
    )
    allow_buy = True
    if card.rarity == "exclusive" and exclusive_stock:
        remaining, _ = exclusive_stock
        allow_buy = remaining > 0
    keyboard = build_shop_keyboard(rarity, index, len(cards), allow_buy=allow_buy)
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=True,
            context=context,
            owner_id=pressed_by.id if pressed_by else None,
            parse_mode=ParseMode.HTML,
        )


async def edit_shop_card(
    message,
    rarity: str,
    index: int,
    cards: List[Card],
    pressed_by=None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> None:
    if not cards:
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0412 \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438 \u043f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a.",
                pressed_by,
            ),
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="shop_menu")]]
            ),
        )
        return
    index = max(0, min(index, len(cards) - 1))
    card = cards[index]
    discount = None
    exclusive_stock = None
    if context is not None:
        discounts = await ensure_discounts(context)
        discount = get_discount_item(discounts, card.file)
        if card.rarity == "exclusive":
            db = context.application.bot_data.get("db", {})
            exclusive_stock = get_exclusive_stock(db, card.file)
    caption = apply_pressed_by(
        build_shop_caption(
            card, index, len(cards), discount=discount, exclusive_stock=exclusive_stock
        ),
        pressed_by,
    )
    allow_buy = True
    if card.rarity == "exclusive" and exclusive_stock:
        remaining, _ = exclusive_stock
        allow_buy = remaining > 0
    keyboard = build_shop_keyboard(rarity, index, len(cards), allow_buy=allow_buy)
    path = get_card_media_path(card)
    if not path.exists():
        await edit_message_text(
            message,
            apply_pressed_by(
                "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                pressed_by,
            ),
            None,
        )
        return
    with path.open("rb") as photo:
        await send_or_edit_photo(
            message,
            photo,
            caption,
            keyboard,
            prefer_edit=True,
            context=context,
            owner_id=pressed_by.id if pressed_by else None,
            parse_mode=ParseMode.HTML,
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    parts = data.split("|")
    action = parts[0]
    tg_user = update.effective_user
    pressed_by = tg_user
    if query.message:
        owner_id = get_message_owner(context.application.bot_data, query.message)
        if owner_id is None:
            set_message_owner(
                context.application.bot_data, query.message, tg_user.id
            )
            owner_id = tg_user.id
        if owner_id is not None and owner_id != tg_user.id:
            await safe_answer_callback(
                query,
                "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u044f \u043a\u043d\u043e\u043f\u043a\u0430.",
                show_alert=True,
            )
            return
    await safe_answer_callback(query)

    if action == "noop":
        return
    if action == "menu":
        await send_main_menu(update, context, pressed_by=pressed_by)
        return
    if action == "roll_menu":
        await roll_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "sausages_menu":
        await sausages_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "donate_menu":
        await donate_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "donate_stars":
        await donate_stars_command(update, context, pressed_by=pressed_by)
        return
    if action == "donate_stars_topup":
        db = context.application.bot_data["db"]
        user = ensure_user(db, tg_user)
        user["input_mode"] = "stars_topup"
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                "\u0412\u0432\u0435\u0434\u0438 \u043a\u043e\u043b-\u0432\u043e \u0437\u0432\u0451\u0437\u0434 (\u043c\u0438\u043d\u0438\u043c\u0443\u043c 25). \u0414\u043b\u044f \u043e\u0442\u043c\u0435\u043d\u044b \u043d\u0430\u043f\u0438\u0448\u0438 \u00ab\u043e\u0442\u043c\u0435\u043d\u0430\u00bb.",
                pressed_by,
            )
        )
        return
    if action == "donate_vip":
        await vip_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "cmd" and len(parts) > 1:
        cmd = parts[1]
        if cmd == "sosiska":
            await sosiska_command(update, context, pressed_by=pressed_by)
        elif cmd == "my":
            await my_command(update, context, pressed_by=pressed_by)
        elif cmd == "shop":
            await shop_command(update, context, pressed_by=pressed_by)
        elif cmd == "kazik":
            await kazik_command(update, context, pressed_by=pressed_by)
        elif cmd == "stars":
            await stars_menu_command(update, context, pressed_by=pressed_by)
        elif cmd == "vip":
            await vip_menu_command(update, context, pressed_by=pressed_by)
        elif cmd == "skidki":
            await skidki_command(update, context, pressed_by=pressed_by)
        elif cmd == "rozigrish":
            await rozigrish_command(update, context, pressed_by=pressed_by)
        elif cmd == "trade":
            await trade_command(update, context, pressed_by=pressed_by)
        elif cmd == "top":
            await top_command(update, context, pressed_by=pressed_by)
        return
    if action == "my_menu":
        await my_command(update, context, pressed_by=pressed_by)
        return
    if action == "shop_menu":
        await shop_command(update, context, pressed_by=pressed_by)
        return
    if action == "skidki_menu":
        await skidki_command(update, context, pressed_by=pressed_by)
        return

    db = context.application.bot_data["db"]
    card_map = context.application.bot_data["card_map"]
    by_rarity = context.application.bot_data["cards_by_rarity"]
    drop_chances = context.application.bot_data["drop_chances"]
    user = ensure_user(db, tg_user)

    if action == "stars_menu":
        await stars_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "stars_buy" and len(parts) > 1:
        try:
            amount = int(parts[1])
        except ValueError:
            return
        if amount not in STARS_TOPUP_AMOUNTS:
            return
        await send_stars_invoice(query.message, amount)
        return
    if action == "vip_buy_balance":
        await vip_purchase_with_balance(update, context, pressed_by=pressed_by)
        return
    if action == "vip_buy_stars":
        await vip_purchase_with_stars(update, context, pressed_by=pressed_by)
        return
    if action == "vip_buy":
        await vip_purchase_with_stars(update, context, pressed_by=pressed_by)
        return
    if action == "vip_reward_menu":
        await vip_reward_menu_command(update, context, pressed_by=pressed_by)
        return
    if action == "vip_reward_nav" and len(parts) > 1:
        try:
            index = int(parts[1])
        except ValueError:
            return
        cards = filter_existing_cards(by_rarity).get("exclusive", [])
        if not cards:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u043d\u044b\u0445 \u0441\u043e\u0441\u0438\u0441\u043e\u043a.",
                    pressed_by,
                )
            )
            return
        await edit_vip_reward_card(
            query.message, cards, index, pressed_by=pressed_by, context=context
        )
        return
    if action == "vip_reward_pick" and len(parts) > 1:
        if user.get("vip_reward_pending"):
            user["vip_reward_pending"] = False
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                "VIP \u043d\u0430\u0433\u0440\u0430\u0434\u044b \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u044b. \u042d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u044b \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0435.",
                pressed_by,
            )
        )
        return
        try:
            index = int(parts[1])
        except ValueError:
            return
        if not user.get("vip_reward_pending"):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0430\u0433\u0440\u0430\u0434\u0430 VIP \u0443\u0436\u0435 \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        cards = filter_existing_cards(by_rarity).get("exclusive", [])
        if not cards:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432\u043d\u044b\u0445 \u0441\u043e\u0441\u0438\u0441\u043e\u043a.",
                    pressed_by,
                )
            )
            return
        index = max(0, min(index, len(cards) - 1))
        card = cards[index]
        user["inventory"].append(make_inventory_item(card.file))
        user["vip_reward_pending"] = False
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                f"VIP \u043d\u0430\u0433\u0440\u0430\u0434\u0430 \u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0430: {card_display_name(card)}.",
                pressed_by,
            )
        )
        return
    if action == "trade_accept_btn" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade or trade.get("status") != "open":
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0438\u043b\u0438 \u0437\u0430\u043a\u0440\u044b\u0442.",
                    pressed_by,
                )
            )
            return
        if trade.get("to_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e\u0442 \u0442\u0440\u0435\u0439\u0434 \u043f\u0440\u0435\u0434\u043d\u0430\u0437\u043d\u0430\u0447\u0435\u043d \u0434\u0440\u0443\u0433\u043e\u043c\u0443 \u0438\u0433\u0440\u043e\u043a\u0443.",
                    pressed_by,
                )
            )
            return
        trade["status"] = "accepting"
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        from_id = trade.get("from_id")
        from_user = db.get("users", {}).get(from_id, {})
        offered_item = find_inventory_item(from_user, trade.get("from_item_id", ""))
        if not offered_item:
            db.get("trades", {}).pop(token, None)
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d: \u0441\u043e\u0441\u0438\u0441\u043a\u0438 \u043d\u0435\u0442 \u0443 \u0430\u0432\u0442\u043e\u0440\u0430.",
                    pressed_by,
                )
            )
            return
        offered_card = card_map.get(offered_item.get("file"))
        offer_text = f"\u0422\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0448\u044c: {card_display_name(offered_card)}"
        caption = apply_pressed_by(
            "\n".join(
                [
                    f"\u0422\u0440\u0435\u0439\u0434 \u043e\u0442: {trade.get('from_tag') or trade.get('from_name') or from_id}",
                    offer_text,
                    "\u0412\u044b\u0431\u0435\u0440\u0438 \u0441\u0432\u043e\u044e \u0441\u043e\u0441\u0438\u0441\u043a\u0443 \u0434\u043b\u044f \u043e\u0431\u043c\u0435\u043d\u0430 \u0438\u043b\u0438 \u043d\u0430\u0436\u043c\u0438 \u00ab\u041d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u0432\u044b\u0431\u0438\u0440\u0430\u0442\u044c\u00bb.",
                ]
            ),
            pressed_by,
        )
        menu_path = get_cached_menu_image(
            "trade_accept",
            "\u0422\u0440\u0435\u0439\u0434",
            "\u0412\u044b\u0431\u0435\u0440\u0438 \u0441\u043e\u0441\u0438\u0441\u043a\u0443",
        )
        with menu_path.open("rb") as photo:
            await send_or_edit_photo(
                query.message,
                photo,
                caption,
                build_trade_rarity_keyboard(token, "accept"),
                prefer_edit=False,
                context=context,
                owner_id=tg_user.id,
            )
        return

    if action == "trade_accept_none" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade or trade.get("status") != "accepting":
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0438\u043b\u0438 \u0437\u0430\u043a\u0440\u044b\u0442.",
                    pressed_by,
                )
            )
            return
        if trade.get("to_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e\u0442 \u0442\u0440\u0435\u0439\u0434 \u043f\u0440\u0435\u0434\u043d\u0430\u0437\u043d\u0430\u0447\u0435\u043d \u0434\u0440\u0443\u0433\u043e\u043c\u0443 \u0438\u0433\u0440\u043e\u043a\u0443.",
                    pressed_by,
                )
            )
            return
        from_id = trade.get("from_id")
        from_user = db.get("users", {}).get(from_id)
        to_user = db.get("users", {}).get(str(tg_user.id))
        if not from_user or not to_user:
            return
        offered_item = find_inventory_item(from_user, trade.get("from_item_id", ""))
        if not offered_item:
            db.get("trades", {}).pop(token, None)
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d: \u0441\u043e\u0441\u0438\u0441\u043a\u0438 \u043d\u0435\u0442 \u0443 \u0430\u0432\u0442\u043e\u0440\u0430.",
                    pressed_by,
                )
            )
            return
        trade["to_item_id"] = None
        trade["status"] = "confirming"
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        offered_card = card_map.get(offered_item.get("file"))
        from_label = trade_user_label(trade, "from")
        to_label = trade_user_label(trade, "to")
        offer_text = (
            card_display_name(offered_card) if offered_card else "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
        )
        summary = "\n".join(
            [
                f"{from_label} \u043e\u0442\u0434\u0430\u0451\u0442: {offer_text}",
                f"{to_label} \u043e\u0442\u0434\u0430\u0451\u0442: \u043d\u0438\u0447\u0435\u0433\u043e",
                "\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438 \u0442\u0440\u0435\u0439\u0434.",
            ]
        )
        try:
            sent = await context.bot.send_message(
                chat_id=int(from_id),
                text=summary,
                reply_markup=build_trade_confirm_keyboard(token),
            )
            set_message_owner(
                context.application.bot_data, sent, int(trade["from_id"])
            )
        except Exception:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0435.",
                    pressed_by,
                )
            )
        await query.message.reply_text(
            apply_pressed_by(
                "\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d. \u0416\u0434\u0438 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f.",
                pressed_by,
            )
        )
        return

    if action == "trade_confirm" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade or trade.get("status") != "confirming":
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0438\u043b\u0438 \u0437\u0430\u043a\u0440\u044b\u0442.",
                    pressed_by,
                )
            )
            return
        if trade.get("from_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        from_id = trade.get("from_id")
        to_id = trade.get("to_id")
        from_user = db.get("users", {}).get(from_id)
        to_user = db.get("users", {}).get(to_id)
        if not from_user or not to_user:
            return
        offered_item = find_inventory_item(from_user, trade.get("from_item_id", ""))
        if not offered_item:
            db.get("trades", {}).pop(token, None)
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d: \u0441\u043e\u0441\u0438\u0441\u043a\u0438 \u043d\u0435\u0442 \u0443 \u0430\u0432\u0442\u043e\u0440\u0430.",
                    pressed_by,
                )
            )
            return
        give_item = None
        to_item_id = trade.get("to_item_id")
        if to_item_id:
            give_item = find_inventory_item(to_user, to_item_id)
            if not give_item:
                db.get("trades", {}).pop(token, None)
                lock = context.application.bot_data["db_lock"]
                async with lock:
                    save_db(db)
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d: \u0441\u043e\u0441\u0438\u0441\u043a\u0438 \u043d\u0435\u0442 \u0443 \u0432\u0442\u043e\u0440\u043e\u0439 \u0441\u0442\u043e\u0440\u043e\u043d\u044b.",
                        pressed_by,
                    )
                )
                return
        from_user["inventory"] = [
            item
            for item in from_user.get("inventory", [])
            if item.get("id") != offered_item.get("id")
        ]
        to_user.setdefault("inventory", []).append(offered_item)
        if give_item:
            to_user["inventory"] = [
                item
                for item in to_user.get("inventory", [])
                if item.get("id") != give_item.get("id")
            ]
            from_user.setdefault("inventory", []).append(give_item)
        db.get("trades", {}).pop(token, None)
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        offered_card = card_map.get(offered_item.get("file"))
        give_card = card_map.get(give_item.get("file")) if give_item else None
        receive_text = (
            card_display_name(give_card) if give_card else "\u043d\u0438\u0447\u0435\u0433\u043e"
        )
        await query.message.reply_text(
            apply_pressed_by(
                f"\u0422\u0440\u0435\u0439\u0434 \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d. \u0422\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u043b {receive_text}.",
                pressed_by,
            )
        )
        try:
            offer_text = (
                card_display_name(offered_card) if offered_card else "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
            )
            await context.bot.send_message(
                chat_id=int(to_id),
                text=f"\u0422\u0440\u0435\u0439\u0434 \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d. \u0422\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u043b {offer_text}.",
            )
        except Exception:
            pass
        return

    if action == "trade_confirm_cancel" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade:
            return
        if trade.get("from_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        to_id = trade.get("to_id")
        db.get("trades", {}).pop(token, None)
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d.",
                pressed_by,
            )
        )
        try:
            if to_id:
                await context.bot.send_message(
                    chat_id=int(to_id),
                    text="\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d.",
                )
        except Exception:
            pass
        return

    if action == "trade_rarity_menu" and len(parts) > 2:
        role = parts[1]
        token = parts[2]
        trade = db.get("trades", {}).get(token)
        if not trade:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.",
                    pressed_by,
                )
            )
            return
        if role == "offer" and trade.get("from_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        if role == "accept" and trade.get("to_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        expected_status = "draft" if role == "offer" else "accepting"
        if trade.get("status") != expected_status:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u0433\u043e\u0442\u043e\u0432 \u043a \u044d\u0442\u043e\u043c\u0443 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044e.",
                    pressed_by,
                )
            )
            return
        await query.message.edit_reply_markup(
            reply_markup=build_trade_rarity_keyboard(token, role)
        )
        return
    if action == "trade_rarity" and len(parts) > 3:
        role = parts[1]
        token = parts[2]
        rarity = parts[3]
        trade = db.get("trades", {}).get(token)
        if not trade:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.",
                    pressed_by,
                )
            )
            return
        if role == "offer" and trade.get("from_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        if role == "accept" and trade.get("to_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        expected_status = "draft" if role == "offer" else "accepting"
        if trade.get("status") != expected_status:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u0433\u043e\u0442\u043e\u0432 \u043a \u044d\u0442\u043e\u043c\u0443 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044e.",
                    pressed_by,
                )
            )
            return
        await show_trade_card(
            query.message,
            user,
            card_map,
            token,
            role,
            rarity,
            0,
            pressed_by=pressed_by,
            prefer_edit=True,
            context=context,
        )
        return
    if action == "trade_nav" and len(parts) > 4:
        role = parts[1]
        token = parts[2]
        rarity = parts[3]
        try:
            index = int(parts[4])
        except ValueError:
            return
        trade = db.get("trades", {}).get(token)
        if not trade:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.",
                    pressed_by,
                )
            )
            return
        if role == "offer" and trade.get("from_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        if role == "accept" and trade.get("to_id") != str(tg_user.id):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                    pressed_by,
                )
            )
            return
        expected_status = "draft" if role == "offer" else "accepting"
        if trade.get("status") != expected_status:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u0433\u043e\u0442\u043e\u0432 \u043a \u044d\u0442\u043e\u043c\u0443 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044e.",
                    pressed_by,
                )
            )
            return
        await edit_trade_card(
            query.message,
            user,
            card_map,
            token,
            role,
            rarity,
            index,
            pressed_by=pressed_by,
            context=context,
        )
        return
    if action == "trade_pick" and len(parts) > 5:
        role = parts[1]
        token = parts[2]
        item_id = parts[3]
        rarity = parts[4]
        try:
            index = int(parts[5])
        except ValueError:
            return
        trade = db.get("trades", {}).get(token)
        if not trade:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.",
                    pressed_by,
                )
            )
            return
        if role == "offer":
            if trade.get("from_id") != str(tg_user.id):
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                        pressed_by,
                    )
                )
                return
            if trade.get("status") != "draft":
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u0422\u0440\u0435\u0439\u0434 \u0443\u0436\u0435 \u043e\u0442\u043a\u0440\u044b\u0442.",
                        pressed_by,
                    )
                )
                return
            item = find_inventory_item(user, item_id)
            if not item:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043d\u0435 \u0432 \u0438\u043d\u0432\u0435\u043d\u0442\u0430\u0440\u0435.",
                        pressed_by,
                    )
                )
                return
            trade["from_item_id"] = item_id
            trade["status"] = "open"
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            target_label = (
                f"@{trade.get('to_tag')}"
                if trade.get("to_tag")
                else trade.get("to_name")
            )
            text = apply_pressed_by(
                f"\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d \u0434\u043b\u044f {target_label}.",
                pressed_by,
            )
            await edit_message_text(
                query.message,
                text,
                build_trade_accept_keyboard(token),
            )
            set_message_owner(
                context.application.bot_data, query.message, int(trade["to_id"])
            )
            offered_card = card_map.get(item.get("file"))
            offer_line = (
                f"\u0422\u0435\u0431\u0435 \u043f\u0440\u0435\u0434\u043b\u0430\u0433\u0430\u044e\u0442: {card_display_name(offered_card)}"
                if offered_card
                else "\u0422\u0435\u0431\u0435 \u043f\u0440\u0435\u0434\u043b\u0430\u0433\u0430\u044e\u0442 \u0442\u0440\u0435\u0439\u0434."
            )
            try:
                sent = await context.bot.send_message(
                    chat_id=int(trade["to_id"]),
                    text="\n".join(
                        [
                            f"\u0422\u0440\u0435\u0439\u0434 \u043e\u0442: {trade.get('from_tag') or trade.get('from_name')}",
                            offer_line,
                        ]
                    ),
                    reply_markup=build_trade_accept_keyboard(token),
                )
                set_message_owner(
                    context.application.bot_data, sent, int(trade["to_id"])
                )
            except (Forbidden, BadRequest) as exc:
                message_text = str(exc).lower()
                if isinstance(exc, Forbidden) or "chat not found" in message_text:
                    username = os.getenv("PUBLIC_BOT_USERNAME", "sosiskikazikbot").lstrip("@")
                    link = f"https://t.me/{username}?start=trade"
                    await query.message.reply_text(
                        apply_pressed_by(
                            "\u041d\u0435 \u043c\u043e\u0433\u0443 \u043d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044e \u0432 \u041b\u0421.\n"
                            "\u041f\u0443\u0441\u0442\u044c \u043e\u043d \u0437\u0430\u0439\u0434\u0451\u0442 \u0432 \u043b\u0438\u0447\u043a\u0443 \u0441 \u0431\u043e\u0442\u043e\u043c \u0438 \u043d\u0430\u0436\u043c\u0451\u0442 /start:\n"
                            f"{link}",
                            pressed_by,
                        )
                    )
                else:
                    await query.message.reply_text(
                        apply_pressed_by(
                            "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0442\u0440\u0435\u0439\u0434 \u0432 \u043b\u0438\u0447\u043a\u0443.",
                            pressed_by,
                        )
                    )
            except Exception:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0442\u0440\u0435\u0439\u0434 \u0432 \u043b\u0438\u0447\u043a\u0443.",
                        pressed_by,
                    )
                )
            return
        if role == "accept":
            if trade.get("to_id") != str(tg_user.id):
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u042d\u0442\u043e \u043d\u0435 \u0442\u0432\u043e\u0439 \u0442\u0440\u0435\u0439\u0434.",
                        pressed_by,
                    )
                )
                return
            if trade.get("status") != "accepting":
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u0422\u0440\u0435\u0439\u0434 \u043d\u0435 \u0433\u043e\u0442\u043e\u0432 \u043a \u043e\u0431\u043c\u0435\u043d\u0443.",
                        pressed_by,
                    )
                )
                return
            from_id = trade.get("from_id")
            from_user = db.get("users", {}).get(from_id)
            to_user = db.get("users", {}).get(str(tg_user.id))
            if not from_user or not to_user:
                return
            offered_item = find_inventory_item(from_user, trade.get("from_item_id", ""))
            if not offered_item:
                db.get("trades", {}).pop(token, None)
                lock = context.application.bot_data["db_lock"]
                async with lock:
                    save_db(db)
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d: \u0441\u043e\u0441\u0438\u0441\u043a\u0438 \u043d\u0435\u0442 \u0443 \u0430\u0432\u0442\u043e\u0440\u0430.",
                        pressed_by,
                    )
                )
                return
            offered_card = card_map.get(offered_item.get("file"))
            give_item = find_inventory_item(to_user, item_id)
            if not give_item:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043d\u0435 \u0432 \u0438\u043d\u0432\u0435\u043d\u0442\u0430\u0440\u0435.",
                        pressed_by,
                    )
                )
                return
            trade["to_item_id"] = give_item.get("id")
            trade["status"] = "confirming"
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            give_card = card_map.get(give_item.get("file"))
            from_label = trade_user_label(trade, "from")
            to_label = trade_user_label(trade, "to")
            offer_text = (
                card_display_name(offered_card) if offered_card else "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
            )
            give_text = (
                card_display_name(give_card) if give_card else "\u0441\u043e\u0441\u0438\u0441\u043a\u0443"
            )
            summary = "\n".join(
                [
                    f"{from_label} \u043e\u0442\u0434\u0430\u0451\u0442: {offer_text}",
                    f"{to_label} \u043e\u0442\u0434\u0430\u0451\u0442: {give_text}",
                    "\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438 \u0442\u0440\u0435\u0439\u0434.",
                ]
            )
            try:
                sent = await context.bot.send_message(
                    chat_id=int(from_id),
                    text=summary,
                    reply_markup=build_trade_confirm_keyboard(token),
                )
                set_message_owner(
                    context.application.bot_data, sent, int(trade["from_id"])
                )
            except Exception:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0435.",
                        pressed_by,
                    )
                )
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0417\u0430\u043f\u0440\u043e\u0441 \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d. \u0416\u0434\u0438 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f.",
                    pressed_by,
                )
            )
            return
    if action == "trade_cancel" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade or trade.get("from_id") != str(tg_user.id):
            return
        db.get("trades", {}).pop(token, None)
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d.",
                pressed_by,
            )
        )
        return
    if action == "trade_decline" and len(parts) > 1:
        token = parts[1]
        trade = db.get("trades", {}).get(token)
        if not trade:
            return
        if trade.get("to_id") != str(tg_user.id):
            return
        from_id = trade.get("from_id")
        db.get("trades", {}).pop(token, None)
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.reply_text(
            apply_pressed_by(
                "\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043c\u0435\u043d\u0451\u043d.",
                pressed_by,
            )
        )
        try:
            if from_id:
                await context.bot.send_message(
                    chat_id=int(from_id),
                    text="\u0422\u0440\u0435\u0439\u0434 \u043e\u0442\u043a\u043b\u043e\u043d\u0451\u043d.",
                )
        except Exception:
            pass
        return
    if action == "gift_pick" and len(parts) > 2:
        token = parts[1]
        try:
            pick = int(parts[2])
        except ValueError:
            return
        if pick < 1 or pick > GIFT_BUTTONS:
            return
        session = user.get("kazik_session") or {}
        if (
            not session
            or session.get("token") != token
            or session.get("used")
            or not session.get("win_index")
        ):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0421\u0435\u0441\u0441\u0438\u044f \u043f\u043e\u0434\u0430\u0440\u043a\u0430 \u0443\u0441\u0442\u0430\u0440\u0435\u043b\u0430.",
                    pressed_by,
                )
            )
            return
        session["used"] = True
        user["kazik_session"] = None
        user["last_kazik_at"] = now_utc().isoformat()

        if pick == int(session.get("win_index")):
            available_by_rarity = filter_existing_cards(by_rarity)
            won_cards = []
            for _ in range(GIFT_REWARD_COUNT):
                card = pick_random_card(available_by_rarity, drop_chances)
                if not card:
                    break
                user["inventory"].append(make_inventory_item(card.file))
                won_cards.append(card)
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.edit_reply_markup(reply_markup=None)
            if won_cards:
                lines = [
                    "\u041f\u043e\u0434\u0430\u0440\u043e\u043a \u043f\u043e\u043b\u0443\u0447\u0435\u043d!",
                    "\u041f\u043e\u043b\u0443\u0447\u0435\u043d\u043e \u0441\u043e\u0441\u0438\u0441\u043e\u043a: "
                    f"{len(won_cards)}",
                ]
                for index, card in enumerate(won_cards, start=1):
                    lines.append(
                        f"{index}. {card_display_name(card)} ({RARITY_NAMES[card.rarity]})"
                    )
                await query.message.reply_text(
                    apply_pressed_by("\n".join(lines), pressed_by)
                )
            else:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u041f\u043e\u0434\u0430\u0440\u043e\u043a \u0435\u0441\u0442\u044c, \u043d\u043e \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a \u043f\u043e\u043a\u0430 \u043d\u0435\u0442.",
                        pressed_by,
                    )
                )
        else:
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.edit_reply_markup(reply_markup=None)
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435 \u043f\u043e\u0432\u0435\u0437\u043b\u043e. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0441\u043d\u043e\u0432\u0430 \u0447\u0435\u0440\u0435\u0437 5 \u0447\u0430\u0441\u043e\u0432.",
                    pressed_by,
                )
            )
        return

    if action == "kazik_spin":
        now = now_local()
        daily_key = kazik_daily_key(now)
        if str(user.get("kazik_daily_date") or "") != daily_key:
            user["kazik_daily_date"] = daily_key
            user["kazik_daily_used"] = 0

        bonus_spins = int(user.get("kazik_bonus_spins", 0) or 0)
        daily_used = int(user.get("kazik_daily_used", 0) or 0)
        daily_limit = kazik_free_spins_limit(user)

        spent_free = False
        if bonus_spins > 0:
            user["kazik_bonus_spins"] = bonus_spins - 1
            spent_free = True
        elif daily_used < daily_limit:
            user["kazik_daily_used"] = daily_used + 1
            spent_free = True
        if not spent_free:
            stars = get_star_balance(user)
            if stars < KAZIK_STAR_SPIN_COST:
                reset_in = max(
                    0,
                    int(
                        (
                            (now + timedelta(days=1)).replace(
                                hour=0, minute=0, second=0, microsecond=0
                            )
                            - now
                        ).total_seconds()
                    ),
                )
                await edit_message_text(
                    query.message,
                    apply_pressed_by(
                        "\n".join(
                            [
                                "\u0424\u0440\u0438 \u0441\u043f\u0438\u043d\u044b \u0437\u0430\u043a\u043e\u043d\u0447\u0438\u043b\u0438\u0441\u044c.",
                                f"\u041d\u0443\u0436\u043d\u043e {KAZIK_STAR_SPIN_COST}\u2b50 \u0434\u043b\u044f \u043a\u0440\u0443\u0442\u043a\u0438.",
                                f"\u0421\u0431\u0440\u043e\u0441 \u0447\u0435\u0440\u0435\u0437: {format_duration(reset_in)}",
                            ]
                        ),
                        pressed_by,
                    ),
                    build_kazik_spin_keyboard(kazik_spin_button_label(user)),
                )
                return
            user["stars"] = stars - KAZIK_STAR_SPIN_COST
        digits = roll_kazik_digits(win_chance=get_kazik_win_chance(user))
        win_digit = digits[0] if digits[0] == digits[1] == digits[2] else None
        reward_card = None
        if win_digit is not None:
            reward_card = pick_kazik_reward_card(
                by_rarity,
                win_digit,
                allow_exclusive=False,
            )
            if reward_card:
                user["inventory"].append(make_inventory_item(reward_card.file))
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)

        try:
            spin_image = build_kazik_spin_image(digits, 0, title="\u041a\u0440\u0443\u0442\u0438\u043c...")
            await send_or_edit_photo(
                query.message,
                spin_image,
                apply_pressed_by("", pressed_by),
                None,
                prefer_edit=True,
            )
            await asyncio.sleep(KAZIK_SPIN_DELAY)
        except Exception:
            pass

        win_text = ""
        if win_digit is not None:
            if reward_card:
                win_text = (
                    "\u0412\u044b\u0438\u0433\u0440\u044b\u0448: "
                    f"{card_display_name(reward_card)} ({RARITY_NAMES[reward_card.rarity]})"
                )
            else:
                win_text = (
                    "\u0412\u044b\u0438\u0433\u0440\u044b\u0448 \u0435\u0441\u0442\u044c, "
                    "\u043d\u043e \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a \u043d\u0435\u0442."
                )
        final_caption = apply_pressed_by(win_text, pressed_by)
        result_image = build_kazik_spin_image(digits, 3)
        spin_keyboard = build_kazik_spin_keyboard(kazik_spin_button_label(user))
        try:
            await send_or_edit_photo(
                query.message,
                result_image,
                final_caption,
                spin_keyboard,
                prefer_edit=True,
            )
        except Exception:
            await edit_message_text(
                query.message,
                final_caption,
                spin_keyboard,
            )
        return

    if action == "my_rarity" and len(parts) > 1:
        rarity = parts[1]
        await show_inventory_card(
            query.message,
            user,
            card_map,
            rarity,
            0,
            pressed_by=pressed_by,
            context=context,
        )
        return
    if action == "my_nav" and len(parts) > 2:
        rarity = parts[1]
        index = int(parts[2])
        await edit_inventory_card(
            query.message, user, card_map, rarity, index, pressed_by=pressed_by
        )
        return
    if action == "my_sell" and len(parts) > 3:
        item_id = parts[1]
        rarity = parts[2]
        index = int(parts[3])
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card or card.price is None:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                    pressed_by,
                )
            )
            return
        sale_price = calc_sale_price(card) or 0
        currency = card_currency(card)
        sale_label = format_short_amount(sale_price, currency)
        original_label = format_short_amount(card.price, currency)
        confirm_caption = apply_pressed_by(
            "\n".join(
                [
                    "\u041f\u0440\u043e\u0434\u0430\u0442\u044c \u044d\u0442\u0443 \u0441\u043e\u0441\u0438\u0441\u043a\u0443?",
                    f"{escape_html(format_card_label(card))} - {sale_label} <s>{escape_html(original_label)}</s>",
                ]
            ),
            pressed_by,
        )
        await query.message.edit_caption(
            caption=confirm_caption,
            reply_markup=build_my_sell_confirm_keyboard(item_id, rarity, index),
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "my_sell_cancel" and len(parts) > 3:
        rarity = parts[2]
        index = int(parts[3])
        await edit_inventory_card(
            query.message,
            user,
            card_map,
            rarity,
            index,
            pressed_by=pressed_by,
        )
        return

    if action == "my_sell_confirm" and len(parts) > 3:
        item_id = parts[1]
        rarity = parts[2]
        index = int(parts[3])
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card or card.price is None:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                    pressed_by,
                )
            )
            return
        sale_price = calc_sale_price(card) or 0
        if card_currency(card) == "stars":
            user["stars"] = get_star_balance(user) + sale_price
        else:
            user["balance"] = int(user.get("balance", 0)) + sale_price
        user["inventory"] = [i for i in items if i.get("id") != item_id]
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        items_left = filter_inventory_by_rarity(user, card_map, rarity)
        if not items_left:
            await query.message.edit_caption(
                apply_pressed_by(
                    "\u0421\u043e\u0441\u0438\u0441\u043e\u043a \u044d\u0442\u043e\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438 \u0431\u043e\u043b\u044c\u0448\u0435 \u043d\u0435\u0442.",
                    pressed_by,
                ),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="my_menu")]]
                ),
            )
        else:
            new_index = min(index, len(items_left) - 1)
            await edit_inventory_card(
                query.message,
                user,
                card_map,
                rarity,
                new_index,
                pressed_by=pressed_by,
            )
        await query.message.reply_text(
            apply_pressed_by(
                f"\u041f\u0440\u043e\u0434\u0430\u043d\u043e \u0437\u0430 {format_short_amount(sale_price, card_currency(card))}.",
                pressed_by,
            )
        )
        return

    if action == "my_upgrade" and len(parts) > 3:
        item_id = parts[1]
        rarity = parts[2]
        index = int(parts[3])
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        user_label = get_user_label(tg_user)
        caption = apply_pressed_by(
            build_upgrade_warning_caption(user_label, card),
            pressed_by,
        )
        await query.message.edit_caption(
            caption=caption,
            reply_markup=build_my_upgrade_confirm_keyboard(item_id, rarity, index),
        )
        return

    if action == "my_upgrade_cancel" and len(parts) > 3:
        item_id = parts[1]
        rarity = parts[2]
        index = int(parts[3])
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        items_in_rarity = filter_inventory_by_rarity(user, card_map, rarity)
        caption = apply_pressed_by(
            build_inventory_caption(card, index, len(items_in_rarity)),
            pressed_by,
        )
        await query.message.edit_caption(
            caption=caption,
            reply_markup=build_inventory_keyboard(
                rarity, index, len(items_in_rarity), item_id
            ),
        )
        return

    if action == "my_upgrade_confirm" and len(parts) > 3:
        item_id = parts[1]
        rarity = parts[2]
        index = int(parts[3])
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        next_rarity = get_next_rarity(card.rarity)
        if not next_rarity:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043c\u0430\u043a\u0441\u0438\u043c\u0430\u043b\u044c\u043d\u0430\u044f \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c.",
                    pressed_by,
                )
            )
            return
        roll = random.randint(0, 1)
        user_label = get_user_label(tg_user)
        if roll == 0:
            user["inventory"] = [i for i in items if i.get("id") != item_id]
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.edit_caption(
                caption=apply_pressed_by(
                    build_upgrade_fail_caption(user_label),
                    pressed_by,
                ),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("\u041d\u0430\u0437\u0430\u0434", callback_data="my_menu")]]
                ),
            )
            return
        available_by_rarity = filter_existing_cards(by_rarity)
        next_cards = available_by_rarity.get(next_rarity, [])
        if not next_cards:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438.",
                    pressed_by,
                )
            )
            return
        upgraded = random.choice(next_cards)
        item["file"] = upgraded.file
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        items_in_new = filter_inventory_by_rarity(user, card_map, upgraded.rarity)
        new_index = next(
            (i for i, it in enumerate(items_in_new) if it.get("id") == item_id),
            0,
        )
        await edit_inventory_card(
            query.message,
            user,
            card_map,
            upgraded.rarity,
            new_index,
            pressed_by=pressed_by,
        )
        return

    if action == "shop_rarity" and len(parts) > 1:
        rarity = parts[1]
        await show_shop_card(
            query.message,
            rarity,
            0,
            by_rarity.get(rarity, []),
            pressed_by=pressed_by,
            context=context,
        )
        return
    if action == "shop_nav" and len(parts) > 2:
        rarity = parts[1]
        index = int(parts[2])
        await edit_shop_card(
            query.message,
            rarity,
            index,
            by_rarity.get(rarity, []),
            pressed_by=pressed_by,
            context=context,
        )
        return
    if action == "shop_buy" and len(parts) > 2:
        rarity = parts[1]
        index = int(parts[2])
        cards = by_rarity.get(rarity, [])
        if not cards:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a \u0434\u043b\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0438.",
                    pressed_by,
                )
            )
            return
        index = max(0, min(index, len(cards) - 1))
        card = cards[index]
        if card.rarity == "exclusive":
            lock = context.application.bot_data["db_lock"]
            async with lock:
                remaining, _ = get_exclusive_stock(db, card.file)
                if remaining <= 0:
                    await query.message.reply_text(
                        apply_pressed_by(
                            "\u042d\u0442\u043e\u0442 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432 \u0443\u0436\u0435 \u0440\u0430\u0441\u043a\u0443\u043f\u0438\u043b\u0438.",
                            pressed_by,
                        )
                    )
                    return
            if card.price is None:
                await query.message.reply_text(
                    apply_pressed_by(
                        "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043a\u0443\u043f\u0438\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                        pressed_by,
                    )
                )
                return
            stars = get_star_balance(user)
            if stars < int(card.price):
                await query.message.reply_text(
                    apply_pressed_by(
                        f"\u041d\u0443\u0436\u043d\u043e {format_short_amount(card.price, 'stars')} \u0434\u043b\u044f \u043f\u043e\u043a\u0443\u043f\u043a\u0438.",
                        pressed_by,
                    )
                )
                return
            async with lock:
                if not consume_exclusive_stock(db, card.file):
                    await query.message.reply_text(
                        apply_pressed_by(
                            "\u042d\u0442\u043e\u0442 \u044d\u043a\u0441\u043a\u043b\u044e\u0437\u0438\u0432 \u0443\u0436\u0435 \u0440\u0430\u0441\u043a\u0443\u043f\u0438\u043b\u0438.",
                            pressed_by,
                        )
                    )
                    return
                user["stars"] = stars - int(card.price)
                user["inventory"].append(make_inventory_item(card.file))
                save_db(db)
            await query.message.reply_text(
                apply_pressed_by(
                    f"\u041a\u0443\u043f\u043b\u0435\u043d\u043e \u0437\u0430 {format_short_amount(card.price, 'stars')}.",
                    pressed_by,
                )
            )
            return
        if card.price is None:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043a\u0443\u043f\u0438\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                    pressed_by,
                )
            )
            return
        price = int(card.price)
        used_discount = False
        discount_lock = context.application.bot_data.setdefault(
            "discount_lock", asyncio.Lock()
        )
        async with discount_lock:
            discounts = load_discount_data()
            today = discount_day_key()
            if discounts.get("date") != today:
                discounts = generate_discounts(by_rarity)
            discount = get_discount_item(discounts, card.file)
            if discount and is_discount_active(discount):
                price = int(discount.get("discount_price", price))
                remaining = int(discount.get("remaining", 0))
                discount["remaining"] = max(0, remaining - 1)
                used_discount = True
            save_discount_data(discounts)
            context.application.bot_data["discounts"] = discounts
        balance = int(user.get("balance", 0))
        if balance < price:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u0441\u0440\u0435\u0434\u0441\u0442\u0432.",
                    pressed_by,
                )
            )
            return
        user["balance"] = balance - price
        user["inventory"].append(make_inventory_item(card.file))
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        price_label = format_short_amount(price, "rub")
        if used_discount:
            price_label += " (\u0430\u043a\u0446\u0438\u044f)"
        await query.message.reply_text(
            apply_pressed_by(
                f"\u041a\u0443\u043f\u043b\u0435\u043d\u043e \u0437\u0430 {price_label}.",
                pressed_by,
            )
        )
        return

    if action == "discount_view" and len(parts) > 1:
        try:
            view_index = int(parts[1]) - 1
        except ValueError:
            return
        discounts = await ensure_discounts(context)
        items = discounts.get("items", [])
        if not isinstance(items, list) or view_index < 0 or view_index >= len(items):
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0430\u043a\u0446\u0438\u044f \u0443\u0436\u0435 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        item = items[view_index]
        filename = str(item.get("file") or "")
        card = card_map.get(filename)
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        caption = apply_pressed_by(
            build_discount_caption(card, view_index, len(items), item),
            pressed_by,
        )
        path = get_card_media_path(card)
        if not path.exists():
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u044d\u0442\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                    pressed_by,
                )
            )
            return
        with path.open("rb") as photo:
            await send_or_edit_photo(
                query.message,
                photo,
                caption,
                build_discount_view_keyboard(),
                prefer_edit=True,
                context=context,
                owner_id=tg_user.id,
                parse_mode=ParseMode.HTML,
            )
        return

    if action == "draw_sell" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card or card.price is None:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                    pressed_by,
                )
            )
            return
        sale_price = calc_sale_price(card) or 0
        currency = card_currency(card)
        sale_label = format_short_amount(sale_price, currency)
        original_label = format_short_amount(card.price, currency)
        confirm_caption = apply_pressed_by(
            "\n".join(
                [
                    "\u041f\u0440\u043e\u0434\u0430\u0442\u044c \u044d\u0442\u0443 \u0441\u043e\u0441\u0438\u0441\u043a\u0443?",
                    f"{escape_html(format_card_label(card))} - {sale_label} <s>{escape_html(original_label)}</s>",
                ]
            ),
            pressed_by,
        )
        await query.message.edit_caption(
            caption=confirm_caption,
            reply_markup=build_draw_sell_confirm_keyboard(item_id),
            parse_mode=ParseMode.HTML,
        )
        return

    if action == "draw_sell_cancel" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        user_label = get_user_label(tg_user)
        caption = apply_pressed_by(build_draw_caption(user_label, card), pressed_by)
        await query.message.edit_caption(
            caption=caption,
            reply_markup=build_draw_keyboard(item_id),
        )
        return

    if action == "draw_sell_confirm" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card or card.price is None:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0426\u0435\u043d\u0430 \u043d\u0435 \u0437\u0430\u0434\u0430\u043d\u0430, \u043f\u0440\u043e\u0434\u0430\u0442\u044c \u043d\u0435\u043b\u044c\u0437\u044f.",
                    pressed_by,
                )
            )
            return
        sale_price = calc_sale_price(card) or 0
        if card_currency(card) == "stars":
            user["stars"] = get_star_balance(user) + sale_price
        else:
            user["balance"] = int(user.get("balance", 0)) + sale_price
        user["inventory"] = [i for i in items if i.get("id") != item_id]
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        await query.message.edit_caption(
            caption=apply_pressed_by(
                f"\u041f\u0440\u043e\u0434\u0430\u043d\u043e \u0437\u0430 {format_short_amount(sale_price, card_currency(card))}.",
                pressed_by,
            ),
            reply_markup=None,
        )
        return

    if action == "draw_upgrade" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        user_label = get_user_label(tg_user)
        caption = apply_pressed_by(
            build_upgrade_warning_caption(user_label, card), pressed_by
        )
        await query.message.edit_caption(
            caption=caption,
            reply_markup=build_upgrade_confirm_keyboard(item_id),
        )
        return

    if action == "draw_upgrade_cancel" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        user_label = get_user_label(tg_user)
        caption = apply_pressed_by(build_draw_caption(user_label, card), pressed_by)
        await query.message.edit_caption(
            caption=caption,
            reply_markup=build_draw_keyboard(item_id),
        )
        return

    if action == "draw_upgrade_confirm" and len(parts) > 1:
        item_id = parts[1]
        items = user.get("inventory", [])
        item = next((i for i in items if i.get("id") == item_id), None)
        if not item:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u0430 \u0441\u043e\u0441\u0438\u0441\u043a\u0430 \u0443\u0436\u0435 \u043f\u0440\u043e\u0434\u0430\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        card = card_map.get(item.get("file"))
        if not card:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.",
                    pressed_by,
                )
            )
            return
        next_rarity = get_next_rarity(card.rarity)
        if not next_rarity:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u042d\u0442\u043e \u043c\u0430\u043a\u0441\u0438\u043c\u0430\u043b\u044c\u043d\u0430\u044f \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u044c.",
                    pressed_by,
                )
            )
            return
        roll = random.randint(0, 1)
        user_label = get_user_label(tg_user)
        if roll == 0:
            user["inventory"] = [i for i in items if i.get("id") != item_id]
            lock = context.application.bot_data["db_lock"]
            async with lock:
                save_db(db)
            await query.message.edit_caption(
                caption=apply_pressed_by(
                    build_upgrade_fail_caption(user_label),
                    pressed_by,
                ),
                reply_markup=None,
            )
            return
        available_by_rarity = filter_existing_cards(by_rarity)
        next_cards = available_by_rarity.get(next_rarity, [])
        if not next_cards:
            await query.message.reply_text(
                apply_pressed_by(
                    "\u041d\u0435\u0442 \u043a\u0430\u0440\u0442\u043e\u0447\u0435\u043a \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0440\u0435\u0434\u043a\u043e\u0441\u0442\u0438.",
                    pressed_by,
                )
            )
            return
        upgraded = random.choice(next_cards)
        item["file"] = upgraded.file
        lock = context.application.bot_data["db_lock"]
        async with lock:
            save_db(db)
        caption = apply_pressed_by(
            build_upgrade_success_caption(user_label, upgraded),
            pressed_by,
        )
        path = get_card_media_path(upgraded)
        if not path.exists():
            await query.message.reply_text(
                apply_pressed_by(
                    "\u0424\u043e\u0442\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0434\u043b\u044f \u043d\u043e\u0432\u043e\u0439 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0438.",
                    pressed_by,
                )
            )
            return
        with path.open("rb") as photo:
            await send_or_edit_photo(
                query.message,
                photo,
                caption,
                build_draw_keyboard(item_id),
                prefer_edit=True,
            )
        return


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Unhandled error in update", exc_info=context.error)


async def background_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await ensure_discounts(context)
        giveaway = await ensure_giveaway(context)
        phase = giveaway_phase()
        if phase == "open":
            await announce_giveaway_start(context, giveaway)
        if phase == "announce" and giveaway.get("status") != "announced":
            await announce_giveaway(context, giveaway)
        if phase == "idle" and giveaway.get("status") != "announced":
            await announce_giveaway(context, giveaway)
    except Exception:
        logging.exception("Background tick failed")


async def reminder_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    interval_sec_raw = os.getenv("REMINDER_INTERVAL_SEC", "").strip()
    try:
        interval_sec = int(interval_sec_raw) if interval_sec_raw else 2 * 24 * 60 * 60
    except ValueError:
        interval_sec = 2 * 24 * 60 * 60

    db = context.application.bot_data.get("db")
    if not isinstance(db, dict):
        return
    users = db.get("users", {})
    if not isinstance(users, dict) or not users:
        return

    username = get_public_bot_username(context)
    startgroup_url = (
        f"https://t.me/{username}?startgroup=true" if username else None
    )
    reply_markup = (
        InlineKeyboardMarkup(
            [[InlineKeyboardButton("\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \u0447\u0430\u0442", url=startgroup_url)]]
        )
        if startgroup_url
        else None
    )
    text = "\n".join(
        [
            "\u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0431\u043e\u0442\u0430 \u0432 \u0441\u0432\u043e\u0439 \u0447\u0430\u0442!",
            "",
            "\u041a\u043d\u043e\u043f\u043a\u0430 \u043d\u0438\u0436\u0435 \u043e\u0442\u043a\u0440\u043e\u0435\u0442 \u043c\u0435\u043d\u044e \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0447\u0430\u0442.",
        ]
    )

    now = now_utc()
    touch_ids: List[str] = []

    for uid, user in users.items():
        if not isinstance(user, dict):
            continue
        last = parse_iso(user.get("last_reminder_at"))
        if last and (now - last).total_seconds() < interval_sec:
            continue
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text=text,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            touch_ids.append(uid)
        except Forbidden:
            touch_ids.append(uid)
        except Exception:
            pass
        await asyncio.sleep(0.04)

    if not touch_ids:
        return
    lock = context.application.bot_data["db_lock"]
    async with lock:
        for uid in touch_ids:
            user = users.get(uid)
            if isinstance(user, dict):
                user["last_reminder_at"] = now.isoformat()
        save_db(db)


async def precheckout_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.pre_checkout_query
    if not query:
        return
    amount = parse_stars_payload(query.invoice_payload)
    if amount is None or amount <= 0:
        await query.answer(
            ok=False,
            error_message="\u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u043f\u043b\u0430\u0442\u0451\u0436.",
        )
        return
    await query.answer(ok=True)


async def successful_payment_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return
    payment = message.successful_payment
    if not payment:
        return
    amount = parse_stars_payload(payment.invoice_payload)
    if amount is None or amount <= 0:
        return
    db = context.application.bot_data["db"]
    user = ensure_user(db, tg_user)
    user["stars"] = get_star_balance(user) + amount
    lock = context.application.bot_data["db_lock"]
    async with lock:
        save_db(db)
    await message.reply_text(
        f"\u2705 \u0417\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e {amount} \u2b50. \u0422\u0435\u043f\u0435\u0440\u044c \u043d\u0430 \u0431\u0430\u043b\u0430\u043d\u0441\u0435 {get_star_balance(user)} \u2b50."
    )


async def setup_bot_commands(application) -> None:
    common = [
        BotCommand("start", "\u041c\u0435\u043d\u044e"),
        BotCommand("sosiska", "\u041a\u0440\u0443\u0442\u043a\u0430 (\u043e\u0431\u044b\u0447\u043d\u0430\u044f)"),
        BotCommand("kazik", "\u041a\u0430\u0437\u0438\u043a"),
        BotCommand("my", "\u041c\u043e\u0438 \u0441\u043e\u0441\u0438\u0441\u043a\u0438"),
        BotCommand("shop", "\u041a\u0443\u043f\u0438\u0442\u044c \u0441\u043e\u0441\u0438\u0441\u043a\u0438"),
        BotCommand("trade", "\u0422\u0440\u0435\u0439\u0434"),
        BotCommand("trade_accept", "\u041f\u0440\u0438\u043d\u044f\u0442\u044c \u0442\u0440\u0435\u0439\u0434"),
        BotCommand("vip", "VIP"),
        BotCommand("top", "\u0422\u043e\u043f"),
    ]
    private_only = [
        BotCommand("pay", "\u041f\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c \u0437\u0432\u0451\u0437\u0434\u044b"),
        BotCommand("ref", "\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430"),
        BotCommand("rozigrish", "\u0420\u043e\u0437\u044b\u0433\u0440\u044b\u0448 \u0434\u043d\u044f"),
    ]
    await application.bot.set_my_commands(common)
    await application.bot.set_my_commands(
        common + private_only, scope=BotCommandScopeAllPrivateChats()
    )
    await application.bot.set_my_commands(
        common, scope=BotCommandScopeAllGroupChats()
    )


async def post_init(application) -> None:
    await setup_bot_commands(application)
    if application.job_queue:
        application.job_queue.run_repeating(background_tick, interval=60, first=10)
        tick_raw = os.getenv("REMINDER_TICK_SEC", "").strip()
        try:
            tick_sec = int(tick_raw) if tick_raw else 6 * 60 * 60
        except ValueError:
            tick_sec = 6 * 60 * 60
        application.job_queue.run_repeating(reminder_tick, interval=tick_sec, first=60)


def bootstrap_env_and_cards() -> Tuple[
    Dict[str, Card],
    Dict[str, List[Card]],
    Dict[str, float],
]:
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

    load_dotenv(ENV_PATH)
    card_map, cards_by_rarity = build_card_index(merged)
    drop_chances = load_drop_chances(env, default_drop)
    return card_map, cards_by_rarity, drop_chances


def migrate_db(db: Dict[str, object]) -> None:
    users = db.get("users", {})
    if not isinstance(users, dict):
        return
    changed = False
    now = now_utc()
    for user in users.values():
        if not isinstance(user, dict):
            continue
        if user.get("vip_reward_pending"):
            user["vip_reward_pending"] = False
            changed = True
        if user.get("vip") and not user.get("vip_until"):
            user["vip_until"] = (now + timedelta(days=VIP_DURATION_DAYS)).isoformat()
            changed = True
        vip_until = parse_iso(user.get("vip_until"))
        if vip_until and vip_until <= now and user.get("vip"):
            user["vip"] = False
            changed = True
    if changed:
        save_db(db)


def acquire_single_instance_lock(path: Path) -> object:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(f"Lock busy: {path}") from exc
    handle.seek(0)
    handle.truncate(0)
    handle.write(f"pid={os.getpid()}\nstarted_at={datetime.utcnow().isoformat()}Z\n")
    handle.flush()
    return handle


def normalize_webhook_path(path: str) -> str:
    cleaned = (path or "").strip()
    if cleaned in {"", "/"}:
        return ""
    return cleaned.lstrip("/")


def main() -> None:
    ensure_utf8()
    ensure_fonts(BASE_DIR)
    card_map, cards_by_rarity, drop_chances = bootstrap_env_and_cards()
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise SystemExit("BOT_TOKEN \u043d\u0435 \u0437\u0430\u0434\u0430\u043d \u0432 .env")

    db = load_db()
    migrate_db(db)
    if sync_exclusive_stock(db, card_map, EXCLUSIVE_STOCK_LIMIT):
        save_db(db)
    logging.basicConfig(level=logging.INFO)
    request = HTTPXRequest(
        connection_pool_size=256,
        connect_timeout=15,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
    )
    get_updates_request = HTTPXRequest(
        connection_pool_size=1,
        connect_timeout=15,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
    )
    application = (
        ApplicationBuilder()
        .token(bot_token)
        .request(request)
        .get_updates_request(get_updates_request)
        .rate_limiter(
            SimpleRateLimiter(
                overall_max_rate=RATE_LIMIT_OVERALL_MAX,
                overall_time_period=RATE_LIMIT_OVERALL_PERIOD,
                group_max_rate=RATE_LIMIT_GROUP_MAX,
                group_time_period=RATE_LIMIT_GROUP_PERIOD,
                max_retries=RATE_LIMIT_MAX_RETRIES,
                min_delay_sec=RATE_LIMIT_MIN_DELAY_SEC,
            )
        )
        .post_init(post_init)
        .build()
    )
    application.bot_data["db"] = db
    application.bot_data["db_lock"] = asyncio.Lock()
    application.bot_data["discount_lock"] = asyncio.Lock()
    application.bot_data["giveaway_lock"] = asyncio.Lock()
    application.bot_data["card_map"] = card_map
    application.bot_data["cards_by_rarity"] = cards_by_rarity
    application.bot_data["drop_chances"] = drop_chances
    discounts = load_discount_data()
    if discounts.get("date") != discount_day_key():
        discounts = generate_discounts(cards_by_rarity)
        save_discount_data(discounts)
    application.bot_data["discounts"] = discounts
    giveaway = load_giveaway_data()
    if giveaway.get("date") != giveaway_day_key():
        giveaway = create_giveaway(cards_by_rarity)
        save_giveaway_data(giveaway)
    application.bot_data["giveaway"] = giveaway

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("sosiska", sosiska_command))
    application.add_handler(CommandHandler("my", my_command))
    application.add_handler(CommandHandler("shop", shop_command))
    application.add_handler(CommandHandler("skidki", skidki_command))
    application.add_handler(CommandHandler("kazik", kazik_command))
    application.add_handler(CommandHandler("pay", stars_menu_command))
    application.add_handler(CommandHandler("ref", ref_command))
    application.add_handler(CommandHandler("vip", vip_menu_command))
    application.add_handler(CommandHandler("rozigrish", rozigrish_command))
    application.add_handler(CommandHandler("trade", trade_command))
    application.add_handler(CommandHandler("trade_accept", trade_accept_command))
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("text", broadcast_text_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(PreCheckoutQueryHandler(precheckout_handler))
    application.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler)
    )
    application.add_error_handler(error_handler)

    mode = os.getenv("BOT_MODE", "polling").strip().lower()
    webhook_url = os.getenv("WEBHOOK_URL", "").strip()
    if mode == "webhook" or webhook_url:
        listen = os.getenv("WEBHOOK_LISTEN", "0.0.0.0").strip() or "0.0.0.0"
        port_raw = os.getenv("WEBHOOK_PORT", "8080").strip() or "8080"
        try:
            port = int(port_raw)
        except ValueError:
            raise SystemExit(f"WEBHOOK_PORT должен быть числом, а не {port_raw!r}")
        url_path = normalize_webhook_path(os.getenv("WEBHOOK_PATH", "/telegram"))
        secret_token = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip() or None
        try:
            application.run_webhook(
                listen=listen,
                port=port,
                url_path=url_path,
                webhook_url=webhook_url or None,
                drop_pending_updates=True,
                secret_token=secret_token,
            )
            return
        except ImportError as exc:
            logging.error(
                "Webhook зависимости не установлены (%s). "
                "Установи: pip install \"python-telegram-bot[webhooks]\"",
                exc,
            )

    lock_path = Path(os.getenv("POLLING_LOCK_FILE", str(DATA_DIR / "polling.lock")))
    try:
        lock_handle = acquire_single_instance_lock(lock_path)
    except RuntimeError:
        raise SystemExit(
            "Уже запущен другой инстанс polling. "
            "Останови его или используй webhook (BOT_MODE=webhook)."
        )
    application.bot_data["polling_lock_handle"] = lock_handle
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

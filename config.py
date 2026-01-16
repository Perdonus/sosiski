import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = Path(os.getenv("ENV_PATH", BASE_DIR / ".env"))
load_dotenv(ENV_PATH, override=True)


def _parse_int(value: Optional[str], default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
STARS_PROVIDER_TOKEN = os.getenv("STARS_PROVIDER_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()
PUBLIC_BOT_USERNAME = os.getenv("PUBLIC_BOT_USERNAME", "").strip()
ADMIN_BROADCAST_USER_ID = _parse_int(os.getenv("ADMIN_BROADCAST_USER_ID"), 6603471853)
BOT_MODE = os.getenv("BOT_MODE", "polling").strip().lower()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
MINIAPP_URL = os.getenv("MINIAPP_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram").strip()
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()
WEBHOOK_LISTEN = os.getenv("WEBHOOK_LISTEN", "0.0.0.0").strip()
WEBHOOK_PORT = _parse_int(os.getenv("WEBHOOK_PORT"), 8080)


def load_env() -> None:
    load_dotenv(ENV_PATH, override=True)


def _parse_float(value: Optional[str], default: float) -> float:
    try:
        return float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return default


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv(
    value: Optional[str],
    default: List[str],
    *,
    cast=str,
) -> List[str]:
    if not value:
        return list(default)
    parts = [item.strip() for item in value.replace(";", ",").split(",")]
    parts = [item for item in parts if item]
    return [cast(item) for item in parts]


def _parse_json(value: Optional[str], default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _resolve_path(raw: Optional[str], default: Path) -> Path:
    if not raw:
        return default
    path = Path(raw)
    if path.is_absolute() or (len(raw) >= 2 and raw[1] == ":"):
        return path
    return BASE_DIR / path


def _resolve_path_from(base: Path, raw: Optional[str], default: Path) -> Path:
    if not raw:
        return default
    path = Path(raw)
    if path.is_absolute() or (len(raw) >= 2 and raw[1] == ":"):
        return path
    return base / path


def _resolve_optional_path(raw: Optional[str]) -> Optional[Path]:
    if not raw:
        return None
    path = Path(raw)
    if path.is_absolute() or (len(raw) >= 2 and raw[1] == ":"):
        return path
    return BASE_DIR / path


LOG_DIR = _resolve_path(os.getenv("LOG_DIR"), BASE_DIR / "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip()
LOG_RUNTIME_FILE = _resolve_path_from(
    LOG_DIR,
    os.getenv("LOG_RUNTIME_FILE"),
    LOG_DIR / "runtime" / "runtime.log",
)
LOG_KAZIK_FILE = _resolve_path_from(
    LOG_DIR,
    os.getenv("LOG_KAZIK_FILE"),
    LOG_DIR / "kazik" / "kazik.log",
)
LOG_CARDS_FILE = _resolve_path_from(
    LOG_DIR,
    os.getenv("LOG_CARDS_FILE"),
    LOG_DIR / "cards" / "cards.log",
)
LOG_GIVEAWAY_FILE = _resolve_path_from(
    LOG_DIR,
    os.getenv("LOG_GIVEAWAY_FILE"),
    LOG_DIR / "giveaway" / "giveaway.log",
)

if not MINIAPP_URL and WEBHOOK_URL:
    MINIAPP_URL = WEBHOOK_URL.rstrip("/") + "/miniapp"


def strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def read_env_file(path: Path) -> Tuple[List[str], Dict[str, str]]:
    if not path.exists():
        return [], {}
    lines = path.read_text(encoding="utf-8").splitlines()
    data = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return lines, data


def upsert_env_lines(lines: List[str], updates: Dict[str, str]) -> List[str]:
    new_lines = []
    seen = set()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            new_lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")
    return new_lines


def _format_env_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        if isinstance(value, set):
            items = sorted(items)
        return ",".join(str(item) for item in items)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _build_env_defaults() -> Dict[str, str]:
    defaults = {
        "ENV_PATH": ENV_PATH,
        "TIMEZONE": TIMEZONE,
        "BOT_TOKEN": BOT_TOKEN,
        "STARS_PROVIDER_TOKEN": STARS_PROVIDER_TOKEN,
        "DATABASE_URL": DATABASE_URL,
        "REDIS_URL": REDIS_URL,
        "PUBLIC_BOT_USERNAME": PUBLIC_BOT_USERNAME,
        "ADMIN_BROADCAST_USER_ID": ADMIN_BROADCAST_USER_ID,
        "BOT_MODE": BOT_MODE,
        "WEBHOOK_URL": WEBHOOK_URL,
        "MINIAPP_URL": MINIAPP_URL,
        "WEBHOOK_PATH": WEBHOOK_PATH,
        "WEBHOOK_SECRET_TOKEN": WEBHOOK_SECRET_TOKEN,
        "WEBHOOK_LISTEN": WEBHOOK_LISTEN,
        "WEBHOOK_PORT": WEBHOOK_PORT,
        "LOG_DIR": LOG_DIR,
        "LOG_LEVEL": LOG_LEVEL,
        "LOG_RUNTIME_FILE": LOG_RUNTIME_FILE,
        "LOG_KAZIK_FILE": LOG_KAZIK_FILE,
        "LOG_CARDS_FILE": LOG_CARDS_FILE,
        "LOG_GIVEAWAY_FILE": LOG_GIVEAWAY_FILE,
        "RARITY_ORDER": RARITY_ORDER,
        "ROLL_RARITY_EXCLUDE": ROLL_RARITY_EXCLUDE,
        "SHOP_RARITY_EXCLUDE": SHOP_RARITY_EXCLUDE,
        "RARITY_NAMES": RARITY_NAMES,
        "RARITY_DIRS": RARITY_DIRS,
        "RARITY_PRICE_MULTIPLIERS": RARITY_PRICE_MULTIPLIERS,
        "IMAGE_EXTENSIONS": IMAGE_EXTENSIONS,
        "DATA_DIR": DATA_DIR,
        "DB_PATH": DB_PATH,
        "SAUSAGE_DIR": SAUSAGE_DIR,
        "PHOTO_CACHE_DIR": PHOTO_CACHE_DIR,
        "LEADERBOARD_BG": LEADERBOARD_BG,
        "ROLL_COOLDOWN_SEC": ROLL_COOLDOWN_SEC,
        "VIP_ROLL_COOLDOWN_SEC": VIP_ROLL_COOLDOWN_SEC,
        "VIP_DAILY_ROLL_LIMIT": VIP_DAILY_ROLL_LIMIT,
        "VIP_DROP_CHANCE_MULTIPLIER": VIP_DROP_CHANCE_MULTIPLIER,
        "VIP_DROP_BOOST_RARITIES": VIP_DROP_BOOST_RARITIES,
        "NON_VIP_DROP_CHANCE_MULTIPLIER": NON_VIP_DROP_CHANCE_MULTIPLIER,
        "NON_VIP_DROP_NERF_RARITIES": NON_VIP_DROP_NERF_RARITIES,
        "NEWBIE_DAYS_STRONG": NEWBIE_DAYS_STRONG,
        "NEWBIE_DAYS_VIP": NEWBIE_DAYS_VIP,
        "NEWBIE_DROP_CHANCE_MULTIPLIER": NEWBIE_DROP_CHANCE_MULTIPLIER,
        "NEWBIE_KAZIK_WIN_MULTIPLIER": NEWBIE_KAZIK_WIN_MULTIPLIER,
        "GIFT_COOLDOWN_SEC": GIFT_COOLDOWN_SEC,
        "GIFT_BUTTONS": GIFT_BUTTONS,
        "GIFT_REWARD_COUNT": GIFT_REWARD_COUNT,
        "CONTRACT_REQUIRED_COUNT": CONTRACT_REQUIRED_COUNT,
        "CONTRACT_SUCCESS_CHANCE": CONTRACT_SUCCESS_CHANCE,
        "CONTRACT_COST_BALANCE": CONTRACT_COST_BALANCE,
        "SHOWCASE_CRAFT_COUNT": SHOWCASE_CRAFT_COUNT,
        "SHOWCASE_CRAFT_COST_BALANCE": SHOWCASE_CRAFT_COST_BALANCE,
        "SHOWCASE_MAX_ACTIVE": SHOWCASE_MAX_ACTIVE,
        "VIP_INFINITE_DAYS": VIP_INFINITE_DAYS,
        "NEWBIE_START_BALANCE": NEWBIE_START_BALANCE,
        "NEWBIE_START_FREE_ROLLS": NEWBIE_START_FREE_ROLLS,
        "KAZIK_SPIN_COST": KAZIK_SPIN_COST,
        "VIP_KAZIK_SPIN_COST": VIP_KAZIK_SPIN_COST,
        "KAZIK_STAR_SPIN_COST": KAZIK_STAR_SPIN_COST,
        "KAZIK_FREE_SPIN_COOLDOWN_SEC": KAZIK_FREE_SPIN_COOLDOWN_SEC,
        "KAZIK_FREE_SPINS_FREE": KAZIK_FREE_SPINS_FREE,
        "KAZIK_FREE_SPINS_VIP": KAZIK_FREE_SPINS_VIP,
        "KAZIK_PAID_SPINS_FOR_BONUS": KAZIK_PAID_SPINS_FOR_BONUS,
        "KAZIK_BONUS_SPINS_PER_BATCH": KAZIK_BONUS_SPINS_PER_BATCH,
        "KAZIK_GUARANTEE_SPINS": KAZIK_GUARANTEE_SPINS,
        "KAZIK_WIN_CHANCE": KAZIK_WIN_CHANCE,
        "VIP_KAZIK_WIN_CHANCE": VIP_KAZIK_WIN_CHANCE,
        "KAZIK_WIN_WEIGHTS": KAZIK_WIN_WEIGHTS,
        "KAZIK_DIGITS": KAZIK_DIGITS,
        "VIP_KAZIK_EXCLUSIVE_CHANCE": VIP_KAZIK_EXCLUSIVE_CHANCE,
        "KAZIK_IMAGE_WIDTH": KAZIK_IMAGE_WIDTH,
        "KAZIK_IMAGE_HEIGHT": KAZIK_IMAGE_HEIGHT,
        "KAZIK_DIGIT_SIZE": KAZIK_DIGIT_SIZE,
        "KAZIK_SLOT_GAP": KAZIK_SLOT_GAP,
        "KAZIK_SLOT_RADIUS": KAZIK_SLOT_RADIUS,
        "KAZIK_SPIN_DELAY": KAZIK_SPIN_DELAY,
        "KAZIK_TITLE_SIZE": KAZIK_TITLE_SIZE,
        "KAZIK_SUBTITLE_SIZE": KAZIK_SUBTITLE_SIZE,
        "STAR_SPIN_COST": STAR_SPIN_COST,
        "STAR_ROLL_FREE_PER_DAY": STAR_ROLL_FREE_PER_DAY,
        "VIP_COST_STARS": VIP_COST_STARS,
        "VIP_COST_RUB": VIP_COST_RUB,
        "VIP_DURATION_DAYS": VIP_DURATION_DAYS,
        "VIP_RENEW_WINDOW_DAYS": VIP_RENEW_WINDOW_DAYS,
        "VIP_STAR_SPIN_COOLDOWN_SEC": VIP_STAR_SPIN_COOLDOWN_SEC,
        "STARS_TOPUP_AMOUNTS": STARS_TOPUP_AMOUNTS,
        "STARS_CURRENCY": STARS_CURRENCY,
        "STAR_DROP_CHANCES": STAR_DROP_CHANCES,
        "STAR_RARITY_ORDER": STAR_RARITY_ORDER,
        "EXCLUSIVE_STOCK_LIMIT": EXCLUSIVE_STOCK_LIMIT,
        "DISCOUNT_FILE": DISCOUNT_FILE,
        "DISCOUNT_ITEMS_PER_DAY": DISCOUNT_ITEMS_PER_DAY,
        "DISCOUNT_PERCENT_MIN": DISCOUNT_PERCENT_MIN,
        "DISCOUNT_PERCENT_MAX": DISCOUNT_PERCENT_MAX,
        "DISCOUNT_RARITY_WEIGHTS": DISCOUNT_RARITY_WEIGHTS,
        "DISCOUNT_QUANTITY_BY_RARITY": DISCOUNT_QUANTITY_BY_RARITY,
        "GIVEAWAY_FILE": GIVEAWAY_FILE,
        "GIVEAWAY_START_HOUR": GIVEAWAY_START_HOUR,
        "GIVEAWAY_SIGNUP_END_HOUR": GIVEAWAY_SIGNUP_END_HOUR,
        "GIVEAWAY_ANNOUNCE_HOUR": GIVEAWAY_ANNOUNCE_HOUR,
        "GIVEAWAY_WINNERS": GIVEAWAY_WINNERS,
        "GIVEAWAY_BALANCE_PRIZES": GIVEAWAY_BALANCE_PRIZES,
        "GIVEAWAY_FREE_ROLLS": GIVEAWAY_FREE_ROLLS,
        "GIVEAWAY_MIN_RARITY": GIVEAWAY_MIN_RARITY,
        "GIVEAWAY_EXCLUSIVE_CHANCE": GIVEAWAY_EXCLUSIVE_CHANCE,
        "REMINDER_INTERVAL_SEC": REMINDER_INTERVAL_SEC,
        "REMINDER_TICK_SEC": REMINDER_TICK_SEC,
        "GIVEAWAY_TICK_SEC": GIVEAWAY_TICK_SEC,
        "RATE_LIMIT_OVERALL_MAX": RATE_LIMIT_OVERALL_MAX,
        "RATE_LIMIT_OVERALL_PERIOD": RATE_LIMIT_OVERALL_PERIOD,
        "RATE_LIMIT_GROUP_MAX": RATE_LIMIT_GROUP_MAX,
        "RATE_LIMIT_GROUP_PERIOD": RATE_LIMIT_GROUP_PERIOD,
        "RATE_LIMIT_MAX_RETRIES": RATE_LIMIT_MAX_RETRIES,
        "RATE_LIMIT_MIN_DELAY_SEC": RATE_LIMIT_MIN_DELAY_SEC,
        "TOP_LIMIT": TOP_LIMIT,
        "MENU_IMAGE_WIDTH": MENU_IMAGE_WIDTH,
        "MENU_IMAGE_HEIGHT": MENU_IMAGE_HEIGHT,
        "MENU_TITLE_SIZE": MENU_TITLE_SIZE,
        "MENU_SUBTITLE_SIZE": MENU_SUBTITLE_SIZE,
        "PROFILE_TITLE_SIZE": PROFILE_TITLE_SIZE,
        "PROFILE_INFO_SIZE": PROFILE_INFO_SIZE,
        "LEADERBOARD_TITLE_SIZE": LEADERBOARD_TITLE_SIZE,
        "LEADERBOARD_SUBTITLE_SIZE": LEADERBOARD_SUBTITLE_SIZE,
        "LEADERBOARD_ENTRY_SIZE": LEADERBOARD_ENTRY_SIZE,
        "LEADERBOARD_AVATAR_SIZE": LEADERBOARD_AVATAR_SIZE,
        "LEADERBOARD_ROW_GAP": LEADERBOARD_ROW_GAP,
        "LEADERBOARD_OUTER_MARGIN": LEADERBOARD_OUTER_MARGIN,
        "LEADERBOARD_PLATE_PADDING": LEADERBOARD_PLATE_PADDING,
        "LEADERBOARD_HEADER_GAP": LEADERBOARD_HEADER_GAP,
        "LEADERBOARD_HEADER_TO_ROWS_GAP": LEADERBOARD_HEADER_TO_ROWS_GAP,
        "FONT_CANDIDATES": FONT_CANDIDATES,
        "CJK_FONT_NAMES": CJK_FONT_NAMES,
        "SYMBOL_FONT_NAMES": SYMBOL_FONT_NAMES,
        "BASE_FONT_PATH": BASE_FONT_PATH or "",
        "BASE_FONT_CJK_PATH": BASE_FONT_CJK_PATH or "",
        "BASE_FONT_SYMBOL_PATH": BASE_FONT_SYMBOL_PATH or "",
        "SOSISKI_FONT_PATH": SOSISKI_FONT_PATH,
        "SOSISKI_FONT_PATHS": SOSISKI_FONT_PATHS,
        "LOGO_FILE": LOGO_FILE,
        "IMAGE_CACHE_VERSION": IMAGE_CACHE_VERSION,
        "PROFILE_FONT_PATH": PROFILE_FONT_PATH or "",
        "PROFILE_FONT_CJK_PATH": PROFILE_FONT_CJK_PATH or "",
        "PROFILE_FONT_SYMBOL_PATH": PROFILE_FONT_SYMBOL_PATH or "",
        "AVATAR_CACHE_TTL_SEC": AVATAR_CACHE_TTL_SEC,
        "SOSISKA_CARDS": {rarity: [] for rarity in DEFAULT_RARITY_ORDER},
    }
    return {key: _format_env_value(value) for key, value in defaults.items()}


def ensure_env_defaults(
    lines: List[str], env: Dict[str, str]
) -> Tuple[List[str], Dict[str, str]]:
    updates = {}
    for key, value in _build_env_defaults().items():
        if key not in env:
            updates[key] = value
    if updates:
        lines = upsert_env_lines(lines, updates)
        env.update(updates)
    return lines, env


DEFAULT_RARITY_ORDER = [
    "dno",
    "common",
    "uncommon",
    "rare",
    "epic",
    "legendary",
    "platinum",
    "meme",
    "exclusive",
]

RARITY_ORDER = _parse_csv(os.getenv("RARITY_ORDER"), DEFAULT_RARITY_ORDER)
ROLL_RARITY_EXCLUDE = set(_parse_csv(os.getenv("ROLL_RARITY_EXCLUDE"), []))
SHOP_RARITY_EXCLUDE = set(_parse_csv(os.getenv("SHOP_RARITY_EXCLUDE"), []))
ROLL_RARITY_ORDER = [
    rarity
    for rarity in RARITY_ORDER
    if rarity != "exclusive" and rarity not in ROLL_RARITY_EXCLUDE
]
SHOP_RARITY_ORDER = [rarity for rarity in RARITY_ORDER if rarity not in SHOP_RARITY_EXCLUDE]

DEFAULT_RARITY_NAMES = {
    "dno": "💩 Дно",
    "common": "🪵 Обычные",
    "uncommon": "🟦 Необычные",
    "rare": "⭐ Редкие",
    "epic": "✨ Эпические",
    "legendary": "🏆 Легендарные",
    "platinum": "💎 Платиновые",
    "meme": "😂 Мемные",
    "exclusive": "👑 Exclusive",
}
RARITY_NAMES = _parse_json(os.getenv("RARITY_NAMES"), DEFAULT_RARITY_NAMES)

DEFAULT_RARITY_DIRS = {
    "dno": "Дно",
    "common": "Обычные",
    "uncommon": "Необычные",
    "rare": "Редкие",
    "epic": "Эпические",
    "legendary": "Легендарные",
    "platinum": "Платиновые",
    "meme": "Мемные",
    "exclusive": "Exclusive",
}
RARITY_DIRS = _parse_json(os.getenv("RARITY_DIRS"), DEFAULT_RARITY_DIRS)

RARITY_PRICE_MULTIPLIERS = _parse_json(os.getenv("RARITY_PRICE_MULTIPLIERS"), {})
if not isinstance(RARITY_PRICE_MULTIPLIERS, dict):
    RARITY_PRICE_MULTIPLIERS = {}
RARITY_PRICE_MULTIPLIERS = {
    str(key): float(value) for key, value in RARITY_PRICE_MULTIPLIERS.items()
}

DEFAULT_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".webm"]
IMAGE_EXTENSIONS: Set[str] = set(
    ext.lower()
    for ext in _parse_csv(os.getenv("IMAGE_EXTENSIONS"), DEFAULT_IMAGE_EXTENSIONS)
)

DATA_DIR = _resolve_path(os.getenv("DATA_DIR"), BASE_DIR / "data")
DB_PATH = _resolve_path(os.getenv("DB_PATH"), DATA_DIR / "db.json")
SAUSAGE_DIR = _resolve_path(os.getenv("SAUSAGE_DIR"), BASE_DIR / "Сосиски")
PHOTO_CACHE_DIR = _resolve_path(os.getenv("PHOTO_CACHE_DIR"), BASE_DIR / "photo")
LEADERBOARD_BG = _resolve_path(os.getenv("LEADERBOARD_BG"), BASE_DIR / "photo.png")

ROLL_COOLDOWN_SEC = _parse_int(os.getenv("ROLL_COOLDOWN_SEC"), 3 * 60 * 60)
VIP_ROLL_COOLDOWN_SEC = _parse_int(os.getenv("VIP_ROLL_COOLDOWN_SEC"), 60 * 60)
VIP_DAILY_ROLL_LIMIT = _parse_int(os.getenv("VIP_DAILY_ROLL_LIMIT"), 7)
VIP_DROP_CHANCE_MULTIPLIER = _parse_float(
    os.getenv("VIP_DROP_CHANCE_MULTIPLIER"), 1.0
)
VIP_DROP_BOOST_RARITIES = _parse_csv(
    os.getenv("VIP_DROP_BOOST_RARITIES"),
    ["rare", "epic", "legendary", "platinum", "meme", "exclusive"],
)
NON_VIP_DROP_CHANCE_MULTIPLIER = _parse_float(
    os.getenv("NON_VIP_DROP_CHANCE_MULTIPLIER"), 0.7
)
NON_VIP_DROP_NERF_RARITIES = _parse_csv(
    os.getenv("NON_VIP_DROP_NERF_RARITIES"),
    list(VIP_DROP_BOOST_RARITIES),
)
NEWBIE_DAYS_STRONG = _parse_int(os.getenv("NEWBIE_DAYS_STRONG"), 1)
NEWBIE_DAYS_VIP = _parse_int(os.getenv("NEWBIE_DAYS_VIP"), 3)
NEWBIE_DROP_CHANCE_MULTIPLIER = _parse_float(
    os.getenv("NEWBIE_DROP_CHANCE_MULTIPLIER"), 1.0
)
NEWBIE_KAZIK_WIN_MULTIPLIER = _parse_float(
    os.getenv("NEWBIE_KAZIK_WIN_MULTIPLIER"), 1.0
)
GIFT_COOLDOWN_SEC = _parse_int(os.getenv("GIFT_COOLDOWN_SEC"), 6 * 60 * 60)
GIFT_BUTTONS = _parse_int(os.getenv("GIFT_BUTTONS"), 3)
GIFT_REWARD_COUNT = _parse_int(os.getenv("GIFT_REWARD_COUNT"), 3)
CONTRACT_REQUIRED_COUNT = _parse_int(os.getenv("CONTRACT_REQUIRED_COUNT"), 4)
CONTRACT_SUCCESS_CHANCE = _parse_float(os.getenv("CONTRACT_SUCCESS_CHANCE"), 0.8)
CONTRACT_COST_BALANCE = _parse_int(os.getenv("CONTRACT_COST_BALANCE"), 250)
SHOWCASE_CRAFT_COUNT = _parse_int(os.getenv("SHOWCASE_CRAFT_COUNT"), 5)
SHOWCASE_CRAFT_COST_BALANCE = _parse_int(
    os.getenv("SHOWCASE_CRAFT_COST_BALANCE"), 5000
)
SHOWCASE_MAX_ACTIVE = _parse_int(os.getenv("SHOWCASE_MAX_ACTIVE"), 3)
VIP_INFINITE_DAYS = _parse_int(os.getenv("VIP_INFINITE_DAYS"), 36500)
NEWBIE_START_BALANCE = _parse_int(os.getenv("NEWBIE_START_BALANCE"), 500)
NEWBIE_START_FREE_ROLLS = _parse_int(os.getenv("NEWBIE_START_FREE_ROLLS"), 50)

KAZIK_SPIN_COST = _parse_int(os.getenv("KAZIK_SPIN_COST"), 10)
VIP_KAZIK_SPIN_COST = _parse_int(os.getenv("VIP_KAZIK_SPIN_COST"), 5)
KAZIK_STAR_SPIN_COST = _parse_int(os.getenv("KAZIK_STAR_SPIN_COST"), 1)
KAZIK_FREE_SPIN_COOLDOWN_SEC = _parse_int(
    os.getenv("KAZIK_FREE_SPIN_COOLDOWN_SEC"), 86400
)
KAZIK_FREE_SPINS_FREE = _parse_int(os.getenv("KAZIK_FREE_SPINS_FREE"), 5)
KAZIK_FREE_SPINS_VIP = _parse_int(os.getenv("KAZIK_FREE_SPINS_VIP"), 10)
KAZIK_PAID_SPINS_FOR_BONUS = _parse_int(
    os.getenv("KAZIK_PAID_SPINS_FOR_BONUS"), 5
)
KAZIK_BONUS_SPINS_PER_BATCH = _parse_int(
    os.getenv("KAZIK_BONUS_SPINS_PER_BATCH"), 1
)
KAZIK_GUARANTEE_SPINS = _parse_int(os.getenv("KAZIK_GUARANTEE_SPINS"), 0)
KAZIK_WIN_CHANCE = _parse_float(os.getenv("KAZIK_WIN_CHANCE"), 0.01)
VIP_KAZIK_WIN_CHANCE = _parse_float(os.getenv("VIP_KAZIK_WIN_CHANCE"), 0.015)
KAZIK_WIN_WEIGHTS = _parse_json(
    os.getenv("KAZIK_WIN_WEIGHTS"), {1: 0.6, 2: 0.3, 3: 0.1}
)
if isinstance(KAZIK_WIN_WEIGHTS, dict):
    KAZIK_WIN_WEIGHTS = {
        int(key): float(value) for key, value in KAZIK_WIN_WEIGHTS.items()
    }
KAZIK_DIGITS = [int(item) for item in _parse_csv(os.getenv("KAZIK_DIGITS"), ["1", "2", "3"])]
VIP_KAZIK_EXCLUSIVE_CHANCE = _parse_float(
    os.getenv("VIP_KAZIK_EXCLUSIVE_CHANCE"), 0.04
)

KAZIK_IMAGE_WIDTH = _parse_int(os.getenv("KAZIK_IMAGE_WIDTH"), 900)
KAZIK_IMAGE_HEIGHT = _parse_int(os.getenv("KAZIK_IMAGE_HEIGHT"), 360)
KAZIK_DIGIT_SIZE = _parse_int(os.getenv("KAZIK_DIGIT_SIZE"), 120)
KAZIK_SLOT_GAP = _parse_int(os.getenv("KAZIK_SLOT_GAP"), 30)
KAZIK_SLOT_RADIUS = _parse_int(os.getenv("KAZIK_SLOT_RADIUS"), 26)
KAZIK_SPIN_DELAY = _parse_float(os.getenv("KAZIK_SPIN_DELAY"), 1.0)
KAZIK_TITLE_SIZE = _parse_int(os.getenv("KAZIK_TITLE_SIZE"), 60)
KAZIK_SUBTITLE_SIZE = _parse_int(os.getenv("KAZIK_SUBTITLE_SIZE"), 30)

STAR_SPIN_COST = _parse_int(os.getenv("STAR_SPIN_COST"), 5)
STAR_ROLL_FREE_PER_DAY = _parse_int(os.getenv("STAR_ROLL_FREE_PER_DAY"), 1)
VIP_COST_STARS = _parse_int(os.getenv("VIP_COST_STARS"), 25)
VIP_COST_RUB = _parse_int(os.getenv("VIP_COST_RUB"), 50)
VIP_DURATION_DAYS = _parse_int(os.getenv("VIP_DURATION_DAYS"), 30)
VIP_RENEW_WINDOW_DAYS = _parse_int(os.getenv("VIP_RENEW_WINDOW_DAYS"), 3)
VIP_STAR_SPIN_COOLDOWN_SEC = _parse_int(
    os.getenv("VIP_STAR_SPIN_COOLDOWN_SEC"), 86400
)
STARS_TOPUP_AMOUNTS = tuple(
    int(item)
    for item in _parse_csv(
        os.getenv("STARS_TOPUP_AMOUNTS"), ["5", "15", "25", "50", "100"]
    )
)
STARS_CURRENCY = os.getenv("STARS_CURRENCY", "XTR")
STAR_DROP_CHANCES = _parse_json(
    os.getenv("STAR_DROP_CHANCES"),
    {
        "uncommon": 5.0,
        "rare": 18.0,
        "epic": 22.0,
        "legendary": 22.0,
        "platinum": 12.0,
        "meme": 12.0,
        "exclusive": 6.0,
    },
)
STAR_DROP_CHANCES = {
    str(key): float(value) for key, value in STAR_DROP_CHANCES.items()
}
STAR_RARITY_ORDER = _parse_csv(
    os.getenv("STAR_RARITY_ORDER"),
    ["uncommon", "rare", "epic", "legendary", "platinum", "meme"],
)

EXCLUSIVE_STOCK_LIMIT = _parse_int(os.getenv("EXCLUSIVE_STOCK_LIMIT"), 3)

DISCOUNT_FILE = _resolve_path(
    os.getenv("DISCOUNT_FILE"), BASE_DIR / "data" / "discounts.json"
)
DISCOUNT_ITEMS_PER_DAY = _parse_int(os.getenv("DISCOUNT_ITEMS_PER_DAY"), 5)
DISCOUNT_PERCENT_MIN = _parse_int(os.getenv("DISCOUNT_PERCENT_MIN"), 10)
DISCOUNT_PERCENT_MAX = _parse_int(os.getenv("DISCOUNT_PERCENT_MAX"), 40)
DISCOUNT_RARITY_WEIGHTS = _parse_json(
    os.getenv("DISCOUNT_RARITY_WEIGHTS"),
    {
        "dno": 6,
        "common": 5,
        "uncommon": 4,
        "rare": 3,
        "epic": 2,
        "legendary": 1,
        "platinum": 0.5,
        "meme": 0.5,
    },
)
DISCOUNT_RARITY_WEIGHTS = {
    str(key): float(value) for key, value in DISCOUNT_RARITY_WEIGHTS.items()
}
DISCOUNT_QUANTITY_BY_RARITY = _parse_json(
    os.getenv("DISCOUNT_QUANTITY_BY_RARITY"),
    {
        "dno": 20,
        "common": 20,
        "uncommon": 20,
        "rare": 10,
        "epic": 10,
        "legendary": 5,
        "platinum": 1,
        "meme": 1,
    },
)
DISCOUNT_QUANTITY_BY_RARITY = {
    str(key): int(value) for key, value in DISCOUNT_QUANTITY_BY_RARITY.items()
}

GIVEAWAY_FILE = _resolve_path(
    os.getenv("GIVEAWAY_FILE"), BASE_DIR / "data" / "giveaway.json"
)
GIVEAWAY_START_HOUR = _parse_int(os.getenv("GIVEAWAY_START_HOUR"), 12)
GIVEAWAY_SIGNUP_END_HOUR = _parse_int(os.getenv("GIVEAWAY_SIGNUP_END_HOUR"), 17)
GIVEAWAY_ANNOUNCE_HOUR = _parse_int(os.getenv("GIVEAWAY_ANNOUNCE_HOUR"), 18)
GIVEAWAY_WINNERS = _parse_int(os.getenv("GIVEAWAY_WINNERS"), 5)
GIVEAWAY_BALANCE_PRIZES = tuple(
    int(item)
    for item in _parse_csv(os.getenv("GIVEAWAY_BALANCE_PRIZES"), ["100", "250", "500"])
)
GIVEAWAY_FREE_ROLLS = _parse_int(os.getenv("GIVEAWAY_FREE_ROLLS"), 1)
GIVEAWAY_MIN_RARITY = os.getenv("GIVEAWAY_MIN_RARITY", "epic")
GIVEAWAY_EXCLUSIVE_CHANCE = _parse_float(
    os.getenv("GIVEAWAY_EXCLUSIVE_CHANCE"), 0.02
)
REMINDER_INTERVAL_SEC = _parse_int(
    os.getenv("REMINDER_INTERVAL_SEC"), 2 * 24 * 60 * 60
)
REMINDER_TICK_SEC = _parse_int(os.getenv("REMINDER_TICK_SEC"), 6 * 60 * 60)
GIVEAWAY_TICK_SEC = _parse_int(os.getenv("GIVEAWAY_TICK_SEC"), 10 * 60)

RATE_LIMIT_OVERALL_MAX = _parse_int(os.getenv("RATE_LIMIT_OVERALL_MAX"), 25)
RATE_LIMIT_OVERALL_PERIOD = _parse_float(os.getenv("RATE_LIMIT_OVERALL_PERIOD"), 1.0)
RATE_LIMIT_GROUP_MAX = _parse_int(os.getenv("RATE_LIMIT_GROUP_MAX"), 18)
RATE_LIMIT_GROUP_PERIOD = _parse_float(os.getenv("RATE_LIMIT_GROUP_PERIOD"), 60.0)
RATE_LIMIT_MAX_RETRIES = _parse_int(os.getenv("RATE_LIMIT_MAX_RETRIES"), 2)
RATE_LIMIT_MIN_DELAY_SEC = _parse_float(
    os.getenv("RATE_LIMIT_MIN_DELAY_SEC"), 0.0
)

TOP_LIMIT = _parse_int(os.getenv("TOP_LIMIT"), 10)

MENU_IMAGE_WIDTH = _parse_int(os.getenv("MENU_IMAGE_WIDTH"), 900)
MENU_IMAGE_HEIGHT = _parse_int(os.getenv("MENU_IMAGE_HEIGHT"), 480)
MENU_TITLE_SIZE = _parse_int(os.getenv("MENU_TITLE_SIZE"), 44)
MENU_SUBTITLE_SIZE = _parse_int(os.getenv("MENU_SUBTITLE_SIZE"), 26)
PROFILE_TITLE_SIZE = _parse_int(os.getenv("PROFILE_TITLE_SIZE"), 40)
PROFILE_INFO_SIZE = _parse_int(os.getenv("PROFILE_INFO_SIZE"), 28)
LEADERBOARD_TITLE_SIZE = _parse_int(os.getenv("LEADERBOARD_TITLE_SIZE"), 52)
LEADERBOARD_SUBTITLE_SIZE = _parse_int(os.getenv("LEADERBOARD_SUBTITLE_SIZE"), 28)
LEADERBOARD_ENTRY_SIZE = _parse_int(os.getenv("LEADERBOARD_ENTRY_SIZE"), 34)
LEADERBOARD_AVATAR_SIZE = _parse_int(os.getenv("LEADERBOARD_AVATAR_SIZE"), 64)
LEADERBOARD_ROW_GAP = _parse_int(os.getenv("LEADERBOARD_ROW_GAP"), 18)
LEADERBOARD_OUTER_MARGIN = _parse_int(os.getenv("LEADERBOARD_OUTER_MARGIN"), 80)
LEADERBOARD_PLATE_PADDING = _parse_int(os.getenv("LEADERBOARD_PLATE_PADDING"), 50)
LEADERBOARD_HEADER_GAP = _parse_int(os.getenv("LEADERBOARD_HEADER_GAP"), 12)
LEADERBOARD_HEADER_TO_ROWS_GAP = _parse_int(
    os.getenv("LEADERBOARD_HEADER_TO_ROWS_GAP"), 28
)

DEFAULT_FONT_CANDIDATES = [
    BASE_DIR / "fonts" / "NotoSans-Regular.ttf",
    BASE_DIR / "fonts" / "NotoSansCJK-Regular.ttc",
    BASE_DIR / "fonts" / "NotoSansSymbols2-Regular.ttf",
    BASE_DIR / "fonts" / "NotoColorEmoji.ttf",
    BASE_DIR / "fonts" / "NotoEmoji-Regular.ttf",
    BASE_DIR / "fonts" / "DejaVuSans.ttf",
    BASE_DIR / "fonts" / "Symbola.ttf",
    Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf"),
    Path("/usr/share/fonts/truetype/noto/NotoEmoji-Regular.ttf"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"),
    Path("/usr/share/fonts/truetype/noto/NotoSansSymbols2-Regular.ttf"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
    Path("/usr/share/fonts/truetype/freefont/FreeSans.ttf"),
    Path("C:/Windows/Fonts/segoeui.ttf"),
    Path("C:/Windows/Fonts/arial.ttf"),
    Path("C:/Windows/Fonts/arialuni.ttf"),
]

_fonts_raw = os.getenv("FONT_CANDIDATES")
if _fonts_raw:
    FONT_CANDIDATES = [
        _resolve_path(item, BASE_DIR / item)
        for item in _parse_csv(_fonts_raw, [], cast=str)
    ]
else:
    FONT_CANDIDATES = DEFAULT_FONT_CANDIDATES

CJK_FONT_NAMES = set(
    _parse_csv(os.getenv("CJK_FONT_NAMES"), ["NotoSansCJK-Regular.ttc"])
)
SYMBOL_FONT_NAMES = set(
    _parse_csv(
        os.getenv("SYMBOL_FONT_NAMES"),
        [
            "NotoSansSymbols2-Regular.ttf",
            "Symbola.ttf",
            "NotoColorEmoji.ttf",
            "NotoEmoji-Regular.ttf",
        ],
    )
)
BASE_FONT_PATH = _resolve_optional_path(os.getenv("BASE_FONT_PATH"))
BASE_FONT_CJK_PATH = _resolve_optional_path(os.getenv("BASE_FONT_CJK_PATH"))
BASE_FONT_SYMBOL_PATH = _resolve_optional_path(os.getenv("BASE_FONT_SYMBOL_PATH"))
SOSISKI_FONT_PATH = os.getenv("SOSISKI_FONT_PATH", "").strip()
SOSISKI_FONT_PATHS = os.getenv("SOSISKI_FONT_PATHS", "").strip()
LOGO_FILE = os.getenv("LOGO_FILE", "").strip()
IMAGE_CACHE_VERSION = os.getenv("IMAGE_CACHE_VERSION", "v5").strip() or "v5"
PROFILE_FONT_PATH = _resolve_optional_path(os.getenv("PROFILE_FONT_PATH"))
PROFILE_FONT_CJK_PATH = _resolve_optional_path(os.getenv("PROFILE_FONT_CJK_PATH"))
PROFILE_FONT_SYMBOL_PATH = _resolve_optional_path(os.getenv("PROFILE_FONT_SYMBOL_PATH"))

AVATAR_CACHE_TTL_SEC = _parse_int(os.getenv("AVATAR_CACHE_TTL_SEC"), 21600)

DROP_CHANCE_KEYS = {
    "dno": "DROP_CHANCE_DNO",
    "common": "DROP_CHANCE_COMMON",
    "uncommon": "DROP_CHANCE_UNCOMMON",
    "rare": "DROP_CHANCE_RARE",
    "epic": "DROP_CHANCE_EPIC",
    "legendary": "DROP_CHANCE_LEGENDARY",
    "platinum": "DROP_CHANCE_PLATINUM",
    "meme": "DROP_CHANCE_MEME",
}

__all__ = [
    "BASE_DIR",
    "ENV_PATH",
    "TIMEZONE",
    "BOT_TOKEN",
    "STARS_PROVIDER_TOKEN",
    "DATABASE_URL",
    "REDIS_URL",
    "PUBLIC_BOT_USERNAME",
    "ADMIN_BROADCAST_USER_ID",
    "BOT_MODE",
    "WEBHOOK_URL",
    "MINIAPP_URL",
    "WEBHOOK_PATH",
    "WEBHOOK_SECRET_TOKEN",
    "WEBHOOK_LISTEN",
    "WEBHOOK_PORT",
    "DATA_DIR",
    "DB_PATH",
    "SAUSAGE_DIR",
    "PHOTO_CACHE_DIR",
    "LEADERBOARD_BG",
    "RARITY_ORDER",
    "ROLL_RARITY_ORDER",
    "ROLL_RARITY_EXCLUDE",
    "SHOP_RARITY_ORDER",
    "SHOP_RARITY_EXCLUDE",
    "RARITY_NAMES",
    "RARITY_DIRS",
    "RARITY_PRICE_MULTIPLIERS",
    "IMAGE_EXTENSIONS",
    "DROP_CHANCE_KEYS",
    "ROLL_COOLDOWN_SEC",
    "VIP_ROLL_COOLDOWN_SEC",
    "VIP_DAILY_ROLL_LIMIT",
    "VIP_DROP_CHANCE_MULTIPLIER",
    "VIP_DROP_BOOST_RARITIES",
    "NON_VIP_DROP_CHANCE_MULTIPLIER",
    "NON_VIP_DROP_NERF_RARITIES",
    "NEWBIE_DAYS_STRONG",
    "NEWBIE_DAYS_VIP",
    "NEWBIE_DROP_CHANCE_MULTIPLIER",
    "NEWBIE_KAZIK_WIN_MULTIPLIER",
    "GIFT_COOLDOWN_SEC",
    "GIFT_BUTTONS",
    "GIFT_REWARD_COUNT",
    "CONTRACT_REQUIRED_COUNT",
    "CONTRACT_SUCCESS_CHANCE",
    "CONTRACT_COST_BALANCE",
    "SHOWCASE_CRAFT_COUNT",
    "SHOWCASE_CRAFT_COST_BALANCE",
    "SHOWCASE_MAX_ACTIVE",
    "VIP_INFINITE_DAYS",
    "NEWBIE_START_BALANCE",
    "NEWBIE_START_FREE_ROLLS",
    "KAZIK_SPIN_COST",
    "VIP_KAZIK_SPIN_COST",
    "KAZIK_STAR_SPIN_COST",
    "KAZIK_FREE_SPIN_COOLDOWN_SEC",
    "KAZIK_FREE_SPINS_FREE",
    "KAZIK_FREE_SPINS_VIP",
    "KAZIK_PAID_SPINS_FOR_BONUS",
    "KAZIK_BONUS_SPINS_PER_BATCH",
    "KAZIK_GUARANTEE_SPINS",
    "KAZIK_WIN_CHANCE",
    "VIP_KAZIK_WIN_CHANCE",
    "KAZIK_WIN_WEIGHTS",
    "KAZIK_DIGITS",
    "VIP_KAZIK_EXCLUSIVE_CHANCE",
    "KAZIK_IMAGE_WIDTH",
    "KAZIK_IMAGE_HEIGHT",
    "KAZIK_DIGIT_SIZE",
    "KAZIK_SLOT_GAP",
    "KAZIK_SLOT_RADIUS",
    "KAZIK_SPIN_DELAY",
    "KAZIK_TITLE_SIZE",
    "KAZIK_SUBTITLE_SIZE",
    "STAR_SPIN_COST",
    "STAR_ROLL_FREE_PER_DAY",
    "VIP_COST_STARS",
    "VIP_COST_RUB",
    "VIP_DURATION_DAYS",
    "VIP_RENEW_WINDOW_DAYS",
    "VIP_STAR_SPIN_COOLDOWN_SEC",
    "STARS_TOPUP_AMOUNTS",
    "STARS_CURRENCY",
    "STAR_DROP_CHANCES",
    "STAR_RARITY_ORDER",
    "EXCLUSIVE_STOCK_LIMIT",
    "DISCOUNT_FILE",
    "DISCOUNT_ITEMS_PER_DAY",
    "DISCOUNT_PERCENT_MIN",
    "DISCOUNT_PERCENT_MAX",
    "DISCOUNT_RARITY_WEIGHTS",
    "DISCOUNT_QUANTITY_BY_RARITY",
    "GIVEAWAY_FILE",
    "GIVEAWAY_START_HOUR",
    "GIVEAWAY_SIGNUP_END_HOUR",
    "GIVEAWAY_ANNOUNCE_HOUR",
    "GIVEAWAY_WINNERS",
    "GIVEAWAY_BALANCE_PRIZES",
    "GIVEAWAY_FREE_ROLLS",
    "GIVEAWAY_MIN_RARITY",
    "GIVEAWAY_EXCLUSIVE_CHANCE",
    "REMINDER_INTERVAL_SEC",
    "REMINDER_TICK_SEC",
    "GIVEAWAY_TICK_SEC",
    "RATE_LIMIT_OVERALL_MAX",
    "RATE_LIMIT_OVERALL_PERIOD",
    "RATE_LIMIT_GROUP_MAX",
    "RATE_LIMIT_GROUP_PERIOD",
    "RATE_LIMIT_MAX_RETRIES",
    "RATE_LIMIT_MIN_DELAY_SEC",
    "TOP_LIMIT",
    "MENU_IMAGE_WIDTH",
    "MENU_IMAGE_HEIGHT",
    "MENU_TITLE_SIZE",
    "MENU_SUBTITLE_SIZE",
    "PROFILE_TITLE_SIZE",
    "PROFILE_INFO_SIZE",
    "LEADERBOARD_TITLE_SIZE",
    "LEADERBOARD_SUBTITLE_SIZE",
    "LEADERBOARD_ENTRY_SIZE",
    "LEADERBOARD_AVATAR_SIZE",
    "LEADERBOARD_ROW_GAP",
    "LEADERBOARD_OUTER_MARGIN",
    "LEADERBOARD_PLATE_PADDING",
    "LEADERBOARD_HEADER_GAP",
    "LEADERBOARD_HEADER_TO_ROWS_GAP",
    "FONT_CANDIDATES",
    "CJK_FONT_NAMES",
    "SYMBOL_FONT_NAMES",
    "BASE_FONT_PATH",
    "BASE_FONT_CJK_PATH",
    "BASE_FONT_SYMBOL_PATH",
    "SOSISKI_FONT_PATH",
    "SOSISKI_FONT_PATHS",
    "LOGO_FILE",
    "IMAGE_CACHE_VERSION",
    "PROFILE_FONT_PATH",
    "PROFILE_FONT_CJK_PATH",
    "PROFILE_FONT_SYMBOL_PATH",
    "AVATAR_CACHE_TTL_SEC",
    "strip_quotes",
    "read_env_file",
    "upsert_env_lines",
    "ensure_env_defaults",
]

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import asyncio
from typing import Optional

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from config import (
    BASE_DIR,
    BOT_MODE,
    BOT_TOKEN,
    EXCLUSIVE_STOCK_LIMIT,
    RATE_LIMIT_GROUP_MAX,
    RATE_LIMIT_GROUP_PERIOD,
    RATE_LIMIT_MIN_DELAY_SEC,
    RATE_LIMIT_OVERALL_MAX,
    RATE_LIMIT_OVERALL_PERIOD,
    REDIS_URL,
    WEBHOOK_LISTEN,
    WEBHOOK_PATH,
    WEBHOOK_PORT,
    WEBHOOK_SECRET_TOKEN,
    WEBHOOK_URL,
)
from font_setup import ensure_fonts, ensure_utf8
from app.cards_loader import load_cards
from app.background import run_background_tasks
from app.db import create_pool, init_db, migrate_from_json
from app.handlers import routers
from app.miniapp import setup_miniapp
from app.ratelimit import RateLimiter
from app.repo import sync_exclusive_stock
from app.ownership_middleware import OwnershipMiddleware
from app.chat_registry_middleware import ChatRegistryMiddleware
from app.dm_required_middleware import DmRequiredMiddleware
from app.logging_setup import setup_logging


async def build_storage() -> MemoryStorage | RedisStorage:
    if REDIS_URL:
        return RedisStorage.from_url(REDIS_URL)
    return MemoryStorage()


async def run_polling(bot: Bot, dispatcher: Dispatcher) -> None:
    await dispatcher.start_polling(bot, allowed_updates=None)


async def run_webhook(
    bot: Bot,
    dispatcher: Dispatcher,
    *,
    db_pool,
    cards_by_rarity,
    card_map,
) -> None:
    app = web.Application()
    app["db_pool"] = db_pool
    app["cards_by_rarity"] = cards_by_rarity
    app["card_map"] = card_map
    app["bot"] = bot
    setup_miniapp(app)
    webhook_path = WEBHOOK_PATH if WEBHOOK_PATH.startswith("/") else f"/{WEBHOOK_PATH}"
    handler = SimpleRequestHandler(dispatcher=dispatcher, bot=bot, secret_token=WEBHOOK_SECRET_TOKEN or None)
    handler.register(app, path=webhook_path)
    setup_application(app, dispatcher, bot=bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEBHOOK_LISTEN, port=WEBHOOK_PORT)
    await site.start()

    if WEBHOOK_URL:
        await bot.set_webhook(
            url=WEBHOOK_URL + webhook_path,
            secret_token=WEBHOOK_SECRET_TOKEN or None,
            drop_pending_updates=True,
        )
    while True:
        await asyncio.sleep(3600)


async def main() -> None:
    ensure_utf8()
    ensure_fonts(BASE_DIR)
    setup_logging()
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set")

    pool = await create_pool()
    await init_db(pool)
    await migrate_from_json(pool)

    card_map, cards_by_rarity, drop_chances = load_cards()
    exclusive_cards = cards_by_rarity.get("exclusive", [])
    if exclusive_cards:
        await sync_exclusive_stock(
            pool,
            [card.file for card in exclusive_cards],
            EXCLUSIVE_STOCK_LIMIT,
        )

    storage = await build_storage()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher(storage=storage)
    dispatcher.message.middleware(DmRequiredMiddleware())
    dispatcher.callback_query.middleware(DmRequiredMiddleware())
    dispatcher.callback_query.middleware(OwnershipMiddleware())
    chat_registry = ChatRegistryMiddleware()
    dispatcher.message.middleware(chat_registry)
    dispatcher.channel_post.middleware(chat_registry)
    dispatcher.edited_channel_post.middleware(chat_registry)
    dispatcher.my_chat_member.middleware(chat_registry)
    dispatcher["db_pool"] = pool
    dispatcher["card_map"] = card_map
    dispatcher["cards_by_rarity"] = cards_by_rarity
    dispatcher["drop_chances"] = drop_chances
    dispatcher["rate_limiter"] = RateLimiter(
        RATE_LIMIT_OVERALL_MAX,
        RATE_LIMIT_OVERALL_PERIOD,
        RATE_LIMIT_GROUP_MAX,
        RATE_LIMIT_GROUP_PERIOD,
        RATE_LIMIT_MIN_DELAY_SEC,
    )
    for router in routers:
        dispatcher.include_router(router)

    await run_background_tasks(
        bot,
        pool,
        cards_by_rarity,
        card_map,
        dispatcher["rate_limiter"],
    )

    mode = BOT_MODE or "polling"
    if mode == "webhook" or WEBHOOK_URL:
        await run_webhook(
            bot,
            dispatcher,
            db_pool=pool,
            cards_by_rarity=cards_by_rarity,
            card_map=card_map,
        )
    else:
        await run_polling(bot, dispatcher)


if __name__ == "__main__":
    asyncio.run(main())

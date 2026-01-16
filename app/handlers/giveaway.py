from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from app.giveaway import add_giveaway_entry, announce_giveaway, ensure_giveaway, giveaway_phase
from app.utils import now_local

router = Router()


@router.message(Command("rozigrish"))
async def giveaway_command(
    message: Message,
    db_pool,
    cards_by_rarity,
    card_map,
) -> None:
    if message.chat.type != "private":
        await message.answer("Розыгрыш доступен только в личке с ботом.")
        return
    now = now_local()
    giveaway = await ensure_giveaway(db_pool)
    if not giveaway:
        await message.answer("Сегодня розыгрыш не запланирован.")
        return
    phase = giveaway_phase(now)
    if phase == "announce" and giveaway.get("status") != "announced":
        await announce_giveaway(db_pool, message.bot, giveaway, card_map)
        giveaway = await ensure_giveaway(db_pool)
    if phase == "idle":
        await message.answer("Розыгрыш еще не начался.")
        return
    if phase == "closed":
        await message.answer("Регистрация закрыта.")
        return
    if phase == "announce":
        await message.answer("Итоги уже оглашены.")
        return
    added, _ = await add_giveaway_entry(db_pool, message.from_user.id, giveaway)
    await message.answer("Ты участвуешь в розыгрыше!" if added else "Ты уже участвуешь.")

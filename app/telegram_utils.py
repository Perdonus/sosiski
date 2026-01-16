from __future__ import annotations

from io import BytesIO
from typing import Optional

from aiogram import Bot

from app.repo import update_user_fields


async def _download_by_file_id(bot: Bot, file_id: str) -> Optional[bytes]:
    try:
        file = await bot.get_file(file_id)
    except Exception:
        return None
    buffer = BytesIO()
    try:
        await bot.download_file(file.file_path, destination=buffer)
    except Exception:
        return None
    return buffer.getvalue()


async def fetch_user_avatar(
    bot: Bot,
    user_id: int,
    *,
    cached_file_id: Optional[str] = None,
    db_pool=None,
) -> Optional[bytes]:
    if cached_file_id:
        downloaded = await _download_by_file_id(bot, cached_file_id)
        if downloaded:
            return downloaded
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
    except Exception:
        photos = None
    if not photos or not photos.photos:
        sizes = None
    else:
        sizes = photos.photos[0]
    if sizes:
        file_id = sizes[-1].file_id
        downloaded = await _download_by_file_id(bot, file_id)
        if downloaded:
            if db_pool and file_id != cached_file_id:
                try:
                    await update_user_fields(db_pool, user_id, {"avatar_file_id": file_id})
                except Exception:
                    pass
            return downloaded
    try:
        chat = await bot.get_chat(user_id)
    except Exception:
        return None
    photo = getattr(chat, "photo", None)
    if not photo:
        return None
    file_id = getattr(photo, "big_file_id", None)
    if not file_id:
        return None
    downloaded = await _download_by_file_id(bot, file_id)
    if downloaded and db_pool and file_id != cached_file_id:
        try:
            await update_user_fields(db_pool, user_id, {"avatar_file_id": file_id})
        except Exception:
            pass
    return downloaded

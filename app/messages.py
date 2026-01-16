from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from typing import Optional

from aiogram.types import (
    BufferedInputFile,
    FSInputFile,
    InlineKeyboardMarkup,
    InputFile,
    InputMediaAnimation,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter

from app.ratelimit import RateLimiter
from app.ownership import remember_owner
from config import RATE_LIMIT_MAX_RETRIES


def _payload_is_empty(payload) -> bool:
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


def _rewind(payload) -> None:
    try:
        seeker = getattr(payload, "seek", None)
        if callable(seeker):
            seeker(0)
    except Exception:
        return


def _media_name(media) -> str:
    if isinstance(media, (str, Path)):
        return str(media)
    return (
        getattr(media, "filename", "")
        or getattr(media, "name", "")
        or getattr(media, "path", "")
        or ""
    )


def _coerce_input_file(media):
    if isinstance(media, InputFile):
        return media
    if isinstance(media, BytesIO):
        return BufferedInputFile(media.getvalue(), filename="image.jpg")
    if isinstance(media, (str, Path)):
        return FSInputFile(str(media))
    if hasattr(media, "read"):
        name = getattr(media, "name", "")
        if name:
            return FSInputFile(str(name))
        try:
            if hasattr(media, "seek"):
                media.seek(0)
            data = media.read() or b""
        except Exception:
            data = b""
        return BufferedInputFile(data, filename="image.jpg")
    return media


async def send_or_edit_media(
    message: Message,
    media,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup],
    prefer_edit: bool,
    *,
    rate_limiter: Optional[RateLimiter] = None,
    parse_mode: Optional[str] = None,
    owner_id: Optional[int] = None,
) -> Message:
    media = _coerce_input_file(media)
    name = _media_name(media)
    ext = Path(str(name)).suffix.lower()
    animation_extensions = {".gif"}
    video_extensions = {".mp4", ".webm"}

    kind = "photo"
    if ext in animation_extensions:
        kind = "animation"
    elif ext in video_extensions:
        kind = "video"

    async def call_with_retry(call, *args, **kwargs):
        attempt = 0
        delay = 0.5
        while True:
            if rate_limiter:
                try:
                    chat_id = message.chat.id if message.chat else None
                except Exception:
                    chat_id = None
                await rate_limiter.acquire(chat_id)
            try:
                return await call(*args, **kwargs)
            except TelegramRetryAfter as exc:
                if rate_limiter:
                    await rate_limiter.register_retry_after(exc.retry_after)
                await asyncio.sleep(max(0.1, float(exc.retry_after)))
            except TelegramNetworkError:
                if attempt >= RATE_LIMIT_MAX_RETRIES:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)
            attempt += 1
            if attempt > RATE_LIMIT_MAX_RETRIES:
                return await call(*args, **kwargs)

    if _payload_is_empty(media):
        if prefer_edit:
            sent = await call_with_retry(
                message.edit_text,
                caption,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
        else:
            sent = await call_with_retry(
                message.answer,
                caption,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
        if owner_id is not None:
            remember_owner(sent.chat.id, sent.message_id, owner_id)
        return sent

    if prefer_edit:
        try:
            _rewind(media)
            if kind == "animation":
                input_media = InputMediaAnimation(media=media, caption=caption, parse_mode=parse_mode)
            elif kind == "video":
                input_media = InputMediaVideo(media=media, caption=caption, parse_mode=parse_mode)
            else:
                input_media = InputMediaPhoto(media=media, caption=caption, parse_mode=parse_mode)
            sent = await call_with_retry(
                message.edit_media, input_media, reply_markup=reply_markup
            )
            if owner_id is not None:
                remember_owner(sent.chat.id, sent.message_id, owner_id)
            return sent
        except Exception:
            _rewind(media)
            if kind == "animation":
                sent = await call_with_retry(
                    message.answer_animation,
                    media,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                if owner_id is not None:
                    remember_owner(sent.chat.id, sent.message_id, owner_id)
                return sent
            if kind == "video":
                sent = await call_with_retry(
                    message.answer_video,
                    media,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                if owner_id is not None:
                    remember_owner(sent.chat.id, sent.message_id, owner_id)
                return sent
            sent = await call_with_retry(
                message.answer_photo,
                media,
                caption=caption,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            if owner_id is not None:
                remember_owner(sent.chat.id, sent.message_id, owner_id)
            return sent

    _rewind(media)
    if kind == "animation":
        sent = await call_with_retry(
            message.answer_animation,
            media,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        if owner_id is not None:
            remember_owner(sent.chat.id, sent.message_id, owner_id)
        return sent
    if kind == "video":
        sent = await call_with_retry(
            message.answer_video,
            media,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        if owner_id is not None:
            remember_owner(sent.chat.id, sent.message_id, owner_id)
        return sent
    sent = await call_with_retry(
        message.answer_photo,
        media,
        caption=caption,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
    )
    if owner_id is not None:
        remember_owner(sent.chat.id, sent.message_id, owner_id)
    return sent

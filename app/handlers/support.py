from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from aiogram import F, Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message

from app.repo import get_user, update_user_fields
from app.utils import now_local
from config import ADMIN_BROADCAST_USER_ID, LOG_DIR

router = Router()

_SUPPORT_INPUT_MODE = "support_message"
_TIMESTAMP_RE = re.compile(r"^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})")


def _extract_recent_log_lines(path: Path, minutes: int = 5) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return ""
    if not lines:
        return ""
    cutoff = now_local() - timedelta(minutes=minutes)
    recent = []
    tzinfo = now_local().tzinfo
    for line in lines[-5000:]:
        match = _TIMESTAMP_RE.match(line)
        if not match:
            continue
        try:
            ts = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
            if tzinfo is not None:
                ts = ts.replace(tzinfo=tzinfo)
        except Exception:
            continue
        if ts >= cutoff:
            recent.append(line)
    if not recent:
        return "\n".join(lines[-200:])
    return "\n".join(recent)


def _collect_recent_logs(paths: list[Path], minutes: int = 5) -> str:
    chunks = []
    for path in paths:
        name = path.name
        lines = _extract_recent_log_lines(path, minutes=minutes)
        if not lines:
            lines = "Лог пуст."
        chunks.append(f"===== {name} =====\n{lines}")
    return "\n\n".join(chunks)


def _list_log_files() -> list[Path]:
    if LOG_DIR.exists():
        files = sorted({path for path in LOG_DIR.rglob("*.log") if path.is_file()})
        if files:
            return files
    return []


async def _send_support_request(message: Message, db_pool, text: str) -> None:
    user = message.from_user
    if not user:
        return
    admin_id = int(ADMIN_BROADCAST_USER_ID)
    log_files = _list_log_files()
    log_text = _collect_recent_logs(log_files, minutes=5) if log_files else "Логов нет."
    support_dir = LOG_DIR / "support"
    support_dir.mkdir(parents=True, exist_ok=True)
    filename = f"support_{user.id}_{now_local().strftime('%Y%m%d_%H%M%S')}.log"
    path = support_dir / filename
    path.write_text(log_text or "Нет логов за последние 5 минут.", encoding="utf-8")
    zip_path = support_dir / f"support_full_{user.id}_{now_local().strftime('%Y%m%d_%H%M%S')}.zip"
    if log_files:
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
            for log_path in log_files:
                try:
                    archive.write(log_path, log_path.relative_to(LOG_DIR))
                except Exception:
                    continue

    user_label = f"{user.full_name} (@{user.username})" if user.username else user.full_name
    header = (
        "Новый запрос /support\n"
        f"Пользователь: {user_label}\n"
        f"ID: {user.id}\n"
        f"Сообщение: {text}"
    )
    await message.bot.send_message(chat_id=admin_id, text=header)
    await message.bot.send_document(
        chat_id=admin_id,
        document=FSInputFile(str(path)),
        caption="Логи за последние 5 минут.",
    )
    if log_files and zip_path.exists():
        await message.bot.send_document(
            chat_id=admin_id,
            document=FSInputFile(str(zip_path)),
            caption="Полный архив логов.",
        )


@router.message(Command("support"))
async def support_command(message: Message, db_pool) -> None:
    if not message.from_user:
        return
    raw_text = (message.text or "").strip()
    payload = raw_text.partition(" ")[2].strip()
    if payload:
        await _send_support_request(message, db_pool, payload)
        await message.answer("Запрос отправлен. Спасибо!")
        return
    if message.chat.type != "private":
        await message.answer("Напиши /support и текст в одном сообщении.")
        return
    await update_user_fields(
        db_pool, message.from_user.id, {"input_mode": _SUPPORT_INPUT_MODE}
    )
    await message.answer("Напиши сообщение для поддержки.")


@router.message(F.text & ~F.text.startswith("/"))
async def support_text_input(message: Message, db_pool) -> None:
    if message.chat.type != "private":
        raise SkipHandler()
    if not message.from_user:
        return
    user = await get_user(db_pool, message.from_user.id)
    if not user:
        raise SkipHandler()
    mode = str(user.get("input_mode") or "")
    if mode != _SUPPORT_INPUT_MODE:
        raise SkipHandler()
    text = (message.text or "").strip()
    if not text:
        await message.answer("Напиши сообщение текстом.")
        return
    await update_user_fields(db_pool, message.from_user.id, {"input_mode": None})
    await _send_support_request(message, db_pool, text)
    await message.answer("Запрос отправлен. Спасибо!")

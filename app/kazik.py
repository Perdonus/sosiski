from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from app.logic import is_vip
from app.utils import now_local
from config import (
    KAZIK_FREE_SPIN_COOLDOWN_SEC,
    KAZIK_FREE_SPINS_FREE,
    KAZIK_FREE_SPINS_VIP,
)


def _get_reset_started_at(user: Dict[str, object]) -> Optional[datetime]:
    raw = user.get("kazik_reset_started_at")
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    return None


def kazik_should_reset(user: Dict[str, object], now: Optional[datetime] = None) -> bool:
    if now is None:
        now = now_local()
    started_at = _get_reset_started_at(user)
    if not started_at:
        return int(user.get("kazik_daily_used", 0) or 0) > 0
    if started_at.tzinfo is None and now.tzinfo is not None:
        started_at = started_at.replace(tzinfo=now.tzinfo)
    elif started_at.tzinfo is not None and now.tzinfo is None:
        now = now.replace(tzinfo=started_at.tzinfo)
    elapsed = (now - started_at).total_seconds()
    return elapsed >= KAZIK_FREE_SPIN_COOLDOWN_SEC


def kazik_reset_remaining_seconds(
    user: Dict[str, object],
    now: Optional[datetime] = None,
) -> int:
    if now is None:
        now = now_local()
    started_at = _get_reset_started_at(user)
    if not started_at:
        return 0
    if started_at.tzinfo is None and now.tzinfo is not None:
        started_at = started_at.replace(tzinfo=now.tzinfo)
    elif started_at.tzinfo is not None and now.tzinfo is None:
        now = now.replace(tzinfo=started_at.tzinfo)
    elapsed = int((now - started_at).total_seconds())
    remaining = KAZIK_FREE_SPIN_COOLDOWN_SEC - elapsed
    return max(0, remaining)


def kazik_free_spins_limit(user: Dict[str, object]) -> int:
    return KAZIK_FREE_SPINS_VIP if is_vip(user) else KAZIK_FREE_SPINS_FREE


def kazik_free_spins_left(user: Dict[str, object], now: Optional[datetime] = None) -> int:
    if now is None:
        now = now_local()
    bonus = int(user.get("kazik_bonus_spins", 0) or 0)
    used = int(user.get("kazik_daily_used", 0) or 0)
    if kazik_should_reset(user, now):
        used = 0
    limit = kazik_free_spins_limit(user)
    return bonus + max(0, limit - used)


def kazik_daily_free_left(user: Dict[str, object], now: Optional[datetime] = None) -> int:
    if now is None:
        now = now_local()
    used = int(user.get("kazik_daily_used", 0) or 0)
    if kazik_should_reset(user, now):
        used = 0
    limit = kazik_free_spins_limit(user)
    return max(0, limit - used)


def kazik_spin_button_label(user: Dict[str, object]) -> str:
    daily_free = kazik_daily_free_left(user)
    if daily_free > 0:
        return f"Покрутить (FREE: {daily_free})"
    return "Покрутить"

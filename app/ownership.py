from __future__ import annotations

from typing import Dict, Optional, Tuple

_OWNER_MAP: Dict[Tuple[int, int], int] = {}


def remember_owner(chat_id: int, message_id: int, owner_id: int) -> None:
    _OWNER_MAP[(int(chat_id), int(message_id))] = int(owner_id)


def get_owner(chat_id: int, message_id: int) -> Optional[int]:
    return _OWNER_MAP.get((int(chat_id), int(message_id)))


def is_owner(chat_id: int, message_id: int, user_id: int) -> bool:
    return get_owner(chat_id, message_id) == int(user_id)

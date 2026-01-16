from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

TURN_TIMEOUT_SEC = 60
BOARD_SIZE = 8


def _initial_board() -> List[List[Optional[str]]]:
    board: List[List[Optional[str]]] = [[None for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]
    board[0] = ["bR", "bN", "bB", "bQ", "bK", "bB", "bN", "bR"]
    board[1] = ["bP"] * BOARD_SIZE
    board[6] = ["wP"] * BOARD_SIZE
    board[7] = ["wR", "wN", "wB", "wQ", "wK", "wB", "wN", "wR"]
    return board


def _in_bounds(row: int, col: int) -> bool:
    return 0 <= row < BOARD_SIZE and 0 <= col < BOARD_SIZE


def _piece_color(piece: Optional[str]) -> Optional[str]:
    if not piece:
        return None
    return piece[0]


def _piece_kind(piece: Optional[str]) -> str:
    return piece[1] if piece else ""


def _path_clear(board: List[List[Optional[str]]], fr: int, fc: int, tr: int, tc: int) -> bool:
    dr = tr - fr
    dc = tc - fc
    step_r = 0 if dr == 0 else (1 if dr > 0 else -1)
    step_c = 0 if dc == 0 else (1 if dc > 0 else -1)
    r = fr + step_r
    c = fc + step_c
    while (r, c) != (tr, tc):
        if board[r][c]:
            return False
        r += step_r
        c += step_c
    return True


def _legal_move(
    board: List[List[Optional[str]]],
    fr: int,
    fc: int,
    tr: int,
    tc: int,
    color: str,
) -> bool:
    if not _in_bounds(fr, fc) or not _in_bounds(tr, tc):
        return False
    if fr == tr and fc == tc:
        return False
    piece = board[fr][fc]
    if not piece or _piece_color(piece) != color:
        return False
    target = board[tr][tc]
    if target and _piece_color(target) == color:
        return False
    kind = _piece_kind(piece)
    dr = tr - fr
    dc = tc - fc
    abs_dr = abs(dr)
    abs_dc = abs(dc)

    if kind == "P":
        direction = -1 if color == "w" else 1
        start_row = 6 if color == "w" else 1
        if dc == 0:
            if dr == direction and target is None:
                return True
            if fr == start_row and dr == 2 * direction and target is None:
                mid_row = fr + direction
                return board[mid_row][fc] is None
        if abs_dc == 1 and dr == direction and target is not None:
            return True
        return False

    if kind == "N":
        return (abs_dr, abs_dc) in {(1, 2), (2, 1)}

    if kind == "B":
        if abs_dr != abs_dc:
            return False
        return _path_clear(board, fr, fc, tr, tc)

    if kind == "R":
        if dr != 0 and dc != 0:
            return False
        return _path_clear(board, fr, fc, tr, tc)

    if kind == "Q":
        if abs_dr == abs_dc or dr == 0 or dc == 0:
            return _path_clear(board, fr, fc, tr, tc)
        return False

    if kind == "K":
        return max(abs_dr, abs_dc) == 1

    return False


def _set_turn(state: Dict[str, object], user_id: Optional[int], color: Optional[str]) -> None:
    state["turn_owner_id"] = int(user_id) if user_id is not None else None
    state["turn_started_at"] = int(time.time()) if user_id is not None else None
    if color:
        state["turn"] = color


def _player_by_id(players: List[Dict[str, object]], user_id: int) -> Optional[Dict[str, object]]:
    for player in players:
        if int(player.get("user_id", 0)) == int(user_id):
            return player
    return None


def _other_player_id(state: Dict[str, object], user_id: int) -> Optional[int]:
    for player in state.get("players", []):
        if int(player.get("user_id", 0)) != int(user_id):
            return int(player.get("user_id", 0))
    return None


def init_chess_state(players: List[Dict[str, object]]) -> Dict[str, object]:
    players = [
        {**player, "color": "white" if idx == 0 else "black"}
        for idx, player in enumerate(players)
    ]
    board = _initial_board()
    white_id = int(players[0]["user_id"]) if players else None
    state = {
        "status": "active",
        "board": board,
        "players": players,
        "turn": "white",
        "winner_id": None,
    }
    _set_turn(state, white_id, "white")
    return state


def serialize_chess_state(state: Dict[str, object]) -> Dict[str, object]:
    return {
        "status": state.get("status"),
        "board": state.get("board"),
        "players": state.get("players", []),
        "turn": state.get("turn"),
        "turn_owner_id": state.get("turn_owner_id"),
        "turn_started_at": state.get("turn_started_at"),
        "turn_timeout_sec": TURN_TIMEOUT_SEC,
        "winner_id": state.get("winner_id"),
    }


def apply_chess_action(
    state: Dict[str, object],
    user_id: int,
    action: str,
    payload: Dict[str, object],
) -> Tuple[bool, Optional[str]]:
    if state.get("status") != "active":
        return False, "game_closed"
    user_id = int(user_id)
    players = state.get("players", [])
    player = _player_by_id(players, user_id)
    if not player:
        return False, "not_player"
    if int(state.get("turn_owner_id") or 0) != user_id:
        return False, "not_turn"

    board = state.get("board") or []
    if action == "resign":
        winner_id = _other_player_id(state, user_id)
        state["winner_id"] = winner_id
        state["status"] = "finished"
        _set_turn(state, None, None)
        return True, None

    if action != "move":
        return False, "action"

    try:
        fr = int(payload.get("from_row", -1))
        fc = int(payload.get("from_col", -1))
        tr = int(payload.get("to_row", -1))
        tc = int(payload.get("to_col", -1))
    except (TypeError, ValueError):
        return False, "coords"

    color = "w" if player.get("color") == "white" else "b"
    if not _legal_move(board, fr, fc, tr, tc, color):
        return False, "invalid_move"
    target = board[tr][tc]
    board[tr][tc] = board[fr][fc]
    board[fr][fc] = None

    if board[tr][tc] and _piece_kind(board[tr][tc]) == "P":
        if color == "w" and tr == 0:
            board[tr][tc] = "wQ"
        if color == "b" and tr == BOARD_SIZE - 1:
            board[tr][tc] = "bQ"

    if target and _piece_kind(target) == "K":
        state["winner_id"] = user_id
        state["status"] = "finished"
        _set_turn(state, None, None)
        return True, None

    other_id = _other_player_id(state, user_id)
    other_player = _player_by_id(players, other_id) if other_id else None
    next_color = other_player.get("color") if other_player else None
    _set_turn(state, other_id, next_color)
    return True, None


def apply_chess_timeout(state: Dict[str, object], now_ts: Optional[int] = None) -> bool:
    if state.get("status") != "active":
        return False
    owner_id = state.get("turn_owner_id")
    if not owner_id:
        return False
    started = int(state.get("turn_started_at") or 0)
    if started <= 0:
        _set_turn(state, int(owner_id), state.get("turn"))
        return False
    now_value = int(now_ts or time.time())
    if (now_value - started) < TURN_TIMEOUT_SEC:
        return False
    winner_id = _other_player_id(state, int(owner_id))
    state["winner_id"] = winner_id
    state["status"] = "finished"
    _set_turn(state, None, None)
    return True

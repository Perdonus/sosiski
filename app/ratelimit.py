from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Dict, Optional, Union


class SlidingWindowLimiter:
    def __init__(self, max_rate: int, time_period: float) -> None:
        self._max_rate = max(0, int(max_rate))
        self._time_period = max(0.0, float(time_period))
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


class RateLimiter:
    def __init__(
        self,
        overall_max_rate: int,
        overall_time_period: float,
        group_max_rate: int,
        group_time_period: float,
        min_delay_sec: float = 0.0,
    ) -> None:
        self._overall = (
            SlidingWindowLimiter(overall_max_rate, overall_time_period)
            if overall_max_rate and overall_time_period
            else None
        )
        self._group_max_rate = max(0, int(group_max_rate))
        self._group_time_period = max(0.0, float(group_time_period))
        self._group_limiters: Dict[Union[int, str], SlidingWindowLimiter] = {}
        self._retry_after_until = 0.0
        self._retry_lock = asyncio.Lock()
        self._min_delay = max(0.0, float(min_delay_sec))
        self._min_delay_lock = asyncio.Lock()
        self._min_delay_until = 0.0

    def _get_group_limiter(self, key: Union[int, str]) -> SlidingWindowLimiter:
        limiter = self._group_limiters.get(key)
        if limiter is None:
            limiter = SlidingWindowLimiter(self._group_max_rate, self._group_time_period)
            self._group_limiters[key] = limiter
        return limiter

    async def _wait_for_retry_after(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            async with self._retry_lock:
                wait_for = self._retry_after_until - loop.time()
            if wait_for <= 0:
                return
            await asyncio.sleep(wait_for)

    async def register_retry_after(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        async with self._retry_lock:
            until = loop.time() + max(0.0, float(delay)) + 0.1
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

    async def acquire(self, chat_id: Optional[Union[int, str]] = None) -> None:
        await self._wait_for_retry_after()
        if chat_id and self._group_max_rate and self._group_time_period:
            await self._get_group_limiter(chat_id).acquire()
        if self._overall:
            await self._overall.acquire()
        await self._wait_for_min_delay()

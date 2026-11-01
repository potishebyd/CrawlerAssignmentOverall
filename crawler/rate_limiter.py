"""Ограничение числа запросов в секунду (день 4)."""

from __future__ import annotations

import asyncio
import random


class RateLimiter:
    """Держит паузу между запросами.

    При ``per_domain=True`` у каждого домена свой ритм. При ``False`` все
    домены делят один общий ритм. Первый запрос к ключу проходит сразу,
    следующие ждут не меньше интервала ``1 / requests_per_second``,
    ``min_delay`` и дополнительной задержки этого вызова.
    """

    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        *,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть > 0")
        if min_delay < 0 or jitter < 0:
            raise ValueError("min_delay и jitter не могут быть отрицательными")
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self._interval = (
            0.0 if requests_per_second == float("inf") else 1.0 / requests_per_second
        )
        self._last_reserved: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._waits: list[float] = []
        self._request_times: list[float] = []

    async def acquire(self, domain: str | None = None, extra_delay: float = 0.0) -> float:
        """Ждёт разрешения и возвращает, сколько секунд реально прождал.

        ``domain`` выбирает отдельный счётчик, если включён режим по доменам.
        ``extra_delay`` — добавка к паузе, например Crawl-delay или backoff.
        """
        key = self._key(domain)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        async with lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            gap = max(self._interval, self.min_delay, max(0.0, extra_delay))
            if self.jitter > 0:
                gap += random.uniform(0, self.jitter)
            previous = self._last_reserved.get(key)
            if previous is None:
                wait = 0.0
                reserved = now
            else:
                reserved = max(now, previous + gap)
                wait = reserved - now
            self._last_reserved[key] = reserved
        if wait > 0:
            await asyncio.sleep(wait)
        self._waits.append(wait)
        self._request_times.append(asyncio.get_running_loop().time())
        return wait

    def get_stats(self) -> dict[str, float | int]:
        """Сколько запросов прошло, средняя пауза и средняя скорость."""
        count = len(self._request_times)
        average_delay = sum(self._waits) / count if count else 0.0
        if count >= 2:
            span = self._request_times[-1] - self._request_times[0]
            rate = (count - 1) / span if span > 0 else 0.0
        else:
            rate = 0.0
        min_interval = min(self._waits[1:]) if count > 1 else 0.0
        return {
            "requests": count,
            "requests_per_second": rate,
            "average_delay": average_delay,
            "min_interval": min_interval,
        }

    def reset_stats(self) -> None:
        """Обнуляет накопленную статистику, не сбрасывая расписание."""
        self._waits.clear()
        self._request_times.clear()

    def _key(self, domain: str | None) -> str:
        if not self.per_domain:
            return "*"
        return (domain or "*").lower()

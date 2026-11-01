"""Глобальный и подоменный пределы одновременных запросов (день 3)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit


class SemaphoreManager:
    """Держит два предела: на все запросы сразу и на каждый домен отдельно.

    Слот домена занимается раньше глобального, поэтому ожидание домена
    не запирает свободные места других доменов. Активные запросы видны
    через ``get_stats``.
    """

    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 10) -> None:
        if max_concurrent < 1 or max_per_domain < 1:
            raise ValueError("пределы конкурентности должны быть >= 1")
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains: dict[str, asyncio.Semaphore] = {}
        self._active_urls: set[str] = set()
        self._active_per_domain: dict[str, int] = {}
        self._active = 0
        self._peak_active = 0

    @asynccontextmanager
    async def limit(self, url: str) -> AsyncIterator[None]:
        """Занимает слот домена и глобальный слот на время запроса."""
        host = _host_of(url)
        domain = self._domain_semaphore(host)
        async with domain:
            async with self._global:
                self._active += 1
                self._peak_active = max(self._peak_active, self._active)
                self._active_urls.add(url)
                self._active_per_domain[host] = self._active_per_domain.get(host, 0) + 1
                try:
                    yield
                finally:
                    self._active -= 1
                    self._active_urls.discard(url)
                    left = self._active_per_domain.get(host, 0) - 1
                    if left > 0:
                        self._active_per_domain[host] = left
                    else:
                        self._active_per_domain.pop(host, None)

    def get_stats(self) -> dict:
        """Сколько запросов выполняется сейчас и каким был пик."""
        return {
            "active": self._active,
            "peak_active": self._peak_active,
            "active_urls": sorted(self._active_urls),
            "active_per_domain": dict(self._active_per_domain),
            "max_concurrent": self.max_concurrent,
            "max_per_domain": self.max_per_domain,
        }

    def _domain_semaphore(self, host: str) -> asyncio.Semaphore:
        semaphore = self._domains.get(host)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self.max_per_domain)
            self._domains[host] = semaphore
        return semaphore


def _host_of(url: str) -> str:
    parts = urlsplit(url)
    return (parts.hostname or parts.netloc or "_").lower()

"""Очередь URL с приоритетами для обхода сайта (день 3)."""

from __future__ import annotations

import asyncio
import heapq
import logging
from urllib.parse import urldefrag, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

_HTTP_SCHEMES = {"http", "https"}


def normalize_url(url: str) -> str:
    """Приводит http(s)-адрес к одному виду.

    Фрагмент после ``#`` отбрасывается, схема и хост приводятся к нижнему
    регистру. Пустая строка означает, что адрес не годится для обхода.
    """
    raw = url.strip()
    if not raw:
        return ""
    try:
        without_fragment, _fragment = urldefrag(raw)
        parts = urlsplit(without_fragment)
    except ValueError:
        return ""
    if parts.scheme.lower() not in _HTTP_SCHEMES or not parts.netloc:
        return ""
    return urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, "")
    )


class CrawlerQueue:
    """Очередь адресов: выше приоритет — раньше выдача, при равенстве FIFO.

    Повторный адрес не добавляется. ``get_next`` ждёт новый URL, а когда
    обход запущен, очередь пуста и никто ничего не обрабатывает, возвращает
    ``None``.
    """

    def __init__(self, max_pages: int | None = None) -> None:
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages должен быть >= 1")
        self._max_pages = max_pages
        self._heap: list[tuple[int, int, str]] = []
        self._seq = 0
        self._depths: dict[str, int] = {}
        self._seen: set[str] = set()
        self._inflight: set[str] = set()
        self._finished: set[str] = set()
        self._processed: list[str] = []
        self._failed: dict[str, str] = {}
        self._in_progress = 0
        self._waiters: list[asyncio.Future[None]] = []
        self._started = False
        self._closed = False

    @property
    def seen(self) -> set[str]:
        return self._seen

    @property
    def failed(self) -> dict[str, str]:
        return self._failed

    def depth_of(self, url: str) -> int:
        return self._depths.get(normalize_url(url), 0)

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """Ставит адрес в очередь. Повтор и негодный URL возвращают ``False``."""
        normalized = normalize_url(url)
        if not normalized or normalized in self._seen or self._closed:
            return False
        self._seen.add(normalized)
        self._depths[normalized] = depth
        heapq.heappush(self._heap, (-priority, self._seq, normalized))
        self._seq += 1
        self._wake_all()
        return True

    def mark_started(self) -> None:
        """Разрешает завершение: пустая очередь без активных задач — это конец."""
        self._started = True
        self._wake_all()

    async def get_next(self) -> str | None:
        """Возвращает следующий URL или ``None``, когда обход закончен."""
        while True:
            if self._closed:
                return None
            url = self._try_pop()
            if url is not None:
                return url
            if self._should_stop():
                self._closed = True
                self._wake_all()
                return None
            loop = asyncio.get_running_loop()
            waiter: asyncio.Future[None] = loop.create_future()
            self._waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

    def mark_processed(self, url: str) -> None:
        """Помечает URL успешно обработанным и освобождает место в лимите."""
        self._finish(url, error=None)

    def mark_failed(self, url: str, error: str) -> None:
        """Помечает URL как неудачный и запоминает текст ошибки."""
        self._finish(url, error=error)

    def get_stats(self) -> dict[str, int]:
        """Снимок очереди: ожидают, в работе, готовы, с ошибкой."""
        return {
            "pending": len(self._heap),
            "in_progress": self._in_progress,
            "processed": len(self._processed),
            "failed": len(self._failed),
            "seen": len(self._seen),
        }

    def _try_pop(self) -> str | None:
        if self._closed or not self._heap or self._at_capacity():
            return None
        _priority, _seq, url = heapq.heappop(self._heap)
        self._in_progress += 1
        self._inflight.add(url)
        return url

    def _at_capacity(self) -> bool:
        if self._max_pages is None:
            return False
        started = self._in_progress + len(self._processed) + len(self._failed)
        return started >= self._max_pages

    def _should_stop(self) -> bool:
        if not self._started or self._in_progress > 0:
            return False
        return not self._heap or self._at_capacity()

    def _finish(self, url: str, error: str | None) -> None:
        normalized = normalize_url(url)
        if not normalized or normalized in self._finished:
            return
        self._finished.add(normalized)
        if error is None:
            self._processed.append(normalized)
        else:
            self._failed[normalized] = error
            logger.warning("URL помечен как неудачный %s: %s", normalized, error)
        if normalized in self._inflight:
            self._inflight.remove(normalized)
            self._in_progress -= 1
        self._wake_all()

    def _wake_all(self) -> None:
        waiters = self._waiters
        self._waiters = []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

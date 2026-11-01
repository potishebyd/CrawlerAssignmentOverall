"""Загрузка и проверка robots.txt (день 4)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

logger = logging.getLogger(__name__)

Download = Callable[[str], Awaitable[tuple[int, str]]]


@dataclass
class _RobotsRecord:
    origin: str
    url: str
    status: int
    groups: list[dict] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)
    parser: RobotFileParser | None = None
    allow_all: bool = False
    disallow_all: bool = False

    def as_dict(self, *, cached: bool) -> dict:
        return {
            "url": self.url,
            "origin": self.origin,
            "status": self.status,
            "groups": self.groups,
            "sitemaps": self.sitemaps,
            "cached": cached,
        }


class RobotsParser:
    """Кэширует robots.txt по домену и отвечает, можно ли открывать адрес."""

    def __init__(self, download: Download | None = None) -> None:
        self._download = download
        self._cache: dict[str, _RobotsRecord] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_origin: str | None = None

    async def fetch_robots(self, base_url: str) -> dict:
        """Скачивает robots.txt домена и возвращает разобранные правила.

        Повторный вызов для того же домена берёт результат из кэша.
        """
        if self._download is None:
            raise RuntimeError("RobotsParser создан без функции загрузки")
        origin = origin_of(base_url)
        cached = self._cache.get(origin)
        if cached is not None:
            return cached.as_dict(cached=True)

        lock = self._locks.get(origin)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[origin] = lock
        async with lock:
            cached = self._cache.get(origin)
            if cached is not None:
                return cached.as_dict(cached=True)
            robots_url = f"{origin}/robots.txt"
            try:
                status, body = await self._download(robots_url)
            except Exception as exc:
                logger.warning(
                    "Не удалось загрузить %s: %s. Считаю сайт разрешённым.",
                    robots_url,
                    type(exc).__name__,
                )
                status, body = 0, ""
            record = _build_record(origin, robots_url, status, body)
            self._cache[origin] = record
            self._last_origin = origin
            return record.as_dict(cached=False)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Разрешён ли адрес этому User-Agent правилами Disallow и Allow."""
        record = self._record_for(url)
        if record is None or record.allow_all:
            return True
        if record.disallow_all:
            return False
        if record.parser is None:
            return True
        return bool(record.parser.can_fetch(user_agent, url))

    def get_crawl_delay(self, user_agent: str = "*", url: str | None = None) -> float:
        """Возвращает Crawl-delay в секундах для User-Agent и домена URL.

        Без ``url`` берётся домен последнего загруженного robots.txt.
        Дробные значения вроде ``0.5`` сохраняются.
        """
        if url:
            record = self._record_for(url)
        else:
            record = self._cache.get(self._last_origin or "")
        if record is None:
            return 0.0
        return _delay_for(record.groups, user_agent)

    def _record_for(self, url: str) -> _RobotsRecord | None:
        return self._cache.get(origin_of(url))


def origin_of(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def _build_record(origin: str, robots_url: str, status: int, body: str) -> _RobotsRecord:
    if status in (401, 403) or 500 <= status < 600:
        logger.warning("robots.txt %s ответил %s, обход домена запрещён", robots_url, status)
        return _RobotsRecord(
            origin=origin,
            url=robots_url,
            status=status,
            disallow_all=True,
        )
    if status == 0 or status >= 400 or not body.strip():
        return _RobotsRecord(
            origin=origin,
            url=robots_url,
            status=status,
            allow_all=True,
        )

    groups, sitemaps = _parse_groups(body)
    parser = RobotFileParser()
    parser.set_url(robots_url)
    parser.parse(body.splitlines())
    return _RobotsRecord(
        origin=origin,
        url=robots_url,
        status=status,
        groups=groups,
        sitemaps=sitemaps,
        parser=parser,
    )


def _parse_groups(body: str) -> tuple[list[dict], list[str]]:
    groups: list[dict] = []
    sitemaps: list[str] = []
    current: dict | None = None

    def finish() -> None:
        nonlocal current
        if current is not None and current["user_agents"]:
            groups.append(current)
        current = None

    for raw_line in body.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "sitemap":
            if value:
                sitemaps.append(value)
            continue
        if key == "user-agent":
            if current is not None and (
                current["allow"] or current["disallow"] or current["crawl_delay"] is not None
            ):
                finish()
            if current is None:
                current = _new_group()
            current["user_agents"].append(value)
            continue
        if current is None:
            continue
        if key == "disallow":
            if value:
                current["disallow"].append(value)
        elif key == "allow":
            if value:
                current["allow"].append(value)
        elif key == "crawl-delay":
            try:
                current["crawl_delay"] = float(value)
            except ValueError:
                logger.warning("Пропущен Crawl-delay %r", value)
    finish()
    return groups, sitemaps


def _new_group() -> dict:
    return {"user_agents": [], "allow": [], "disallow": [], "crawl_delay": None}


def _delay_for(groups: list[dict], user_agent: str) -> float:
    token = (user_agent or "*").split("/", 1)[0].lower()
    specific: float | None = None
    fallback = 0.0
    for group in groups:
        agents = [agent.lower() for agent in group["user_agents"]]
        delay = float(group["crawl_delay"] or 0.0)
        if any(agent != "*" and agent in token for agent in agents):
            specific = delay
        elif "*" in agents:
            fallback = delay
    if specific is not None:
        return specific
    return fallback

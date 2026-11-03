"""Разбор sitemap.xml и sitemap index (день 7)."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

Download = Callable[[str], Awaitable[tuple[int, str]]]


class SitemapParser:
    """Достаёт адреса страниц из sitemap и из индекса sitemap.

    Индекс раскрывается рекурсивно: каждый вложенный файл читается отдельно.
    Повтор одного и того же файла не уходит в бесконечный цикл.
    """

    def __init__(self, download: Download, *, max_sitemaps: int = 50) -> None:
        if max_sitemaps < 1:
            raise ValueError("max_sitemaps должен быть >= 1")
        self._download = download
        self.max_sitemaps = max_sitemaps

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Возвращает адреса страниц из sitemap или из всех файлов индекса."""
        return await self._walk(sitemap_url, seen=set(), pages=[])

    async def _walk(self, sitemap_url: str, seen: set[str], pages: list[str]) -> list[str]:
        if sitemap_url in seen or len(seen) >= self.max_sitemaps or len(pages) >= _MAX_PAGES:
            return pages
        seen.add(sitemap_url)
        try:
            status, body = await self._download(sitemap_url)
        except Exception as exc:
            logger.warning("Не удалось скачать sitemap %s: %s", sitemap_url, type(exc).__name__)
            return pages
        if status == 0 or status >= 400 or not body.strip():
            logger.warning("Sitemap %s ответил %s", sitemap_url, status)
            return pages
        try:
            kind, locs = _parse_sitemap(body)
        except ET.ParseError:
            logger.warning("Sitemap %s не разобрался как XML", sitemap_url)
            return pages
        if kind == "index":
            for loc in locs:
                await self._walk(loc, seen, pages)
                if len(pages) >= _MAX_PAGES:
                    break
            return pages
        for loc in locs:
            if loc in pages or len(pages) >= _MAX_PAGES:
                continue
            pages.append(loc)
        return pages


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _parse_sitemap(body: str) -> tuple[str, list[str]]:
    root = ET.fromstring(body)
    kind = "index" if _local(root.tag) == "sitemapindex" else "urlset"
    locs = []
    for node in root.iter():
        if _local(node.tag) == "loc" and node.text and node.text.strip():
            locs.append(node.text.strip())
    return kind, locs


_MAX_PAGES = 10_000

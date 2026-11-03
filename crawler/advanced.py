"""Единая точка входа: обход, sitemap, статистика и отчёты (день 7)."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

from crawler.async_crawler import AsyncCrawler
from crawler.config import config_to_kwargs, load_config
from crawler.logconfig import setup_logging
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage import CSVStorage, JSONStorage, SQLiteStorage

logger = logging.getLogger(__name__)

_STATUS_IN_REASON = re.compile(r"HTTP (\d{3})")


class AdvancedCrawler:
    """Краулер со всеми частями предыдущих дней, конфигом и отчётом.

    Пример::

        crawler = AdvancedCrawler.from_config("config.yaml")
        await crawler.crawl()
        print(crawler.get_stats())
        crawler.export_to_html_report("report.html")
        await crawler.close()
    """

    def __init__(
        self,
        start_urls: list[str],
        *,
        max_pages: int = 100,
        max_depth: int = 1,
        max_concurrent: int = 10,
        requests_per_second: float | None = None,
        respect_robots: bool = False,
        same_domain_only: bool = True,
        use_sitemap: bool = False,
        sitemaps: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
        user_agent: str | None = None,
        output: str | None = None,
        storage_format: str | None = None,
        batch_size: int = 20,
        log_file: str | None = None,
        log_level: str = "INFO",
        show_progress: bool = True,
        min_delay: float = 0.0,
    ) -> None:
        if not start_urls:
            raise ValueError("Нужен хотя бы один стартовый URL")
        self.start_urls = list(start_urls)
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.same_domain_only = same_domain_only
        self.use_sitemap = use_sitemap
        self.sitemaps = list(sitemaps or [])
        self.exclude_patterns = list(exclude_patterns or [])
        self.include_patterns = list(include_patterns or [])
        self.show_progress = show_progress
        self.output = output
        self.stats = CrawlerStats()
        self.pages: list[dict] = []
        self._progress_open = False
        if log_file or log_level:
            setup_logging(level=log_level, log_file=log_file)
        storage = _build_storage(output, storage_format, batch_size) if output else None
        self.crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
            requests_per_second=requests_per_second,
            respect_robots=respect_robots,
            min_delay=min_delay,
            user_agent=user_agent or "AdvancedCrawler/1.0",
            storage=storage,
            backoff_base=0,
        )
        self.crawler.on_progress = self._on_progress if show_progress else None
        self.sitemap = SitemapParser(self._download)

    @classmethod
    def from_config(cls, path: str | Path) -> AdvancedCrawler:
        """Собирает краулер из YAML или JSON."""
        return cls.from_dict(load_config(path))

    @classmethod
    def from_dict(cls, data: dict) -> AdvancedCrawler:
        return cls(**config_to_kwargs(data))

    async def crawl(self, start_urls: list[str] | None = None) -> dict[str, dict]:
        """Обходит стартовые адреса и, если включено, адреса из sitemap."""
        urls = list(start_urls or self.start_urls)
        self.stats = CrawlerStats()
        self.pages = []
        if self.use_sitemap:
            urls.extend(await self._urls_from_sitemaps(urls))
        try:
            results = await self.crawler.crawl(
                urls,
                self.max_pages,
                same_domain_only=self.same_domain_only,
                exclude_patterns=self.exclude_patterns,
                include_patterns=self.include_patterns,
            )
        finally:
            self._end_progress()
        self._fill_stats(results)
        self.stats.finish()
        return results

    def get_stats(self) -> dict:
        """Сводка последнего обхода."""
        return self.stats.as_dict()

    def export_to_json(self, filename: str) -> None:
        """Сохраняет сводку в JSON."""
        self.stats.export_to_json(filename)

    def export_to_html_report(self, filename: str) -> None:
        """Сохраняет HTML-отчёт со сводкой и полосками кодов ответа."""
        self.stats.export_to_html_report(filename, self.pages)

    async def close(self) -> None:
        """Закрывает HTTP-сессию и хранилище страниц."""
        self._end_progress()
        await self.crawler.close()

    async def _urls_from_sitemaps(self, urls: list[str]) -> list[str]:
        targets = list(self.sitemaps)
        if not targets:
            origins: set[str] = set()
            for url in urls:
                parts = urlsplit(url)
                if not parts.scheme or not parts.netloc:
                    continue
                origins.add(f"{parts.scheme}://{parts.netloc}")
            targets = [f"{origin}/sitemap.xml" for origin in origins]
        found: list[str] = []
        for sitemap_url in targets:
            found.extend(await self.sitemap.fetch_sitemap(sitemap_url))
        logger.info("Из sitemap взято %d адресов", len(found))
        return found

    async def _download(self, url: str) -> tuple[int, str]:
        parts = urlsplit(url)
        domain = (parts.hostname or parts.netloc or "*").lower()
        await self.crawler._rate_limiter.acquire(domain)
        session = await self.crawler._ensure_session()
        try:
            async with self.crawler._limits.limit(url):
                async with session.get(url) as response:
                    body = await response.text(errors="replace")
                    return response.status, body
        except Exception as exc:
            logger.warning("Ошибка загрузки %s: %s", url, type(exc).__name__)
            return 0, ""

    def _fill_stats(self, results: dict[str, dict]) -> None:
        self.stats.robots_blocked = self.crawler.robots_blocked
        for url, page in results.items():
            status = int(page.get("status_code") or 200)
            self.stats.record_success(url, status)
            self.pages.append(
                {
                    "url": url,
                    "title": page.get("title", ""),
                    "status_code": status,
                }
            )
        for url, reason in self.crawler.failed_urls.items():
            self.stats.record_failure(url, _status_from_reason(str(reason)))

    def _on_progress(self, snapshot: dict) -> None:
        done = snapshot["processed"] + snapshot["failed"]
        limit = snapshot["max_pages"] or 0
        speed = snapshot["pages_per_second"]
        active = snapshot["in_progress"]
        if limit:
            ratio = min(done / limit, 1.0)
            filled = int(ratio * 20)
            eta = (max(limit - done, 0) / speed) if speed > 0 else 0.0
            percent = f"{ratio * 100:5.1f}%"
            eta_text = f"{eta:.0f} с"
        else:
            filled = 0
            percent = "  ?  "
            eta_text = "—"
        bar = "█" * filled + "░" * (20 - filled)
        sys.stderr.write(
            f"\r[{bar}] {percent} {speed:.2f} стр/с ETA {eta_text} активных {active}"
        )
        sys.stderr.flush()
        self._progress_open = True

    def _end_progress(self) -> None:
        if self._progress_open:
            sys.stderr.write("\n")
            sys.stderr.flush()
            self._progress_open = False


def _build_storage(path: str, storage_format: str | None, batch_size: int):
    suffix = Path(path).suffix.lower()
    kind = (storage_format or "").lower()
    if kind == "csv" or (not kind and suffix == ".csv"):
        return CSVStorage(path, overwrite=True, batch_size=batch_size)
    if kind in {"sqlite", "db"} or (not kind and suffix in {".db", ".sqlite"}):
        return SQLiteStorage(path, overwrite=True, batch_size=batch_size)
    return JSONStorage(path, indent=2, overwrite=True, batch_size=batch_size)


def _status_from_reason(reason: str) -> int:
    match = _STATUS_IN_REASON.search(reason)
    if not match:
        return 0
    return int(match.group(1))

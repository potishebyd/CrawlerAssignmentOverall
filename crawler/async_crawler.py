"""Асинхронный HTTP-клиент, очередь обхода и разбор страниц."""

from __future__ import annotations

import asyncio
import logging
import re
import ssl
import time
from urllib.parse import urlsplit

import aiohttp
import certifi

from crawler.html_parser import HTMLParser
from crawler.limits import SemaphoreManager
from crawler.queue import CrawlerQueue, normalize_url

logger = logging.getLogger(__name__)

_USER_AGENT = "AsyncCrawler/1.0 (crawler-assignment)"


class AsyncCrawler:
    """Асинхронный HTTP-клиент с ограничением числа одновременных запросов.

    Сетевые ошибки, таймауты и ответы с кодом 4xx/5xx не прерывают работу:
    они пишутся в лог, а вместо тела страницы возвращается пустая строка.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        total_timeout: float | None = None,
        same_host_only: bool = False,
        max_depth: int = 1,
        max_per_domain: int | None = None,
    ) -> None:
        """Создаёт краулер.

        Сессия открывается при первом запросе и закрывается методом ``close``.

        :param max_concurrent: сколько запросов выполнять одновременно.
            Тем же числом ограничен пул соединений.
        :param connect_timeout: таймаут установки соединения, в секундах.
        :param read_timeout: таймаут чтения данных от сервера, в секундах.
        :param total_timeout: предел на весь запрос. ``None`` снимает общий предел,
            остаются только таймауты соединения и чтения.

        HTTPS проверяется по списку центров сертификации из пакета ``certifi``.

        :param same_host_only: при разборе HTML оставлять только ссылки
            на хост загруженной страницы.
        :param max_depth: насколько глубоко ``crawl`` уходит по ссылкам.
            Стартовые страницы имеют глубину 0.
        :param max_per_domain: сколько запросов к одному домену выполнять
            одновременно. По умолчанию совпадает с ``max_concurrent``.
        """
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть >= 1")
        if connect_timeout <= 0 or read_timeout <= 0:
            raise ValueError("таймауты connect и read должны быть положительными")
        if total_timeout is not None and total_timeout <= 0:
            raise ValueError("total_timeout должен быть положительным")
        if max_depth < 0:
            raise ValueError("max_depth должен быть >= 0")
        per_domain = max_concurrent if max_per_domain is None else max_per_domain
        if per_domain < 1:
            raise ValueError("max_per_domain должен быть >= 1")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._total_timeout = total_timeout
        self._limits = SemaphoreManager(max_concurrent, per_domain)
        self._session_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._parser = HTMLParser(same_host_only=same_host_only)
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self._queue: CrawlerQueue | None = None
        self._allowed_hosts: set[str] = set()
        self._same_domain_only = False
        self._exclude_patterns: list[re.Pattern[str]] = []
        self._include_patterns: list[re.Pattern[str]] = []
        self._crawl_started = 0.0

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(
                    total=self._total_timeout,
                    connect=self._connect_timeout,
                    sock_read=self._read_timeout,
                )
                connector = aiohttp.TCPConnector(
                    limit=self.max_concurrent,
                    limit_per_host=self.max_concurrent,
                    ssl=ssl.create_default_context(cafile=certifi.where()),
                )
                self._session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers={"User-Agent": _USER_AGENT},
                )
            return self._session

    async def fetch_url(self, url: str) -> str:
        """Загружает одну страницу и возвращает её текст.

        При ошибке возвращает пустую строку и пишет причину в лог.
        """
        session = await self._ensure_session()
        async with self._limits.limit(url):
            return await self._fetch(session, url)

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        logger.info("Начало загрузки URL: %s", url)
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text(errors="replace")
        except (TimeoutError, asyncio.TimeoutError) as exc:
            logger.warning("Таймаут при загрузке URL %s: %s", url, type(exc).__name__)
            return ""
        except aiohttp.ClientResponseError as exc:
            logger.warning(
                "HTTP-ошибка при загрузке URL %s: %s (status=%s)",
                url,
                type(exc).__name__,
                exc.status,
            )
            return ""
        except aiohttp.ClientError as exc:
            logger.warning(
                "Сетевая ошибка при загрузке URL %s: %s",
                url,
                type(exc).__name__,
            )
            return ""

        logger.info("Успешное завершение: %s (%d символов)", url, len(text))
        return text

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Параллельно загружает страницы и возвращает словарь URL → текст.

        Неудачные адреса остаются в словаре со значением ``""``, остальные
        запросы при этом доводятся до конца.
        """
        pages = await asyncio.gather(*(self.fetch_url(url) for url in urls))
        return dict(zip(urls, pages, strict=True))

    async def fetch_and_parse(self, url: str) -> dict:
        """Загружает страницу и возвращает разобранные данные.

        В словаре есть исходный адрес, заголовок, текст, абсолютные ссылки
        и метаданные. Изображения, заголовки, таблицы и списки добавляются
        рядом. Ошибка загрузки или разбора оставляет поля пустыми.
        """
        html = await self.fetch_url(url)
        parsed = await self._parser.parse_html(html, url)
        return {
            "url": url,
            "title": parsed["title"],
            "text": parsed["text"],
            "links": parsed["links"],
            "metadata": parsed["metadata"],
            "images": parsed["images"],
            "headings": parsed["headings"],
            "tables": parsed["tables"],
            "lists": parsed["lists"],
        }

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        *,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        """Обходит сайты от стартовых адресов и возвращает разобранные страницы.

        Найденные ссылки ставятся в очередь, уже виденные адреса не
        запрашиваются снова. Глубина ограничена ``max_depth``, число
        страниц — ``max_pages``.
        """
        if max_pages < 1:
            raise ValueError("max_pages должен быть >= 1")

        queue = CrawlerQueue(max_pages=max_pages)
        self._queue = queue
        self.visited_urls = queue.seen
        self.failed_urls = queue.failed
        self.processed_urls = {}
        self._same_domain_only = same_domain_only
        self._exclude_patterns = _compile_patterns(exclude_patterns)
        self._include_patterns = _compile_patterns(include_patterns)
        self._allowed_hosts = set()
        self._crawl_started = time.perf_counter()

        for url in start_urls:
            normalized = normalize_url(url)
            if not normalized:
                logger.warning("Стартовый URL пропущен: %s", url)
                continue
            if queue.add_url(normalized, priority=0, depth=0):
                host = urlsplit(normalized).hostname
                if host:
                    self._allowed_hosts.add(host.lower())

        queue.mark_started()
        workers = [asyncio.create_task(self._crawl_worker()) for _ in range(self.max_concurrent)]
        try:
            await asyncio.gather(*workers)
        finally:
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        logger.info(
            "Обход завершён: обработано %d, ошибок %d, в множестве visited %d",
            len(self.processed_urls),
            len(self.failed_urls),
            len(self.visited_urls),
        )
        return self.processed_urls

    async def _crawl_worker(self) -> None:
        assert self._queue is not None
        while True:
            url = await self._queue.get_next()
            if url is None:
                return
            try:
                await self._process_page(url)
            except Exception as exc:
                logger.warning("Сбой обработки %s: %s", url, type(exc).__name__)
                self._queue.mark_failed(url, type(exc).__name__)
                self._report_progress()

    async def _process_page(self, url: str) -> None:
        assert self._queue is not None
        depth = self._queue.depth_of(url)
        html = await self.fetch_url(url)
        if not html.strip():
            self._queue.mark_failed(url, "не удалось загрузить страницу")
            self._report_progress()
            return

        parsed = await self._parser.parse_html(html, url)
        parsed["depth"] = depth
        self._enqueue_links(parsed.get("links", []), depth)
        self.processed_urls[url] = parsed
        self._queue.mark_processed(url)
        self._report_progress()

    def _enqueue_links(self, links: list[str], depth: int) -> None:
        assert self._queue is not None
        next_depth = depth + 1
        if next_depth > self.max_depth:
            return
        for link in links:
            normalized = normalize_url(link)
            if not normalized or not self._link_allowed(normalized):
                continue
            self._queue.add_url(normalized, priority=-next_depth, depth=next_depth)

    def _link_allowed(self, url: str) -> bool:
        if self._same_domain_only:
            host = urlsplit(url).hostname
            if host is None or host.lower() not in self._allowed_hosts:
                return False
        if any(pattern.search(url) for pattern in self._exclude_patterns):
            return False
        if self._include_patterns and not any(
            pattern.search(url) for pattern in self._include_patterns
        ):
            return False
        return True

    def _report_progress(self) -> None:
        if self._queue is None:
            return
        stats = self._queue.get_stats()
        elapsed = max(time.perf_counter() - self._crawl_started, 1e-6)
        speed = stats["processed"] / elapsed
        logger.info(
            "Прогресс: обработано %d, в очереди %d, ошибок %d, %.2f стр/с",
            stats["processed"],
            stats["pending"],
            stats["failed"],
            speed,
        )

    async def close(self) -> None:
        """Закрывает сессию и возвращает соединения в операционную систему."""
        async with self._session_lock:
            session = self._session
            self._session = None
        if session is not None and not session.closed:
            await session.close()

    async def __aenter__(self) -> AsyncCrawler:
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


def _compile_patterns(patterns: list[str] | None) -> list[re.Pattern[str]]:
    compiled: list[re.Pattern[str]] = []
    for pattern in patterns or []:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            logger.warning("Пропущен некорректный шаблон %r: %s", pattern, exc)
    return compiled

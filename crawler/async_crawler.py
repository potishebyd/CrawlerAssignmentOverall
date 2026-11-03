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

from crawler.errors import CrawlerError, NetworkError, ParseError, TransientError
from crawler.html_parser import HTMLParser
from crawler.limits import SemaphoreManager
from crawler.queue import CrawlerQueue, normalize_url
from crawler.rate_limiter import RateLimiter
from crawler.retry import CircuitBreaker, RetryStrategy, classify_http_status, current_attempt
from crawler.robots import RobotsParser
from crawler.storage import DataStorage, build_record

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
        requests_per_second: float | None = None,
        per_domain_rate_limit: bool = True,
        respect_robots: bool = False,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str | None = None,
        user_agents: list[str] | None = None,
        backoff_base: float = 0.5,
        backoff_max: float = 8.0,
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        storage: DataStorage | None = None,
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
        :param requests_per_second: верхняя скорость запросов. ``None`` снимает
            этот предел, остаются ``min_delay`` и задержки из robots.txt.
        :param per_domain_rate_limit: отдельный ритм для каждого домена.
        :param respect_robots: проверять robots.txt перед запросом страницы.
        :param min_delay: минимальная пауза между запросами к одному ключу, секунды.
        :param jitter: случайная добавка к паузе от 0 до этого значения, секунды.
        :param user_agent: значение заголовка User-Agent.
        :param user_agents: список для поочерёдной подмены User-Agent.
        :param backoff_base: первая пауза после окончательной ошибки домена,
            дальше она удваивается. Повторы одного адреса ждут отдельно,
            по правилам ``retry_strategy``.
        :param backoff_max: потолок паузы после ошибок, секунды.
        :param retry_strategy: правила повторов. По умолчанию повторов нет,
            чтобы один сбой не запускал серию запросов. Класс ``RetryStrategy``
            сам по себе повторяет временные ошибки до трёх раз.
        :param circuit_breaker: закрывает домен после серии временных ошибок.
            По умолчанию порог пять ошибок и пауза 30 секунд.
        :param storage: куда складывать разобранные страницы. ``None`` ничего
            не пишет. Ошибка записи логируется и не останавливает обход.
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
        if requests_per_second is not None and requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть > 0")
        if min_delay < 0 or jitter < 0:
            raise ValueError("min_delay и jitter не могут быть отрицательными")
        if backoff_base < 0 or backoff_max < 0:
            raise ValueError("параметры backoff не могут быть отрицательными")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
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
        self._user_agents = list(user_agents or [user_agent or _USER_AGENT])
        self._ua_index = 0
        self._domain_failures: dict[str, int] = {}
        self._block_reasons: dict[str, str] = {}
        self.robots_blocked = 0
        self.robots_blocked_urls: list[str] = []
        rate = float("inf") if requests_per_second is None else requests_per_second
        self._rate_limiter = RateLimiter(
            rate,
            per_domain_rate_limit,
            min_delay=min_delay,
            jitter=jitter,
        )
        self._robots = RobotsParser(self._download_robots)
        self.retry_strategy = retry_strategy or RetryStrategy(max_retries=0)
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.storage = storage
        self._page_meta: dict[str, dict] = {}
        self.on_progress = None
        self._max_pages = 0

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
                    headers={"User-Agent": self._user_agents[0]},
                )
            return self._session

    async def fetch_url(self, url: str) -> str:
        """Загружает одну страницу и возвращает её текст.

        Перед запросом проверяет robots.txt, если это включено. Временные
        и сетевые ошибки повторяет стратегия ``retry_strategy``. При окончательной
        ошибке или запрете возвращает пустую строку и пишет причину в лог.
        """
        user_agent = self._next_user_agent()
        crawl_delay = 0.0
        if self.respect_robots and not _is_robots_txt(url):
            rules = await self._robots.fetch_robots(url)
            if not rules.get("cached"):
                logger.info(
                    "Загружен robots.txt %s, Crawl-delay %.2f с для %s",
                    rules["url"],
                    self._robots.get_crawl_delay(user_agent, url),
                    user_agent,
                )
            if not self._robots.can_fetch(url, user_agent):
                self.robots_blocked += 1
                self.robots_blocked_urls.append(url)
                self._block_reasons[url] = "запрещено robots.txt"
                logger.warning("URL заблокирован robots.txt: %s (User-Agent: %s)", url, user_agent)
                return ""
            crawl_delay = self._robots.get_crawl_delay(user_agent, url)

        try:
            return await self.retry_strategy.execute_with_retry(
                self._fetch_attempt,
                url,
                user_agent,
                crawl_delay,
            )
        except CrawlerError as exc:
            self._note_failure(url)
            self._block_reasons[url] = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Итог загрузки %s: неудача, %s, попыток %d",
                url,
                type(exc).__name__,
                exc.attempts,
            )
            return ""

    async def _fetch_attempt(self, url: str, user_agent: str, crawl_delay: float) -> str:
        """Одна попытка загрузки. Ошибки поднимает, а не прячет."""
        domain = _domain_of(url)
        service = _service_of(url)
        await self.circuit_breaker.check(service, url)
        extra = max(crawl_delay, self._backoff_delay(domain))
        await self._rate_limiter.acquire(domain, extra_delay=extra)
        session = await self._ensure_session()
        timeout = self._timeout_for(current_attempt())
        try:
            async with self._limits.limit(url):
                logger.info("Начало загрузки URL: %s (попытка %d)", url, current_attempt())
                async with session.get(
                    url,
                    headers={"User-Agent": user_agent},
                    timeout=timeout,
                ) as response:
                    if response.status >= 400:
                        await response.read()
                        raise classify_http_status(response.status, url)
                    text = await response.text(errors="replace")
                    self._page_meta[url] = {
                        "status_code": response.status,
                        "content_type": response.content_type or "",
                    }
        except CrawlerError as exc:
            await self._observe_failure(service, exc)
            raise
        except (TimeoutError, asyncio.TimeoutError) as exc:
            error = TransientError("таймаут", url=url)
            await self._observe_failure(service, error)
            raise error from exc
        except aiohttp.ClientResponseError as exc:
            error = classify_http_status(exc.status or 0, url)
            await self._observe_failure(service, error)
            raise error from exc
        except aiohttp.ClientError as exc:
            error = NetworkError(type(exc).__name__, url=url)
            await self._observe_failure(service, error)
            raise error from exc

        self._note_success(url)
        await self.circuit_breaker.record_success(service)
        logger.info("Успешное завершение: %s (%d символов)", url, len(text))
        return text

    def _timeout_for(self, attempt: int) -> aiohttp.ClientTimeout:
        scale = self.retry_strategy.timeout_scale(attempt)
        total = None if self._total_timeout is None else self._total_timeout * scale
        return aiohttp.ClientTimeout(
            total=total,
            connect=self._connect_timeout * scale,
            sock_read=self._read_timeout * scale,
        )

    async def _observe_failure(self, service: str, exc: CrawlerError) -> None:
        if isinstance(exc, (TransientError, NetworkError)):
            await self.circuit_breaker.record_failure(service)

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
        self.robots_blocked = 0
        self.robots_blocked_urls = []
        self._block_reasons = {}
        self._page_meta = {}
        self._max_pages = max_pages
        self._rate_limiter.reset_stats()
        self.retry_strategy.reset_stats()

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

        if self.storage is not None:
            try:
                await self.storage.flush()
            except Exception as exc:
                logger.warning("Сброс хранилища не удался: %s", type(exc).__name__)
            stats = self.storage.stats()
            logger.info(
                "Хранилище: сохранено %d, ошибок записи %d, пачек %d",
                stats["saved"],
                stats["errors"],
                stats["batches"],
            )

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
            reason = self._block_reasons.pop(url, "не удалось загрузить страницу")
            self._queue.mark_failed(url, reason)
            self._report_progress()
            return

        try:
            parsed = await self._parser.parse_html(html, url)
        except Exception as exc:
            error = ParseError(type(exc).__name__, url=url)
            self.retry_strategy.record_terminal(error)
            reason = f"ParseError: {error}"
            self._queue.mark_failed(url, reason)
            self._report_progress()
            return
        parsed["depth"] = depth
        self._enqueue_links(parsed.get("links", []), depth)
        self.processed_urls[url] = parsed
        await self._store_page(parsed)
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
        polite = self.politeness_stats()
        elapsed = max(time.perf_counter() - self._crawl_started, 1e-6)
        speed = stats["processed"] / elapsed
        logger.info(
            "Прогресс: обработано %d, в очереди %d, ошибок %d, %.2f стр/с, "
            "запросы %.2f req/s, средняя задержка %.3f с, блокировок robots %d",
            stats["processed"],
            stats["pending"],
            stats["failed"],
            speed,
            polite["requests_per_second"],
            polite["average_delay"],
            polite["blocked_by_robots"],
        )
        if self.on_progress is None:
            return
        try:
            limits = self._limits.get_stats()
            self.on_progress(
                {
                    "processed": stats["processed"],
                    "failed": stats["failed"],
                    "pending": stats["pending"],
                    "in_progress": stats["in_progress"],
                    "active_requests": limits["active"],
                    "max_pages": self._max_pages,
                    "elapsed": elapsed,
                    "pages_per_second": speed,
                }
            )
        except Exception as exc:
            logger.warning("Сбой вывода прогресса: %s", type(exc).__name__)

    async def _store_page(self, parsed: dict) -> None:
        meta = self._page_meta.pop(parsed.get("url", ""), {})
        if meta:
            parsed["status_code"] = meta.get("status_code", 0)
            parsed["content_type"] = meta.get("content_type", "")
        if self.storage is None:
            return
        record = build_record(
            parsed,
            status_code=int(parsed.get("status_code") or 0),
            content_type=str(parsed.get("content_type") or ""),
        )
        try:
            await self.storage.save(record)
        except Exception as exc:
            logger.warning(
                "Сохранение %s не остановило обход: %s",
                record["url"],
                type(exc).__name__,
            )

    async def close(self) -> None:
        """Закрывает сессию и сбрасывает хранилище, если оно подключено."""
        async with self._session_lock:
            session = self._session
            self._session = None
        if session is not None and not session.closed:
            await session.close()
        if self.storage is not None:
            try:
                await self.storage.close()
            except Exception as exc:
                logger.warning("Закрытие хранилища не удалось: %s", type(exc).__name__)

    async def __aenter__(self) -> AsyncCrawler:
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


    def error_stats(self) -> dict:
        """Ошибки по классам, удачные повторы, средняя пауза и постоянные адреса."""
        return self.retry_strategy.stats()

    def politeness_stats(self) -> dict:
        """Скорость запросов, средняя пауза и число адресов, закрытых robots.txt."""
        stats = self._rate_limiter.get_stats()
        return {
            "requests": stats["requests"],
            "requests_per_second": stats["requests_per_second"],
            "average_delay": stats["average_delay"],
            "min_interval": stats["min_interval"],
            "blocked_by_robots": self.robots_blocked,
            "blocked_urls": list(self.robots_blocked_urls),
        }

    def _next_user_agent(self) -> str:
        agent = self._user_agents[self._ua_index % len(self._user_agents)]
        self._ua_index += 1
        return agent

    def _backoff_delay(self, domain: str) -> float:
        failures = self._domain_failures.get(domain, 0)
        if failures <= 0 or self.backoff_base <= 0:
            return 0.0
        return min(self.backoff_max, self.backoff_base * (2 ** (failures - 1)))

    def _note_success(self, url: str) -> None:
        self._domain_failures[_domain_of(url)] = 0

    def _note_failure(self, url: str) -> None:
        domain = _domain_of(url)
        self._domain_failures[domain] = self._domain_failures.get(domain, 0) + 1

    async def _download_robots(self, url: str) -> tuple[int, str]:
        domain = _domain_of(url)
        await self._rate_limiter.acquire(domain, extra_delay=self._backoff_delay(domain))
        session = await self._ensure_session()
        try:
            async with self._limits.limit(url):
                async with session.get(
                    url,
                    headers={"User-Agent": self._user_agents[0]},
                ) as response:
                    return response.status, await response.text(errors="replace")
        except (TimeoutError, asyncio.TimeoutError, aiohttp.ClientError) as exc:
            logger.warning("Ошибка загрузки robots.txt %s: %s", url, type(exc).__name__)
            return 0, ""


def _domain_of(url: str) -> str:
    parts = urlsplit(url)
    return (parts.hostname or parts.netloc or "*").lower()


def _service_of(url: str) -> str:
    parts = urlsplit(url)
    return (parts.netloc or "*").lower()


def _is_robots_txt(url: str) -> bool:
    return urlsplit(url).path.rstrip("/") == "/robots.txt"


def _compile_patterns(patterns: list[str] | None) -> list[re.Pattern[str]]:
    compiled: list[re.Pattern[str]] = []
    for pattern in patterns or []:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            logger.warning("Пропущен некорректный шаблон %r: %s", pattern, exc)
    return compiled

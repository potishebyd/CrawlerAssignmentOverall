"""Базовый асинхронный HTTP-клиент (день 1)."""

from __future__ import annotations

import asyncio
import logging
import ssl

import aiohttp
import certifi

from crawler.html_parser import HTMLParser

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
        """
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть >= 1")
        if connect_timeout <= 0 or read_timeout <= 0:
            raise ValueError("таймауты connect и read должны быть положительными")
        if total_timeout is not None and total_timeout <= 0:
            raise ValueError("total_timeout должен быть положительным")

        self.max_concurrent = max_concurrent
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._total_timeout = total_timeout
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._parser = HTMLParser(same_host_only=same_host_only)

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
        async with self._semaphore:
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

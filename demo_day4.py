"""День 4: скорость запросов, паузы и robots.txt.

Запуск из корня проекта:

    python demo_day4.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

import aiohttp
from aiohttp import web

from crawler import AsyncCrawler, RateLimiter, RobotsParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

ROBOTS = """User-agent: *
Disallow: /private
Crawl-delay: 0.2

User-agent: MyBot
Disallow: /private
Disallow: /bot-secret
Crawl-delay: 0.4

Sitemap: https://example.com/sitemap.xml
"""

PAGES = {
    "/": ["/a", "/b", "/private", "/bot-secret"],
    "/a": [],
    "/b": [],
    "/private": [],
    "/bot-secret": [],
    "/after": [],
}


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _mark(condition: bool) -> str:
    return "OK" if condition else "НЕТ"


def _html(title: str, links: list[str]) -> str:
    anchors = "\n".join(f'<a href="{href}">{href}</a>' for href in links)
    return (
        "<!DOCTYPE html><html><head>"
        f"<title>{title}</title></head><body><h1>{title}</h1>{anchors}</body></html>"
    )


async def _burst(limiter: RateLimiter, domain: str, count: int) -> list[float]:
    waits: list[float] = []
    for _ in range(count):
        waits.append(await limiter.acquire(domain))
    return waits


async def demo_one_domain() -> None:
    _section("Скорость для одного домена: 5 запросов в секунду")
    limiter = RateLimiter(5, per_domain=True)
    started = time.perf_counter()
    waits = await _burst(limiter, "example.com", 4)
    elapsed = time.perf_counter() - started
    print(f"Паузы: {[round(item, 3) for item in waits]}")
    print(f"Время четырёх запросов: {elapsed:.2f} с")
    print(f"Первый запрос без ожидания: {_mark(waits[0] < 0.05)}")
    print(f"Дальше пауза около 0.2 с: {_mark(all(item >= 0.18 for item in waits[1:]))}")
    print(f"Четыре запроса уложились в секунду: {_mark(elapsed < 1.0)}")


async def demo_many_domains() -> None:
    _section("Разные домены не делят один лимит")
    separate = RateLimiter(5, per_domain=True)
    started = time.perf_counter()
    await asyncio.gather(_burst(separate, "a.example", 3), _burst(separate, "b.example", 3))
    separate_elapsed = time.perf_counter() - started

    shared = RateLimiter(5, per_domain=False)
    started = time.perf_counter()
    await asyncio.gather(_burst(shared, "a.example", 3), _burst(shared, "b.example", 3))
    shared_elapsed = time.perf_counter() - started

    print(f"Два домена по отдельности: {separate_elapsed:.2f} с")
    print(f"Два домена в общем лимите: {shared_elapsed:.2f} с")
    print(f"Отдельные лимиты идут параллельно: {_mark(separate_elapsed < 0.7)}")
    print(f"Общий лимит складывает паузы: {_mark(shared_elapsed > 0.85)}")


async def demo_jitter() -> None:
    _section("Случайная добавка к паузе")
    limiter = RateLimiter(5, per_domain=True, jitter=0.15)
    waits = await _burst(limiter, "jitter.example", 4)
    print(f"Паузы: {[round(item, 3) for item in waits]}")
    print(
        "Пауза между запросами от 0.2 до 0.35 с: "
        f"{_mark(all(0.18 <= item <= 0.42 for item in waits[1:]))}"
    )


async def _start_server() -> tuple[web.AppRunner, str]:
    flaky_hits = {"count": 0}

    async def robots(_request: web.Request) -> web.Response:
        return web.Response(text=ROBOTS, content_type="text/plain")

    async def page(request: web.Request) -> web.Response:
        path = request.path
        title = path.strip("/") or "home"
        return web.Response(text=_html(title, PAGES.get(path, [])), content_type="text/html")

    async def flaky(_request: web.Request) -> web.Response:
        flaky_hits["count"] += 1
        if flaky_hits["count"] == 1:
            return web.Response(status=500, text="fail")
        return web.Response(text=_html("flaky", []), content_type="text/html")

    async def echo(request: web.Request) -> web.Response:
        agent = request.headers.get("User-Agent", "")
        return web.Response(text=agent)

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/flaky", flaky)
    app.router.add_get("/echo/{name}", echo)
    app.router.add_get("/", page)
    app.router.add_get("/{name}", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}"


async def demo_robots_parser(origin: str) -> None:
    _section("Разбор robots.txt")

    async def download(url: str) -> tuple[int, str]:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return response.status, await response.text()

    parser = RobotsParser(download)
    rules = await parser.fetch_robots(origin + "/")
    again = await parser.fetch_robots(origin + "/a")
    private = origin + "/private"
    secret = origin + "/bot-secret"
    allowed = origin + "/a"
    print(f"Групп правил: {len(rules['groups'])}, карта сайта: {rules['sitemaps']}")
    print(f"Повтор взят из кэша: {_mark(again['cached'] is True)}")
    print(f"MyBot не ходит в /private: {_mark(not parser.can_fetch(private, 'MyBot/1.0'))}")
    print(f"MyBot не ходит в /bot-secret: {_mark(not parser.can_fetch(secret, 'MyBot/1.0'))}")
    print(f"MyBot ходит в /a: {_mark(parser.can_fetch(allowed, 'MyBot/1.0'))}")
    print(f"Чужой бот ходит в /bot-secret: {_mark(parser.can_fetch(secret, 'OtherBot/1.0'))}")
    print(f"Чужой бот не ходит в /private: {_mark(not parser.can_fetch(private, 'OtherBot/1.0'))}")
    print(
        "Crawl-delay MyBot 0.4 с, для остальных 0.2 с: "
        f"{_mark(parser.get_crawl_delay('MyBot/1.0', allowed) == 0.4 and parser.get_crawl_delay('OtherBot/1.0', allowed) == 0.2)}"
    )


async def demo_crawl(origin: str) -> None:
    _section("Обход с robots.txt и двумя запросами в секунду")
    crawler = AsyncCrawler(
        max_concurrent=2,
        max_depth=1,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.3,
        user_agent="MyBot/1.0",
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        started = time.perf_counter()
        results = await crawler.crawl(
            [f"{origin}/"],
            max_pages=10,
            same_domain_only=True,
        )
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()

    prefix = origin.rstrip("/")
    processed = {url.removeprefix(prefix) or "/" for url in results}
    failed = {url.removeprefix(prefix) or "/": reason for url, reason in crawler.failed_urls.items()}
    stats = crawler.politeness_stats()
    print(f"Обработано: {sorted(processed)}")
    print(f"Ошибки: {failed}")
    print(
        "Скорость запросов: "
        f"{stats['requests_per_second']:.2f} req/s, "
        f"средняя задержка {stats['average_delay']:.3f} с, "
        f"блокировок {stats['blocked_by_robots']}"
    )
    print(f"Время обхода: {elapsed:.2f} с")
    print(f"Открыты только разрешённые страницы: {_mark(processed == {'/', '/a', '/b'})}")
    print(
        "Запрещённые адреса заблокированы: "
        f"{_mark(failed.get('/private') == 'запрещено robots.txt' and failed.get('/bot-secret') == 'запрещено robots.txt')}"
    )
    print(f"Между запросами не меньше 0.45 с: {_mark(stats['min_interval'] >= 0.45)}")


async def demo_backoff(origin: str) -> None:
    _section("Пауза растёт после ошибки")
    crawler = AsyncCrawler(
        max_concurrent=1,
        max_depth=0,
        requests_per_second=100,
        respect_robots=False,
        backoff_base=0.4,
        backoff_max=2,
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        started = time.perf_counter()
        await crawler.crawl(
            [f"{origin}/flaky", f"{origin}/after"],
            max_pages=2,
        )
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()
    print(f"Два запроса заняли {elapsed:.2f} с")
    print(f"После ошибки следующая пауза не меньше 0.35 с: {_mark(elapsed >= 0.35)}")


async def demo_user_agents(origin: str) -> None:
    _section("Ротация User-Agent")
    crawler = AsyncCrawler(
        max_concurrent=1,
        respect_robots=False,
        user_agents=["BotA/1.0", "BotB/1.0"],
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        first = await crawler.fetch_url(f"{origin}/echo/1")
        second = await crawler.fetch_url(f"{origin}/echo/2")
    finally:
        await crawler.close()
    print(f"Первый запрос: {first}")
    print(f"Второй запрос: {second}")
    print(f"Агенты чередуются: {_mark(first == 'BotA/1.0' and second == 'BotB/1.0')}")


async def main() -> None:
    await demo_one_domain()
    await demo_many_domains()
    await demo_jitter()
    runner, origin = await _start_server()
    try:
        await demo_robots_parser(origin)
        await demo_crawl(origin)
        await demo_backoff(origin)
        await demo_user_agents(origin)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

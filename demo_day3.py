"""День 3: очередь, глубина обхода, фильтры и предел на домен.

Запуск из корня проекта:

    python demo_day3.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from aiohttp import web

from crawler import AsyncCrawler, CrawlerQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

OUTPUT_PATH = Path("output/day3_results.json")

PAGES = {
    "/": ["/a", "/b", "/skip/secret", "/files/doc.pdf", "https://example.com/out"],
    "/a": ["/a/deep", "/b"],
    "/b": ["/"],
    "/a/deep": ["/too/deep"],
    "/too/deep": [],
    "/skip/secret": [],
    "/files/doc.pdf": [],
    "/only/yes": ["/only/more", "/nope"],
    "/only/more": [],
    "/nope": [],
}


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _page(title: str, links: list[str]) -> str:
    anchors = "\n".join(f'<a href="{href}">{href}</a>' for href in links)
    return (
        "<!DOCTYPE html><html><head>"
        f"<title>{title}</title></head><body><h1>{title}</h1>{anchors}</body></html>"
    )


def _mark(condition: bool) -> str:
    return "OK" if condition else "НЕТ"


async def demo_priority() -> None:
    _section("Очередь с приоритетами")
    queue = CrawlerQueue()
    queue.add_url("https://a.test/first", priority=1)
    queue.add_url("https://a.test/second", priority=1)
    queue.add_url("https://a.test/urgent", priority=3)
    queue.mark_started()

    order: list[str] = []
    while True:
        url = await queue.get_next()
        if url is None:
            break
        order.append(url)
        if url.endswith("/second"):
            queue.mark_failed(url, "демонстрационная ошибка")
        else:
            queue.mark_processed(url)

    expected = [
        "https://a.test/urgent",
        "https://a.test/first",
        "https://a.test/second",
    ]
    print("Порядок выдачи:")
    for url in order:
        print(f"  {url}")
    print(f"Приоритет и FIFO: {_mark(order == expected)}")
    stats = queue.get_stats()
    print(
        "Статистика: "
        f"обработано {stats['processed']}, ошибок {stats['failed']}, "
        f"в очереди {stats['pending']}"
    )
    print(f"Ошибок ровно одна: {_mark(stats['failed'] == 1 and stats['pending'] == 0)}")


async def _start_server(delay: float) -> tuple[web.AppRunner, str, dict[str, int]]:
    state = {"in_flight": 0, "max_in_flight": 0}

    async def handle(request: web.Request) -> web.Response:
        state["in_flight"] += 1
        state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
        try:
            if delay:
                await asyncio.sleep(delay)
            path = request.path
            links = PAGES.get(path, [])
            title = path.strip("/") or "home"
            return web.Response(text=_page(title, links), content_type="text/html")
        finally:
            state["in_flight"] -= 1

    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/{tail:.*}", handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}", state


def _paths(origin: str, urls: set[str]) -> set[str]:
    prefix = origin.rstrip("/")
    return {url.removeprefix(prefix) or "/" for url in urls}


async def demo_filters(origin: str) -> dict[str, dict]:
    _section("Глубина, фильтры и дубликаты")
    crawler = AsyncCrawler(max_concurrent=4, max_depth=2, max_per_domain=4)
    try:
        results = await crawler.crawl(
            start_urls=[f"{origin}/"],
            max_pages=20,
            same_domain_only=True,
            exclude_patterns=[r"/skip", r"\.pdf$"],
        )
    finally:
        await crawler.close()

    visited = _paths(origin, crawler.visited_urls)
    processed = _paths(origin, set(results))
    print(f"Обработано: {len(results)} страниц")
    print(f"Посещены: {sorted(visited)}")
    print(f"Глубины: { {path: results[f'{origin}{path}'].get('depth') for path in sorted(processed)} }")
    print(f"Страницы до глубины 2: {_mark(visited == {'/', '/a', '/b', '/a/deep'})}")
    print(f"Глубина 3 не открыта: {_mark('/too/deep' not in visited)}")
    print(f"Фильтры отсеяли skip и pdf: {_mark('/skip/secret' not in visited and '/files/doc.pdf' not in visited)}")
    print(f"Чужой домен не посещён: {_mark(not any('example.com' in url for url in crawler.visited_urls))}")
    print(f"Дубликатов в visited нет: {_mark(len(crawler.visited_urls) == len(visited) == len(processed))}")
    return results


async def demo_include(origin: str) -> None:
    _section("Фильтр включения")
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
    try:
        results = await crawler.crawl(
            start_urls=[f"{origin}/only/yes"],
            max_pages=10,
            same_domain_only=True,
            include_patterns=[r"/only"],
        )
    finally:
        await crawler.close()

    visited = _paths(origin, crawler.visited_urls)
    print(f"Посещены: {sorted(visited)}")
    print(f"Только пути /only: {_mark(visited == {'/only/yes', '/only/more'})}")


async def demo_domain_limit(origin: str, state: dict[str, int]) -> None:
    _section("Не больше двух запросов к одному домену")
    state["max_in_flight"] = 0
    started = time.perf_counter()
    crawler = AsyncCrawler(
        max_concurrent=4,
        max_per_domain=2,
        max_depth=0,
        connect_timeout=2,
        read_timeout=2,
    )
    try:
        results = await crawler.crawl(
            start_urls=[f"{origin}/slow/{index}" for index in range(4)],
            max_pages=4,
        )
    finally:
        await crawler.close()
    elapsed = time.perf_counter() - started
    print(f"Обработано: {len(results)} страниц за {elapsed:.2f} с")
    print(f"Пик одновременных запросов на сервере: {state['max_in_flight']}")
    print(f"Пик не превысил max_per_domain=2: {_mark(state['max_in_flight'] == 2)}")
    print(f"Пик по данным семафора: {crawler._limits.get_stats()['peak_active']}")


async def demo_public() -> None:
    _section("Один публичный адрес, без перехода по ссылкам")
    crawler = AsyncCrawler(max_concurrent=2, max_depth=0, connect_timeout=10, read_timeout=20)
    try:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=1,
            same_domain_only=True,
        )
    finally:
        await crawler.close()
    print(f"Обработано: {len(results)} страниц")
    for url, page in results.items():
        print(f"  {url}  title={page['title']!r}  глубина={page['depth']}")
    if crawler.failed_urls:
        print(f"Ошибки: {crawler.failed_urls}")


def _save(results: dict[str, dict]) -> None:
    payload = {
        url: {
            "url": page["url"],
            "title": page["title"],
            "depth": page["depth"],
            "text": page["text"],
            "links": page["links"],
            "metadata": page["metadata"],
        }
        for url, page in results.items()
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nРезультаты записаны в {OUTPUT_PATH}")


async def main() -> None:
    await demo_priority()
    runner, origin, state = await _start_server(delay=0.25)
    try:
        results = await demo_filters(origin)
        await demo_include(origin)
        await demo_domain_limit(origin, state)
    finally:
        await runner.cleanup()
    _save(results)
    await demo_public()


if __name__ == "__main__":
    asyncio.run(main())

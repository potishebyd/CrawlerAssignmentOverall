"""День 1: параллельная загрузка, ошибки, таймаут и сравнение с последовательной.

Запуск из корня проекта:

    python demo_day1.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

from aiohttp import web

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

PUBLIC_URLS = [
    "https://example.com",
    "https://example.org",
    "https://httpbin.org/get",
    "https://httpbin.org/html",
    "https://httpbin.org/robots.txt",
    "https://httpbin.org/uuid",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/status/404",
]


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _print_report(results: dict[str, str], elapsed: float) -> None:
    print("\nСтатус запросов:")
    for url, body in results.items():
        if body:
            print(f"  OK    {url}  ({len(body)} символов)")
        else:
            print(f"  FAIL  {url}")
    loaded = sum(bool(body) for body in results.values())
    print(f"\nЗагружено {loaded} из {len(results)} страниц за {elapsed:.2f} с")


async def _fetch_sequential(crawler: AsyncCrawler, urls: list[str]) -> dict[str, str]:
    results: dict[str, str] = {}
    for url in urls:
        results[url] = await crawler.fetch_url(url)
    return results


async def _start_server() -> tuple[web.AppRunner, str, dict[str, int]]:
    state = {"in_flight": 0, "max_in_flight": 0}

    async def page(request: web.Request) -> web.Response:
        return web.Response(text=f"ok:{request.match_info['name']}")

    async def status(request: web.Request) -> web.Response:
        code = int(request.match_info["code"])
        return web.Response(status=code, text="missing")

    async def slow(request: web.Request) -> web.Response:
        await asyncio.sleep(float(request.query.get("delay", "0.5")))
        return web.Response(text="done")

    async def track(_request: web.Request) -> web.Response:
        state["in_flight"] += 1
        state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
        try:
            await asyncio.sleep(0.2)
            return web.Response(text="tracked")
        finally:
            state["in_flight"] -= 1

    app = web.Application()
    app.router.add_get("/page/{name}", page)
    app.router.add_get("/status/{code}", status)
    app.router.add_get("/slow", slow)
    app.router.add_get("/track", track)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, site.name.rstrip("/"), state


async def demo_public() -> None:
    _section("Параллельная загрузка публичных URL")

    crawler = AsyncCrawler(
        max_concurrent=5,
        connect_timeout=10,
        read_timeout=20,
        total_timeout=25,
    )
    try:
        started = time.perf_counter()
        results = await crawler.fetch_urls(PUBLIC_URLS)
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()

    _print_report(results, elapsed)


async def demo_errors(base_url: str) -> None:
    _section("Ошибки отдельных адресов не останавливают остальные")

    urls = [
        f"{base_url}/page/a",
        f"{base_url}/status/404",
        f"{base_url}/page/b",
        f"{base_url}/status/500",
    ]
    crawler = AsyncCrawler(max_concurrent=4)
    try:
        started = time.perf_counter()
        results = await crawler.fetch_urls(urls)
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()

    _print_report(results, elapsed)

    _section("Несуществующий адрес")
    missing = "http://127.0.0.1:1/"
    crawler = AsyncCrawler(connect_timeout=1, read_timeout=1, total_timeout=2)
    try:
        body = await crawler.fetch_url(missing)
    finally:
        await crawler.close()
    print(f"  FAIL  {missing}" if not body else f"  OK    {missing}")
    print("Запрос завершился без падения программы.")

    _section("Таймаут")
    slow_url = f"{base_url}/slow?delay=1"
    crawler = AsyncCrawler(connect_timeout=1, read_timeout=0.2, total_timeout=0.4)
    try:
        started = time.perf_counter()
        body = await crawler.fetch_url(slow_url)
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()
    print(f"  {'FAIL' if not body else 'OK'}  {slow_url}")
    print(f"Сервер отвечал бы через 1 с, клиент остановился через {elapsed:.2f} с.")


async def demo_speedup(base_url: str) -> None:
    _section("Сравнение последовательной и параллельной загрузки")

    delay = 0.5
    count = 6
    urls = [f"{base_url}/slow?delay={delay}&i={index}" for index in range(count)]
    crawler = AsyncCrawler(max_concurrent=count, connect_timeout=5, read_timeout=5)
    try:
        started = time.perf_counter()
        sequential_results = await _fetch_sequential(crawler, urls)
        sequential = time.perf_counter() - started

        started = time.perf_counter()
        parallel_results = await crawler.fetch_urls(urls)
        parallel = time.perf_counter() - started
    finally:
        await crawler.close()

    print(f"Запросов: {count}, искусственная задержка ответа: {delay:.1f} с")
    print(f"Последовательно: {sequential:.2f} с")
    print(f"Параллельно:     {parallel:.2f} с")
    if parallel > 0:
        print(f"Ускорение:       {sequential / parallel:.2f}x")
    if not all(sequential_results.values()) or not all(parallel_results.values()):
        print("Часть запросов завершилась ошибкой, сравнение времени ненадёжно.")


async def demo_concurrency(base_url: str, state: dict[str, int]) -> None:
    _section("Ограничение числа одновременных запросов")

    limit = 2
    count = 6
    urls = [f"{base_url}/track?i={index}" for index in range(count)]
    crawler = AsyncCrawler(max_concurrent=limit, read_timeout=5)
    try:
        results = await crawler.fetch_urls(urls)
    finally:
        await crawler.close()

    loaded = sum(bool(body) for body in results.values())
    print(f"Адресов: {count}, max_concurrent: {limit}")
    print(f"Успешно загружено: {loaded}")
    print(f"Пик одновременных запросов на сервере: {state['max_in_flight']}")


async def main() -> None:
    await demo_public()

    runner, base_url, state = await _start_server()
    try:
        await demo_errors(base_url)
        await demo_speedup(base_url)
        await demo_concurrency(base_url, state)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

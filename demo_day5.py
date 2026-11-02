"""День 5: классы ошибок, повторы и временное закрытие домена.

Запуск из корня проекта:

    python demo_day5.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from aiohttp import web

from crawler import (
    AsyncCrawler,
    CircuitBreaker,
    NetworkError,
    ParseError,
    PermanentError,
    RetryStrategy,
    TransientError,
    classify_http_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

OUTPUT_PATH = Path("output/day5_errors.json")


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


def _strategy(**overrides) -> RetryStrategy:
    params = {
        "max_retries": 3,
        "backoff_factor": 2.0,
        "retry_on": [TransientError, NetworkError],
        "base_delay": 0.1,
    }
    params.update(overrides)
    return RetryStrategy(**params)


def demo_classification() -> None:
    _section("Классификация кодов HTTP")
    expected = {
        401: PermanentError,
        403: PermanentError,
        404: PermanentError,
        429: TransientError,
        500: TransientError,
        503: TransientError,
    }
    ok = True
    for status, kind in expected.items():
        error = classify_http_status(status, "https://example.com/x")
        matched = type(error) is kind
        ok = ok and matched
        print(f"  {status} -> {type(error).__name__}")
    print(f"Коды разобраны по классам: {_mark(ok)}")


async def demo_backoff() -> None:
    _section("Пауза удваивается перед каждым повтором")
    strategy = _strategy(base_delay=0.1, backoff_factor=2.0, max_retries=3)
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 4:
            raise TransientError("временно", url="http://fake/backoff")
        return "готово"

    started = time.perf_counter()
    result = await strategy.execute_with_retry(flaky)
    elapsed = time.perf_counter() - started
    delays = [round(item, 3) for item in strategy.retry_delays]
    print(f"Паузы: {delays}, результат: {result}, вызовов: {calls['n']}")
    print(f"Время с паузами: {elapsed:.2f} с")
    grows = delays == [0.1, 0.2, 0.4]
    print(f"Паузы 0.1, 0.2, 0.4 с: {_mark(grows and elapsed >= 0.65)}")
    print(f"Удачный повтор записан: {_mark(strategy.successful_retries == 1)}")


async def demo_parse_error() -> None:
    _section("Ошибку разбора не повторяют")
    strategy = _strategy(max_retries=3)
    calls = {"n": 0}

    async def broken() -> str:
        calls["n"] += 1
        raise ParseError("битый html", url="http://fake/parse")

    try:
        await strategy.execute_with_retry(broken)
    except ParseError:
        caught = True
    else:
        caught = False
    print(f"Вызовов: {calls['n']}, пауз: {len(strategy.retry_delays)}")
    print(f"Один вызов и исключение наружу: {_mark(caught and calls['n'] == 1 and not strategy.retry_delays)}")


async def _start_server() -> tuple[web.AppRunner, str, dict[str, int]]:
    hits: dict[str, int] = {}

    def bump(path: str) -> int:
        hits[path] = hits.get(path, 0) + 1
        return hits[path]

    async def unavailable(_request: web.Request) -> web.Response:
        if bump("/unavailable") < 3:
            return web.Response(status=503, text="later")
        return web.Response(text=_html("unavailable", []), content_type="text/html")

    async def boom(_request: web.Request) -> web.Response:
        bump("/boom")
        return web.Response(status=500, text="boom")

    async def missing(_request: web.Request) -> web.Response:
        bump("/missing")
        return web.Response(status=404, text="no")

    async def denied(_request: web.Request) -> web.Response:
        bump("/denied")
        return web.Response(status=403, text="no")

    async def unauthorized(_request: web.Request) -> web.Response:
        bump("/unauthorized")
        return web.Response(status=401, text="no")

    async def busy(_request: web.Request) -> web.Response:
        if bump("/busy") == 1:
            return web.Response(status=429, text="slow down")
        return web.Response(text=_html("busy", []), content_type="text/html")

    async def slow(_request: web.Request) -> web.Response:
        bump("/slow")
        await asyncio.sleep(0.45)
        return web.Response(text=_html("slow", []), content_type="text/html")

    async def outage(_request: web.Request) -> web.Response:
        bump("/outage")
        return web.Response(status=503, text="down")

    async def probe(_request: web.Request) -> web.Response:
        bump("/probe")
        return web.Response(text=_html("probe", []), content_type="text/html")

    async def site_home(_request: web.Request) -> web.Response:
        bump("/site")
        links = ["/site/ok", "/site/missing", "/site/denied", "/site/flaky"]
        return web.Response(text=_html("site", links), content_type="text/html")

    async def site_page(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        path = f"/site/{name}"
        count = bump(path)
        if name == "missing":
            return web.Response(status=404, text="no")
        if name == "denied":
            return web.Response(status=403, text="no")
        if name == "flaky" and count < 3:
            return web.Response(status=503, text="later")
        return web.Response(text=_html(name, []), content_type="text/html")

    app = web.Application()
    app.router.add_get("/unavailable", unavailable)
    app.router.add_get("/boom", boom)
    app.router.add_get("/missing", missing)
    app.router.add_get("/denied", denied)
    app.router.add_get("/unauthorized", unauthorized)
    app.router.add_get("/busy", busy)
    app.router.add_get("/slow", slow)
    app.router.add_get("/outage", outage)
    app.router.add_get("/probe", probe)
    app.router.add_get("/site", site_home)
    app.router.add_get("/site/{name}", site_page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}", hits


async def demo_http(origin: str, hits: dict[str, int]) -> None:
    _section("Повторы 503, отказ для 404 и короткая серия для 500")
    strategy = _strategy(base_delay=0.1)
    crawler = AsyncCrawler(
        max_concurrent=1,
        retry_strategy=strategy,
        circuit_breaker=CircuitBreaker(failure_threshold=20, recovery_timeout=30),
        backoff_base=0,
        connect_timeout=2,
        read_timeout=2,
        total_timeout=3,
    )
    try:
        unavailable = f"{origin}/unavailable"
        body = await strategy.execute_with_retry(crawler.fetch_url, unavailable)
        service_delay = strategy.retry_delays[0]
        print(f"503: обращений {hits.get('/unavailable', 0)}, паузы {[round(item, 3) for item in strategy.retry_delays]}")
        print(
            "503 повторён и страница открылась: "
            f"{_mark(hits.get('/unavailable') == 3 and 'unavailable' in body and strategy.successful_retries == 1)}"
        )

        strategy.reset_stats()
        await crawler.fetch_url(f"{origin}/boom")
        print(f"500: обращений {hits.get('/boom', 0)}")
        print(f"500 повторён один раз, не три: {_mark(hits.get('/boom') == 2)}")

        strategy.reset_stats()
        await crawler.fetch_url(f"{origin}/missing")
        await crawler.fetch_url(f"{origin}/denied")
        await crawler.fetch_url(f"{origin}/unauthorized")
        permanent = strategy.permanent_urls
        print(f"Постоянные адреса: {len(permanent)}, обращений 404/403/401: {hits.get('/missing')}/{hits.get('/denied')}/{hits.get('/unauthorized')}")
        print(
            "404, 403 и 401 не повторяются: "
            f"{_mark(hits.get('/missing') == 1 and hits.get('/denied') == 1 and hits.get('/unauthorized') == 1 and len(permanent) == 3)}"
        )

        strategy.reset_stats()
        busy_body = await crawler.fetch_url(f"{origin}/busy")
        busy_delay = strategy.retry_delays[0]
        print(f"429: пауза {busy_delay:.3f} с против {service_delay:.3f} с у 503")
        print(
            "429 ждёт дольше обычной временной ошибки: "
            f"{_mark('busy' in busy_body and busy_delay > service_delay * 1.5)}"
        )
    finally:
        await crawler.close()


async def demo_timeout(origin: str, hits: dict[str, int]) -> None:
    _section("Таймаут повторяется, лимит времени на повторе больше")
    strategy = _strategy(max_retries=2, base_delay=0.05, timeout_factor=3)
    crawler = AsyncCrawler(
        max_concurrent=1,
        retry_strategy=strategy,
        backoff_base=0,
        connect_timeout=0.2,
        read_timeout=0.2,
        total_timeout=0.2,
    )
    try:
        body = await crawler.fetch_url(f"{origin}/slow")
    finally:
        await crawler.close()
    kinds = [event["error_type"] for event in strategy.events]
    print(f"Обращений: {hits.get('/slow', 0)}, ошибки: {kinds}, масштаб таймаута: {strategy.timeout_scale(1)} -> {strategy.timeout_scale(2)}")
    print(
        "Первая попытка не успела, вторая с большим таймаутом успела: "
        f"{_mark(hits.get('/slow') == 2 and 'slow' in body and kinds == ['TransientError'] and strategy.successful_retries == 1)}"
    )


async def demo_network() -> None:
    _section("Сетевая ошибка повторяется")
    strategy = _strategy(max_retries=2, base_delay=0.05)
    crawler = AsyncCrawler(
        max_concurrent=1,
        retry_strategy=strategy,
        backoff_base=0,
        connect_timeout=0.4,
        read_timeout=0.4,
        total_timeout=0.5,
    )
    try:
        await crawler.fetch_url("http://127.0.0.1:1/down")
    finally:
        await crawler.close()
    kinds = [event["error_type"] for event in strategy.events]
    print(f"Попытки: {len(kinds)}, классы: {set(kinds)}")
    print(f"Три попытки, все NetworkError: {_mark(kinds == ['NetworkError'] * 3)}")
    print(f"В постоянные адреса отказ соединения не попал: {_mark(not strategy.permanent_urls)}")


async def demo_circuit(origin: str, hits: dict[str, int]) -> None:
    _section("Домен закрывается после серии ошибок и потом пробуется снова")
    crawler = AsyncCrawler(
        max_concurrent=1,
        retry_strategy=RetryStrategy(max_retries=0),
        circuit_breaker=CircuitBreaker(failure_threshold=3, recovery_timeout=0.4),
        backoff_base=0,
        connect_timeout=2,
        read_timeout=2,
    )
    try:
        for _ in range(3):
            await crawler.fetch_url(f"{origin}/outage")
        await crawler.fetch_url(f"{origin}/probe")
        blocked = hits.get("/probe", 0)
        await asyncio.sleep(0.55)
        body = await crawler.fetch_url(f"{origin}/probe")
    finally:
        await crawler.close()
    print(f"Ошибок 503: {hits.get('/outage', 0)}, проб до паузы: {blocked}, после: {hits.get('/probe', 0)}")
    print(
        "Три ошибки закрыли домен, после паузы проба прошла: "
        f"{_mark(hits.get('/outage') == 3 and blocked == 0 and hits.get('/probe') == 1 and 'probe' in body)}"
    )


async def demo_crawl(origin: str) -> None:
    _section("Обход страниц с разными ошибками")
    strategy = _strategy(base_delay=0.05, max_retries=3)
    crawler = AsyncCrawler(
        max_concurrent=2,
        max_depth=1,
        retry_strategy=strategy,
        circuit_breaker=CircuitBreaker(failure_threshold=20),
        backoff_base=0,
        connect_timeout=2,
        read_timeout=2,
        total_timeout=3,
    )
    try:
        results = await crawler.crawl(
            [f"{origin}/site"],
            max_pages=10,
            same_domain_only=True,
        )
        stats = crawler.error_stats()
    finally:
        await crawler.close()

    prefix = origin.rstrip("/")
    processed = sorted(url.removeprefix(prefix) or "/" for url in results)
    failed = {
        url.removeprefix(prefix) or "/": reason
        for url, reason in crawler.failed_urls.items()
    }
    print(f"Обработано: {processed}")
    print(f"Ошибки: {failed}")
    print(
        "По типам: "
        f"{stats['errors_by_type']}, удачных повторов {stats['successful_retries']}, "
        f"средняя пауза {stats['average_retry_time']:.3f} с"
    )
    print(f"Постоянные адреса: {stats['permanent_urls']}")
    pages_ok = processed == ["/site", "/site/flaky", "/site/ok"]
    permanent_ok = (
        "PermanentError" in failed.get("/site/missing", "")
        and "404" in failed.get("/site/missing", "")
        and "403" in failed.get("/site/denied", "")
    )
    stats_ok = (
        stats["successful_retries"] >= 1
        and stats["average_retry_time"] > 0
        and stats["errors_by_type"].get("TransientError", 0) >= 1
        and stats["errors_by_type"].get("PermanentError", 0) >= 2
        and len(stats["permanent_urls"]) >= 2
    )
    print(f"Открыты живые страницы, 503 дожат повтором: {_mark(pages_ok)}")
    print(f"404 и 403 записаны как постоянные: {_mark(permanent_ok)}")
    print(f"Статистика ошибок сходится: {_mark(stats_ok)}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "stats": stats,
        "events": strategy.events,
        "processed": processed,
        "failed": failed,
    }
    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Отчёт записан в {OUTPUT_PATH}")


async def main() -> None:
    demo_classification()
    await demo_backoff()
    await demo_parse_error()
    runner, origin, hits = await _start_server()
    try:
        await demo_http(origin, hits)
        await demo_timeout(origin, hits)
        await demo_network()
        await demo_circuit(origin, hits)
        await demo_crawl(origin)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

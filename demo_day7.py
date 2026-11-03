"""День 7: sitemap, конфиг, отчёт, CLI и скорость.

Запуск из корня проекта:

    python demo_day7.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import tracemalloc
import urllib.request
from pathlib import Path

import aiohttp
from aiohttp import web

from crawler import AdvancedCrawler, SitemapParser
from crawler.logconfig import setup_logging

ROOT = Path("output/day7")


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _mark(condition: bool) -> str:
    return "OK" if condition else "НЕТ"


def _html(title: str, links: list[str]) -> str:
    anchors = "".join(f'<a href="{href}">{href}</a>' for href in links)
    return (
        "<!DOCTYPE html><html><head>"
        f"<title>{title}</title></head><body><h1>{title}</h1>{anchors}</body></html>"
    )


def _origin_of(request: web.Request) -> str:
    return f"{request.scheme}://{request.host}"


async def _start_server() -> tuple[web.AppRunner, str]:
    async def page(request: web.Request) -> web.Response:
        origin = _origin_of(request)
        path = request.path
        if path == "/robots.txt":
            return web.Response(text="User-agent: *\nAllow: /\n", content_type="text/plain")
        if path == "/sitemap.xml":
            body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                f"<sitemap><loc>{origin}/child.xml</loc></sitemap>"
                "</sitemapindex>"
            )
            return web.Response(text=body, content_type="application/xml")
        if path == "/child.xml":
            body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                f"<url><loc>{origin}/</loc></url>"
                f"<url><loc>{origin}/hidden</loc></url>"
                "</urlset>"
            )
            return web.Response(text=body, content_type="application/xml")
        if path == "/loop.xml":
            body = (
                "<sitemapindex>"
                f"<sitemap><loc>{origin}/loop.xml</loc></sitemap>"
                "</sitemapindex>"
            )
            return web.Response(text=body, content_type="application/xml")
        if path == "/bad.xml":
            return web.Response(text="это не xml", content_type="application/xml")
        if path == "/":
            return web.Response(text=_html("Главная", ["/linked"]), content_type="text/html")
        if path == "/hidden":
            return web.Response(text=_html("Скрытая", []), content_type="text/html")
        if path == "/linked":
            return web.Response(text=_html("Ссылка", []), content_type="text/html")
        if path.startswith("/slow/") or path.startswith("/fast/"):
            if path.startswith("/slow/"):
                await asyncio.sleep(0.05)
            name = path.rsplit("/", 1)[-1]
            return web.Response(text=_html(name, []), content_type="text/html")
        return web.Response(status=404, text="no")

    app = web.Application()
    app.router.add_get("/", page)
    app.router.add_get("/{tail:.*}", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}"


async def demo_sitemap(origin: str) -> None:
    _section("Sitemap: индекс, цикл и битый файл")

    async def download(url: str) -> tuple[int, str]:
        async with session.get(url) as response:
            return response.status, await response.text()

    async with aiohttp.ClientSession() as session:
        parser = SitemapParser(download)
        pages = await parser.fetch_sitemap(f"{origin}/sitemap.xml")
        looped = await parser.fetch_sitemap(f"{origin}/loop.xml")
        broken = await parser.fetch_sitemap(f"{origin}/bad.xml")
        missing = await parser.fetch_sitemap(f"{origin}/missing.xml")
    print(f"Из индекса: {pages}")
    print(
        "Индекс раскрыт до страниц, цикл и ошибки дают пустой список: "
        f"{_mark(set(pages) == {f'{origin}/', f'{origin}/hidden'} and looped == [] and broken == [] and missing == [])}"
    )


async def demo_config(origin: str) -> None:
    _section("Конфиг, статистика, JSON и HTML-отчёт")
    config_path = ROOT / "demo.yaml"
    config_path.write_text(
        "\n".join(
            [
                "start_urls:",
                f"  - {origin}/",
                "max_pages: 10",
                "max_depth: 1",
                "max_concurrent: 4",
                "requests_per_second: 100",
                "respect_robots: true",
                "same_domain_only: true",
                "use_sitemap: true",
                "exclude_patterns:",
                '  - "linked"',
                "output: output/day7/pages.json",
                "storage:",
                "  format: json",
                "  batch_size: 10",
                "log_file: output/day7/crawler.log",
                "log_level: INFO",
                "show_progress: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    crawler = AdvancedCrawler.from_config(config_path)
    captured = _Stderr()
    old = sys.stderr
    sys.stderr = captured
    try:
        await crawler.crawl()
        stats = crawler.get_stats()
        crawler.export_to_json(str(ROOT / "stats.json"))
        crawler.export_to_html_report(str(ROOT / "report.html"))
    finally:
        sys.stderr = old
        await crawler.close()
    print(captured.getvalue().strip() or "(прогресс пуст)")
    saved = json.loads((ROOT / "pages.json").read_text(encoding="utf-8"))
    saved_urls = {row["url"] for row in saved}
    report = (ROOT / "report.html").read_text(encoding="utf-8")
    exported = json.loads((ROOT / "stats.json").read_text(encoding="utf-8"))
    print(f"Статистика: {stats}")
    print(
        "Sitemap добавил скрытую страницу, ссылка с главной отфильтрована: "
        f"{_mark(saved_urls == {f'{origin}/', f'{origin}/hidden'} and stats['successful'] == 2 and stats['failed'] == 0)}"
    )
    print(
        "JSON и HTML содержат сводку и код 200: "
        f"{_mark(exported['total_pages'] == 2 and 'Скрытая' in report and 'Коды ответов' in report and 'bar' in report)}"
    )
    progress = captured.getvalue()
    print(
        "На экране процент, скорость, ETA и активные задачи: "
        f"{_mark('%' in progress and 'стр/с' in progress and 'ETA' in progress and 'активных' in progress)}"
    )


def demo_logging() -> None:
    _section("Журнал с временем и ротацией файла")
    path = ROOT / "rotate.log"
    backup = Path(str(path) + ".1")
    for file in (path, backup):
        if file.exists():
            file.unlink()
    logger = setup_logging(
        level="DEBUG",
        log_file=path,
        max_bytes=400,
        backup_count=2,
        logger_name="crawler.day7.rotate",
    )
    for index in range(6):
        logger.warning("запись %s %s", index, "x" * 180)
    for handler in logger.handlers:
        handler.flush()
    combined = ""
    if path.exists():
        combined += path.read_text(encoding="utf-8")
    if backup.exists():
        combined += backup.read_text(encoding="utf-8")
    print(
        "Файл журнала порезан, в строке есть время и уровень: "
        f"{_mark(backup.exists() and '[WARNING]' in combined and combined[:4].isdigit())}"
    )


async def demo_cli(origin: str) -> None:
    _section("Командная строка")
    help_proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "crawler.py",
        "--help",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    help_out, _help_err = await help_proc.communicate()
    help_text = help_out.decode()
    output = ROOT / "cli.json"
    if output.exists():
        output.unlink()
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "crawler.py",
        "--urls",
        f"{origin}/",
        "--max-pages",
        "5",
        "--max-depth",
        "0",
        "--output",
        str(output),
        "--rate-limit",
        "50",
        "--respect-robots",
        "--log-level",
        "WARNING",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    print(stdout.decode().strip())
    if proc.returncode != 0:
        print(stderr.decode())
    saved = output.read_text(encoding="utf-8") if output.exists() else ""
    print(
        "Справка и обход из командной строки: "
        f"{_mark(help_proc.returncode == 0 and '--max-pages' in help_text and proc.returncode == 0 and origin in saved)}"
    )


def _sync_fetch(urls: list[str]) -> int:
    done = 0
    for url in urls:
        with urllib.request.urlopen(url, timeout=5) as response:
            response.read()
        done += 1
    return done


async def _timed_crawl(urls: list[str], *, concurrent: int) -> tuple[float, int]:
    crawler = AdvancedCrawler(
        urls,
        max_pages=len(urls),
        max_depth=0,
        max_concurrent=concurrent,
        requests_per_second=None,
        respect_robots=False,
        same_domain_only=True,
        show_progress=False,
        log_level="ERROR",
    )
    started = time.perf_counter()
    try:
        results = await crawler.crawl()
    finally:
        await crawler.close()
    return time.perf_counter() - started, len(results)


async def demo_performance(origin: str) -> None:
    _section("Скорость и память на локальном сервере")
    slow = [f"{origin}/slow/{index}" for index in range(40)]
    sync_started = time.perf_counter()
    sync_done = await asyncio.to_thread(_sync_fetch, slow)
    sync_elapsed = time.perf_counter() - sync_started
    async_elapsed, async_done = await _timed_crawl(slow, concurrent=10)
    print(
        f"40 страниц по 0.05 с: синхронно {sync_elapsed:.2f} с ({sync_done}), "
        f"асинхронно {async_elapsed:.2f} с ({async_done})"
    )
    print(
        "Асинхронный обход быстрее синхронного: "
        f"{_mark(async_done == 40 and sync_done == 40 and async_elapsed < sync_elapsed * 0.6)}"
    )

    print(f"{'страниц':>8} {'секунды':>10} {'память, МБ':>12}")
    largest_ok = True
    memory_ok = True
    for count, concurrent in ((100, 20), (500, 40), (1000, 40)):
        urls = [f"{origin}/fast/{index}" for index in range(count)]
        tracemalloc.start()
        elapsed, done = await _timed_crawl(urls, concurrent=concurrent)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peak_mb = peak / (1024 * 1024)
        print(f"{count:8d} {elapsed:10.2f} {peak_mb:12.1f}")
        if done != count:
            largest_ok = False
        if count == 1000 and (elapsed >= 20 or peak_mb >= 80):
            memory_ok = False
            largest_ok = False
    print(
        "1000 страниц уложились в 20 с и 80 МБ: "
        f"{_mark(largest_ok and memory_ok)}"
    )


class _Stderr:
    def __init__(self) -> None:
        self._parts: list[str] = []

    def write(self, text: str) -> int:
        self._parts.append(text)
        return len(text)

    def flush(self) -> None:
        return None

    def getvalue(self) -> str:
        return "".join(self._parts)


async def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    runner, origin = await _start_server()
    print(f"Локальный сервер: {origin}")
    try:
        await demo_sitemap(origin)
        await demo_config(origin)
        demo_logging()
        await demo_cli(origin)
        await demo_performance(origin)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

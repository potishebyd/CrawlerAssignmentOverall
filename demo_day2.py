"""День 2: загрузка страниц и разбор HTML.

Запуск из корня проекта:

    python demo_day2.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys

from aiohttp import web

from crawler import AsyncCrawler, HTMLParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

PUBLIC_URLS = [
    "https://example.com",
    "https://example.org",
    "https://httpbin.org/html",
]

PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
  <title>Демонстрационная страница</title>
  <meta name="description" content="Страница для парсера">
  <meta name="keywords" content="краулер, html, день2">
</head>
<body>
  <h1>Главный заголовок</h1>
  <h2>Раздел</h2>
  <h3>Подраздел</h3>
  <p>Абзац текста о краулере.</p>
  <a href="/about">О проекте</a>
  <a href="guide.html">Руководство</a>
  <a href="https://example.com/docs">Внешняя документация</a>
  <a href="javascript:void(0)">Скрипт</a>
  <img src="/images/logo.png" alt="Логотип">
  <img src="photo.jpg" alt="Фото">
  <ul>
    <li>Первый</li>
    <li>Второй</li>
  </ul>
  <ol>
    <li>Шаг 1</li>
    <li>Шаг 2</li>
  </ol>
  <table>
    <thead>
      <tr><th>Имя</th><th>Роль</th></tr>
    </thead>
    <tbody>
      <tr><td>Анна</td><td>Парсер</td></tr>
    </tbody>
  </table>
</body>
</html>
"""

BROKEN_HTML = """<html><head><title>Битая страница</title>
<body>
<a href="/ok">рабочая ссылка
<a href="javascript:alert(1)">скрипт
<p>текст без закрытия
<img src="pic.png" alt="Картинка">
"""


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _summary(parsed: dict) -> dict:
    links = parsed["links"]
    return {
        "url": parsed["url"],
        "title": parsed["title"],
        "text_length": len(parsed["text"]),
        "links_count": len(links),
        "links": links,
        "images_count": len(parsed["images"]),
    }


def _print_parsed(parsed: dict) -> None:
    print(json.dumps(_summary(parsed), ensure_ascii=False, indent=2))


async def _start_server() -> tuple[web.AppRunner, str]:
    async def page(_request: web.Request) -> web.Response:
        return web.Response(text=PAGE_HTML, content_type="text/html")

    app = web.Application()
    app.router.add_get("/docs/index.html", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}/docs/index.html"


async def demo_public() -> None:
    _section("Публичные страницы")
    crawler = AsyncCrawler(max_concurrent=3, connect_timeout=10, read_timeout=20, total_timeout=25)
    try:
        pages = await asyncio.gather(*(crawler.fetch_and_parse(url) for url in PUBLIC_URLS))
    finally:
        await crawler.close()

    for parsed in pages:
        _print_parsed(parsed)
        if parsed["text"]:
            preview = parsed["text"][:120]
            print(f"Текст: {preview}")
        print()


async def demo_local(page_url: str) -> None:
    _section("Локальная страница: ссылки, заголовки, таблицы, списки, картинки")
    crawler = AsyncCrawler()
    try:
        parsed = await crawler.fetch_and_parse(page_url)
    finally:
        await crawler.close()

    _print_parsed(parsed)
    print("Метаданные:")
    print(json.dumps(parsed["metadata"], ensure_ascii=False, indent=2))
    print("Заголовки:")
    print(json.dumps(parsed["headings"], ensure_ascii=False, indent=2))
    print("Изображения:")
    print(json.dumps(parsed["images"], ensure_ascii=False, indent=2))
    print("Списки:")
    print(json.dumps(parsed["lists"], ensure_ascii=False, indent=2))
    print("Таблицы:")
    print(json.dumps(parsed["tables"], ensure_ascii=False, indent=2))

    origin = page_url.split("/docs/")[0]
    expected = [
        f"{origin}/about",
        f"{origin}/docs/guide.html",
        "https://example.com/docs",
    ]
    print("\nОтносительные ссылки стали абсолютными:")
    for link in expected:
        mark = "OK" if link in parsed["links"] else "НЕТ"
        print(f"  {mark}  {link}")
    script_links = [link for link in parsed["links"] if link.startswith("javascript:")]
    print(f"Ссылок javascript в списке: {len(script_links)}")


async def demo_same_host(page_url: str) -> None:
    _section("Только ссылки того же хоста")
    crawler = AsyncCrawler(same_host_only=True)
    try:
        parsed = await crawler.fetch_and_parse(page_url)
    finally:
        await crawler.close()

    print(json.dumps(parsed["links"], ensure_ascii=False, indent=2))


async def demo_broken() -> None:
    _section("Битый HTML не останавливает разбор")
    parser = HTMLParser()
    parsed = await parser.parse_html(BROKEN_HTML, "https://example.com/broken")
    _print_parsed(parsed)
    print(f"Текст сохранился: {'текст без закрытия' in parsed['text']}")
    print(f"Картинок: {len(parsed['images'])}")


async def main() -> None:
    await demo_public()
    runner, page_url = await _start_server()
    try:
        await demo_local(page_url)
        await demo_same_host(page_url)
    finally:
        await runner.cleanup()
    await demo_broken()


if __name__ == "__main__":
    asyncio.run(main())

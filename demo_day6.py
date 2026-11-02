"""День 6: сохранение страниц в JSON, CSV и SQLite.

Запуск из корня проекта:

    python demo_day6.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from aiohttp import web

from crawler import AsyncCrawler, CSVStorage, JSONStorage, SQLiteStorage
from crawler.storage import RECORD_FIELDS, DataStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

ROOT = Path("output/day6")
HOME_TITLE = 'Главная, "цитата"'


def _section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def _mark(condition: bool) -> str:
    return "OK" if condition else "НЕТ"


def _html(title: str, links: list[str], text: str) -> str:
    anchors = "\n".join(f'<a href="{href}">{href}</a>' for href in links)
    return (
        "<!DOCTYPE html><html><head>"
        f"<title>{title}</title>"
        '<meta name="description" content="описание, с запятой">'
        "</head><body>"
        f"<h1>{title}</h1><p>{text}</p>{anchors}</body></html>"
    )


def _page(title: str, links: list[str]) -> str:
    return _html(title, links, title)


class FlakyStorage(DataStorage):
    """Первая запись падает, следующая попытка проходит."""

    def __init__(self) -> None:
        super().__init__(batch_size=1, save_retries=2, retry_delay=0.01)
        self.attempts = 0
        self.rows: list[dict] = []

    async def _write_batch(self, batch: list[dict]) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise OSError("временный сбой записи")
        self.rows.extend(batch)


class BrokenStorage(DataStorage):
    """Запись не удаётся ни разу. Обход из-за этого не останавливается."""

    def __init__(self) -> None:
        super().__init__(batch_size=1, save_retries=1, retry_delay=0.01)
        self.attempts = 0

    async def _write_batch(self, batch: list[dict]) -> None:
        self.attempts += 1
        raise OSError("диск недоступен")


async def _start_server() -> tuple[web.AppRunner, str]:
    async def page(request: web.Request) -> web.Response:
        path = request.path
        if path == "/":
            body = _page(HOME_TITLE, ["/docs", "/about"])
        elif path == "/docs":
            body = _page("Документы", [])
        elif path == "/about":
            body = _page("О сайте", [])
        else:
            return web.Response(status=404, text="no")
        return web.Response(text=body, content_type="text/html")

    app = web.Application()
    app.router.add_get("/", page)
    app.router.add_get("/{name}", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    return runner, f"http://{host}:{port}"


def _record(url: str, title: str, text: str) -> dict:
    return {
        "url": url,
        "title": title,
        "text": text,
        "links": [],
        "metadata": {"description": "описание, с запятой"},
        "status_code": 200,
        "content_type": "text/html",
    }


async def demo_json_lines() -> None:
    _section("JSON по строке на страницу")
    path = ROOT / "lines.jsonl"
    storage = JSONStorage(path, indent=None, overwrite=True, batch_size=1)
    newline_text = 'Строка с запятой, кавычкой " и\nпереносом'
    await storage.save(_record("https://example.test/1", "Первая", newline_text))
    await storage.save(_record("https://example.test/2", "Вторая", "коротко"))
    await storage.close()
    raw_lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows = await storage.load()
    print(f"Строк в файле: {len(raw_lines)}, прочитано: {len(rows)}")
    print(
        "Две строки, перенос внутри текста не рвёт запись: "
        f"{_mark(len(raw_lines) == 2 and rows[0]['text'] == newline_text and rows[1]['title'] == 'Вторая')}"
    )


async def demo_csv_roundtrip() -> None:
    _section("CSV сохраняет запятые, кавычки и переносы")
    path = ROOT / "special.csv"
    storage = CSVStorage(path, encoding="utf-8", overwrite=True)
    text = 'Поле, с "кавычками"\nи второй строкой'
    await storage.save(_record("https://example.test/csv", "Заголовок, да", text))
    await storage.close()
    rows = await storage.load()
    header = path.read_text(encoding="utf-8").splitlines()[0]
    print(f"Заголовок: {header}")
    print(
        "Заголовки определены, спецсимволы читаются обратно: "
        f"{_mark(header.split(',') == list(RECORD_FIELDS) and len(rows) == 1 and rows[0]['text'] == text and rows[0]['metadata']['description'] == 'описание, с запятой')}"
    )


async def _crawl(origin: str, storage: DataStorage) -> dict:
    crawler = AsyncCrawler(
        max_concurrent=1,
        max_depth=1,
        storage=storage,
        backoff_base=0,
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        return await crawler.crawl([f"{origin}/"], max_pages=10, same_domain_only=True)
    finally:
        await crawler.close()


def _saved_ok(rows: list[dict], origin: str) -> bool:
    by_url = {row["url"]: row for row in rows}
    expected = {f"{origin}/", f"{origin}/docs", f"{origin}/about"}
    if set(by_url) != expected:
        return False
    home = by_url[f"{origin}/"]
    return (
        home["title"] == HOME_TITLE
        and home["status_code"] == 200
        and home["content_type"] == "text/html"
        and len(home["links"]) == 2
        and home["metadata"].get("description") == "описание, с запятой"
        and isinstance(home["crawled_at"], datetime)
        and by_url[f"{origin}/docs"]["title"] == "Документы"
        and by_url[f"{origin}/about"]["title"] == "О сайте"
    )


async def demo_crawl(origin: str) -> None:
    _section("Обход пишется в JSON, CSV и SQLite")
    json_storage = JSONStorage(ROOT / "results.json", indent=2, overwrite=True, batch_size=3)
    csv_storage = CSVStorage(ROOT / "results.csv", encoding="utf-8", overwrite=True)
    db_storage = SQLiteStorage(ROOT / "crawler.db", overwrite=True, batch_size=2)

    await _crawl(origin, json_storage)
    json_rows = await json_storage.load()
    pretty = (ROOT / "results.json").read_text(encoding="utf-8")
    print(f"JSON: {json_storage.stats()}")
    print(
        "Форматированный JSON, одна пачка на три страницы: "
        f"{_mark(_saved_ok(json_rows, origin) and pretty.lstrip().startswith('[') and json_storage.write_batches == 1)}"
    )

    await _crawl(origin, csv_storage)
    csv_rows = await csv_storage.load()
    print(f"CSV: {csv_storage.stats()}")
    print(f"CSV совпадает со страницами: {_mark(_saved_ok(csv_rows, origin))}")

    await _crawl(origin, db_storage)
    db_rows = await db_storage.load()
    indexes = await db_storage.index_names()
    print(f"SQLite: {db_storage.stats()}, индексы: {indexes}")
    print(
        "База читается, пачки по две строки, индексы на месте: "
        f"{_mark(_saved_ok(db_rows, origin) and db_storage.write_batches == 2 and 'idx_pages_status_code' in indexes and 'idx_pages_crawled_at' in indexes)}"
    )


async def demo_write_errors(origin: str) -> None:
    _section("Сбой записи повторяется и не роняет обход")
    flaky = FlakyStorage()
    await flaky.save(_record("https://example.test/retry", "Повтор", "текст"))
    await flaky.close()
    print(f"Попыток до успеха: {flaky.attempts}, сохранено: {flaky.saved_count}")
    print(f"Повтор записи прошёл: {_mark(flaky.attempts == 2 and flaky.saved_count == 1 and flaky.error_count == 0)}")

    broken = BrokenStorage()
    crawler = AsyncCrawler(
        max_concurrent=1,
        max_depth=0,
        storage=broken,
        backoff_base=0,
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        results = await crawler.crawl([f"{origin}/"], max_pages=1)
    finally:
        await crawler.close()
    print(f"Попыток записи: {broken.attempts}, ошибок: {broken.error_count}, страниц в памяти: {len(results)}")
    print(
        "Обход дошёл до конца, хотя файл не записался: "
        f"{_mark(len(results) == 1 and broken.attempts == 2 and broken.error_count == 1 and broken.saved_count == 0)}"
    )


async def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    await demo_json_lines()
    await demo_csv_roundtrip()
    runner, origin = await _start_server()
    try:
        await demo_crawl(origin)
        await demo_write_errors(origin)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

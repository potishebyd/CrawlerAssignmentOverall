"""Сводка обхода и выгрузка отчёта (день 7)."""

from __future__ import annotations

import html
import json
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit


class CrawlerStats:
    """Считает страницы, коды ответов, домены и скорость одного обхода."""

    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.finished: float | None = None
        self.successful = 0
        self.failed = 0
        self.robots_blocked = 0
        self._status_codes: Counter[int] = Counter()
        self._domains: Counter[str] = Counter()

    def record_success(self, url: str, status_code: int) -> None:
        self.successful += 1
        self._status_codes[int(status_code or 200)] += 1
        self._domains[_host(url)] += 1

    def record_failure(self, url: str, status_code: int = 0) -> None:
        self.failed += 1
        if status_code:
            self._status_codes[int(status_code)] += 1
        self._domains[_host(url)] += 1

    def finish(self) -> None:
        self.finished = time.perf_counter()

    def as_dict(self) -> dict:
        """Снимок для печати и для JSON."""
        elapsed = self.elapsed
        total = self.successful + self.failed
        return {
            "total_pages": total,
            "successful": self.successful,
            "failed": self.failed,
            "robots_blocked": self.robots_blocked,
            "elapsed_seconds": round(elapsed, 3),
            "pages_per_second": round(total / elapsed, 3) if elapsed else 0.0,
            "status_codes": {str(code): count for code, count in sorted(self._status_codes.items())},
            "top_domains": [
                {"domain": domain, "pages": count}
                for domain, count in self._domains.most_common(10)
            ],
        }

    @property
    def elapsed(self) -> float:
        end = self.finished if self.finished is not None else time.perf_counter()
        return max(end - self.started, 0.0)

    def export_to_json(self, filename: str | Path) -> None:
        """Пишет сводку в JSON-файл."""
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def export_to_html_report(self, filename: str | Path, pages: list[dict] | None = None) -> None:
        """Пишет одну HTML-страницу со сводкой, полосками и таблицами."""
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_render_html(self.as_dict(), pages or []), encoding="utf-8")


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").lower() or "—"


def _render_html(stats: dict, pages: list[dict]) -> str:
    status_rows = _bars(stats["status_codes"])
    domain_rows = "".join(
        f"<tr><td>{html.escape(item['domain'])}</td><td>{item['pages']}</td></tr>"
        for item in stats["top_domains"]
    )
    page_rows = "".join(
        "<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(
            html.escape(str(page.get("url", ""))),
            html.escape(str(page.get("title", ""))),
            html.escape(str(page.get("status_code", ""))),
        )
        for page in pages[:50]
    )
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Отчёт краулера</title>
  <style>
    body {{ font-family: sans-serif; margin: 2rem; color: #1c1917; background: #faf7f2; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .cards {{ display: flex; gap: 1rem; flex-wrap: wrap; margin: 1.5rem 0; }}
    .card {{ background: white; border: 1px solid #e7e5e4; border-radius: 8px; padding: 1rem 1.2rem; min-width: 9rem; }}
    .card b {{ display: block; font-size: 1.6rem; }}
    table {{ border-collapse: collapse; width: 100%; background: white; margin: 0.5rem 0 1.5rem; }}
    th, td {{ border-bottom: 1px solid #e7e5e4; text-align: left; padding: 0.4rem 0.6rem; }}
    .bar {{ height: 0.7rem; background: #2563eb; border-radius: 4px; }}
    .track {{ background: #e7e5e4; border-radius: 4px; width: 12rem; }}
  </style>
</head>
<body>
  <h1>Отчёт краулера</h1>
  <p>Время работы: {stats['elapsed_seconds']} с, скорость: {stats['pages_per_second']} стр/с.</p>
  <div class="cards">
    <div class="card">Всего<b>{stats['total_pages']}</b></div>
    <div class="card">Успешно<b>{stats['successful']}</b></div>
    <div class="card">Ошибок<b>{stats['failed']}</b></div>
    <div class="card">Закрыто robots<b>{stats['robots_blocked']}</b></div>
  </div>
  <h2>Коды ответов</h2>
  <table>
    <tr><th>Код</th><th>Страниц</th><th></th></tr>
    {status_rows}
  </table>
  <h2>Домены</h2>
  <table>
    <tr><th>Домен</th><th>Страниц</th></tr>
    {domain_rows}
  </table>
  <h2>Страницы</h2>
  <table>
    <tr><th>Адрес</th><th>Заголовок</th><th>Код</th></tr>
    {page_rows}
  </table>
</body>
</html>
"""


def _bars(status_codes: dict) -> str:
    if not status_codes:
        return "<tr><td colspan='3'>Нет данных</td></tr>"
    largest = max(status_codes.values()) or 1
    rows = []
    for code, count in status_codes.items():
        width = max(4, int(count / largest * 100))
        rows.append(
            "<tr><td>{}</td><td>{}</td><td><div class='track'><div class='bar' style='width:{}%'></div></div></td></tr>".format(
                html.escape(str(code)),
                count,
                width,
            )
        )
    return "".join(rows)

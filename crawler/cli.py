"""Командная строка краулера (день 7)."""

from __future__ import annotations

import argparse
import asyncio

from crawler.advanced import AdvancedCrawler
from crawler.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Асинхронный краулер")
    parser.add_argument("--urls", nargs="+", help="Стартовые URL")
    parser.add_argument("--max-pages", type=int, help="Сколько страниц обработать")
    parser.add_argument("--max-depth", type=int, help="Глубина перехода по ссылкам")
    parser.add_argument("--output", help="Куда сохранить страницы: .json, .csv или .db")
    parser.add_argument("--config", help="Файл настроек YAML или JSON")
    parser.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Соблюдать robots.txt",
    )
    parser.add_argument("--rate-limit", type=float, help="Запросов в секунду")
    parser.add_argument("--log-file", help="Файл журнала")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING или ERROR")
    return parser


def settings_from_args(args: argparse.Namespace) -> dict:
    data = load_config(args.config) if args.config else {}
    if args.urls:
        data["start_urls"] = args.urls
    if args.max_pages is not None:
        data["max_pages"] = args.max_pages
    if args.max_depth is not None:
        data["max_depth"] = args.max_depth
    if args.output:
        data["output"] = args.output
    if args.respect_robots is not None:
        data["respect_robots"] = args.respect_robots
    if args.rate_limit is not None:
        data["requests_per_second"] = args.rate_limit
    if args.log_file:
        data["log_file"] = args.log_file
    if args.log_level:
        data["log_level"] = args.log_level
    if not data.get("start_urls"):
        raise SystemExit("Нужны --urls или --config со start_urls")
    return data


async def _run(crawler: AdvancedCrawler) -> None:
    try:
        await crawler.crawl()
        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Скорость: {stats['pages_per_second']} стр/с")
    finally:
        await crawler.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    crawler = AdvancedCrawler.from_dict(settings_from_args(args))
    asyncio.run(_run(crawler))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

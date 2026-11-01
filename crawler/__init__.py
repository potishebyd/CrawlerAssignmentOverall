"""Асинхронный веб-краулер."""

from crawler.async_crawler import AsyncCrawler
from crawler.html_parser import HTMLParser

__all__ = ["AsyncCrawler", "HTMLParser"]

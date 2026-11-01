"""Асинхронный веб-краулер."""

from crawler.async_crawler import AsyncCrawler
from crawler.html_parser import HTMLParser
from crawler.limits import SemaphoreManager
from crawler.queue import CrawlerQueue

__all__ = ["AsyncCrawler", "CrawlerQueue", "HTMLParser", "SemaphoreManager"]

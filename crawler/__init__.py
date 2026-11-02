"""Асинхронный веб-краулер."""

from crawler.async_crawler import AsyncCrawler
from crawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    TransientError,
    classify_http_status,
)
from crawler.html_parser import HTMLParser
from crawler.limits import SemaphoreManager
from crawler.queue import CrawlerQueue
from crawler.rate_limiter import RateLimiter
from crawler.retry import CircuitBreaker, RetryStrategy
from crawler.robots import RobotsParser
from crawler.storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage

__all__ = [
    "AsyncCrawler",
    "CircuitBreaker",
    "CircuitOpenError",
    "CrawlerError",
    "CSVStorage",
    "CrawlerQueue",
    "DataStorage",
    "HTMLParser",
    "JSONStorage",
    "NetworkError",
    "ParseError",
    "PermanentError",
    "RateLimiter",
    "RetryStrategy",
    "RobotsParser",
    "SQLiteStorage",
    "SemaphoreManager",
    "TransientError",
    "classify_http_status",
]

"""Повторы загрузки и временное закрытие домена (день 5)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass

import aiohttp

from crawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    PermanentError,
    TransientError,
    classify_http_status,
)

logger = logging.getLogger(__name__)

_attempt_number: ContextVar[int] = ContextVar("crawler_retry_attempt", default=1)


def current_attempt() -> int:
    """Номер попытки внутри ``execute_with_retry``. Вне её всегда 1."""
    return _attempt_number.get()


def classify_exception(exc: BaseException, url: str = "") -> CrawlerError | None:
    """Оборачивает сетевое исключение в класс ошибки краулера.

    Уже готовый ``CrawlerError`` возвращается как есть. Незнакомое
    исключение даёт ``None``: его не повторяют и отдают вызывающему.
    """
    if isinstance(exc, CrawlerError):
        if not exc.url and url:
            exc.url = url
        return exc
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return TransientError("таймаут", url=url)
    if isinstance(exc, aiohttp.ClientResponseError):
        return classify_http_status(exc.status or 0, url)
    if isinstance(exc, aiohttp.ClientError):
        return NetworkError(type(exc).__name__, url=url)
    return None


class RetryStrategy:
    """Повторяет вызов, пока ошибка временная и попытки не кончились.

    Пауза перед повтором растёт: ``base_delay * backoff_factor ** номер``.
    Для 429 она дополнительно умножается. 500 повторяется меньше остальных
    временных ошибок. Постоянные ошибки и ошибки разбора не повторяются.
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list | None = None,
        *,
        base_delay: float = 0.25,
        max_delay: float = 30.0,
        retry_limits: dict | None = None,
        backoff_factors: dict | None = None,
        status_retry_limits: dict | None = None,
        status_delay_multiplier: dict | None = None,
        timeout_factor: float = 1.5,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries должен быть >= 0")
        if backoff_factor < 1:
            raise ValueError("backoff_factor должен быть >= 1")
        if base_delay < 0 or max_delay <= 0:
            raise ValueError("base_delay >= 0, max_delay > 0")
        if timeout_factor < 1:
            raise ValueError("timeout_factor должен быть >= 1")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.timeout_factor = timeout_factor
        self.retry_on = tuple(retry_on if retry_on is not None else (TransientError, NetworkError))
        self.retry_limits = {kind: max_retries for kind in self.retry_on}
        if retry_limits:
            self.retry_limits.update(retry_limits)
        self.backoff_factors = {
            TransientError: backoff_factor,
            NetworkError: backoff_factor,
        }
        if backoff_factors:
            self.backoff_factors.update(backoff_factors)
        self.status_retry_limits = (
            {500: min(1, max_retries)} if status_retry_limits is None else dict(status_retry_limits)
        )
        self.status_delay_multiplier = (
            {429: 2.0} if status_delay_multiplier is None else dict(status_delay_multiplier)
        )
        self.errors_by_type: Counter[str] = Counter()
        self.successful_retries = 0
        self.retry_delays: list[float] = []
        self.permanent_urls: list[str] = []
        self.events: list[dict] = []

    async def execute_with_retry(self, coro, *args, **kwargs):
        """Вызывает ``coro(*args, **kwargs)`` и повторяет его после временных ошибок.

        ``coro`` — функция, которая возвращает корутину, а не уже созданная
        корутина: повтор должен запускать запрос заново. Последняя ошибка
        пробрасывается наружу.
        """
        attempt = 0
        seen: dict[str, int] = {}
        url = _call_url(args)
        while True:
            attempt += 1
            token = _attempt_number.set(attempt)
            try:
                result = await coro(*args, **kwargs)
            except Exception as exc:
                classified = classify_exception(exc, url)
                if classified is None:
                    raise
                url = classified.url or url
                bucket = _bucket(classified, self.status_retry_limits)
                seen[bucket] = seen.get(bucket, 0) + 1
                self.errors_by_type[type(classified).__name__] += 1
                will_retry = self._can_retry(classified, seen[bucket])
                delay = self.delay_for(classified, attempt - 1) if will_retry else 0.0
                if will_retry:
                    self.retry_delays.append(delay)
                self._remember(classified, attempt, delay, final=not will_retry)
                if will_retry:
                    logger.warning(
                        "Ошибка %s на %s, попытка %d, следующая через %.3f с",
                        type(classified).__name__,
                        classified.url or "?",
                        attempt,
                        delay,
                    )
                else:
                    logger.warning(
                        "Ошибка %s на %s, попытка %d, повтора не будет",
                        type(classified).__name__,
                        classified.url or "?",
                        attempt,
                    )
                    classified.attempts = attempt
                    if classified is exc:
                        raise
                    raise classified from exc
            else:
                if attempt > 1:
                    self.successful_retries += 1
                    logger.info(
                        "Повтор успешен для %s с попытки %d",
                        url or "?",
                        attempt,
                    )
                return result
            finally:
                _attempt_number.reset(token)
            if delay > 0:
                await asyncio.sleep(delay)

    def delay_for(self, exc: CrawlerError, retry_index: int) -> float:
        """Пауза перед повтором. ``retry_index`` начинается с нуля."""
        factor = self.backoff_factor
        for kind, value in self.backoff_factors.items():
            if isinstance(exc, kind):
                factor = value
                break
        delay = self.base_delay * (factor ** max(0, retry_index))
        status = getattr(exc, "status", None)
        if status in self.status_delay_multiplier:
            delay *= self.status_delay_multiplier[status]
        return min(self.max_delay, delay)

    def timeout_scale(self, attempt: int) -> float:
        """Во сколько раз вырастают таймауты на этой попытке. Первая — без роста."""
        return self.timeout_factor ** max(0, attempt - 1)

    def stats(self) -> dict:
        """Сводка: ошибки по классам, удачные повторы, средняя пауза, постоянные адреса."""
        count = len(self.retry_delays)
        average = sum(self.retry_delays) / count if count else 0.0
        return {
            "errors_by_type": dict(self.errors_by_type),
            "successful_retries": self.successful_retries,
            "average_retry_time": average,
            "permanent_urls": list(self.permanent_urls),
        }

    def reset_stats(self) -> None:
        """Очищает накопленную статистику, не меняя правила повторов."""
        self.errors_by_type.clear()
        self.successful_retries = 0
        self.retry_delays.clear()
        self.permanent_urls.clear()
        self.events.clear()

    def record_terminal(self, exc: CrawlerError) -> None:
        """Записывает ошибку, которую повторять не будут. Например ошибку разбора."""
        self.errors_by_type[type(exc).__name__] += 1
        self._remember(exc, attempt=1, delay=0.0, final=True)
        logger.warning(
            "Ошибка %s на %s, попытка 1, повтора не будет",
            type(exc).__name__,
            exc.url or "?",
        )

    def _can_retry(self, exc: CrawlerError, failures: int) -> bool:
        if not isinstance(exc, self.retry_on):
            return False
        return failures - 1 < self._retries_allowed(exc)

    def _retries_allowed(self, exc: CrawlerError) -> int:
        status = getattr(exc, "status", None)
        if status is not None and status in self.status_retry_limits:
            return self.status_retry_limits[status]
        for kind, limit in self.retry_limits.items():
            if isinstance(exc, kind):
                return limit
        return 0

    def _remember(self, exc: CrawlerError, attempt: int, delay: float, *, final: bool) -> None:
        if (
            final
            and isinstance(exc, PermanentError)
            and exc.url
            and exc.url not in self.permanent_urls
        ):
            self.permanent_urls.append(exc.url)
        self.events.append(
            {
                "url": exc.url,
                "error_type": type(exc).__name__,
                "status": exc.status,
                "attempt": attempt,
                "delay": delay,
                "final": final,
                "message": str(exc),
            }
        )


class CircuitBreaker:
    """Закрывает домен, если ошибки сыпятся слишком часто.

    После ``failure_threshold`` временных или сетевых ошибок домен молчит
    ``recovery_timeout`` секунд. Затем один пробный запрос либо открывает
    его снова, либо закрывает ещё на такой же срок.
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold должен быть >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout не может быть отрицательным")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._states: dict[str, _DomainCircuit] = {}
        self._lock = asyncio.Lock()

    async def check(self, domain: str, url: str = "") -> None:
        """Пропускает запрос или поднимает ``CircuitOpenError``, если домен закрыт."""
        async with self._lock:
            state = self._states.setdefault(domain, _DomainCircuit())
            now = time.monotonic()
            if state.status == "open":
                if now - state.opened_at < self.recovery_timeout:
                    raise CircuitOpenError(f"домен {domain} временно закрыт", url=url)
                state.status = "half_open"
                state.probe = False
                logger.info("Домен %s снова допускается к пробному запросу", domain)
            if state.status == "half_open":
                if state.probe:
                    raise CircuitOpenError(f"домен {domain} ждёт результат пробы", url=url)
                state.probe = True

    async def record_failure(self, domain: str) -> None:
        """Учитывает временную или сетевую ошибку и при пороге закрывает домен."""
        async with self._lock:
            state = self._states.setdefault(domain, _DomainCircuit())
            if state.status == "half_open":
                state.status = "open"
                state.opened_at = time.monotonic()
                state.probe = False
                state.failures = self.failure_threshold
                logger.warning(
                    "Пробный запрос к %s не удался, домен снова закрыт на %.1f с",
                    domain,
                    self.recovery_timeout,
                )
                return
            state.failures += 1
            if state.failures >= self.failure_threshold and state.status != "open":
                state.status = "open"
                state.opened_at = time.monotonic()
                state.probe = False
                logger.warning(
                    "Домен %s закрыт на %.1f с после %d ошибок",
                    domain,
                    self.recovery_timeout,
                    state.failures,
                )

    async def record_success(self, domain: str) -> None:
        """Успешный ответ снимает накопленные ошибки и открывает домен."""
        async with self._lock:
            state = self._states.setdefault(domain, _DomainCircuit())
            state.failures = 0
            state.status = "closed"
            state.probe = False


@dataclass
class _DomainCircuit:
    failures: int = 0
    status: str = "closed"
    opened_at: float = 0.0
    probe: bool = False


def _call_url(args: tuple) -> str:
    if args and isinstance(args[0], str):
        return args[0]
    return ""


def _bucket(exc: CrawlerError, status_limits: dict) -> str:
    if exc.status is not None and exc.status in status_limits:
        return f"status:{exc.status}"
    return type(exc).__name__

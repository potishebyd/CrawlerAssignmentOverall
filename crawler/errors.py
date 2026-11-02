"""Типы ошибок загрузки и разбора страниц (день 5)."""

from __future__ import annotations


class CrawlerError(Exception):
    """Базовая ошибка краулера.

    ``url`` — адрес, на котором она случилась. ``status`` — код HTTP,
    если ошибка пришла из ответа сервера. ``attempts`` заполняет стратегия
    повторов: сколько раз адрес уже пробовали.
    """

    def __init__(self, message: str = "", *, url: str = "", status: int | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status = status
        self.attempts = 1


class TransientError(CrawlerError):
    """Временный сбой: таймаут, 429, 503 или 500.

    Такой адрес имеет смысл запросить ещё раз.
    """


class PermanentError(CrawlerError):
    """Постоянный отказ: 401, 403, 404 и прочие 4xx.

    Повтор того же адреса даст тот же ответ, поэтому его не повторяют.
    """


class NetworkError(CrawlerError):
    """Сеть не пустила запрос: отказ соединения, DNS, обрыв."""


class ParseError(CrawlerError):
    """Страница скачалась, но разобрать её не удалось."""


class CircuitOpenError(CrawlerError):
    """Домен на время закрыт: ошибок подряд стало слишком много.

    Пока пауза не вышла, новые запросы к нему не отправляются.
    """


def classify_http_status(status: int, url: str = "") -> CrawlerError:
    """Превращает код HTTP в ошибку нужного класса.

    429, 408 и ответы 5xx — временные. Остальные 4xx — постоянные.
    """
    if status in (408, 429) or status >= 500:
        return TransientError(f"HTTP {status}", url=url, status=status)
    if status >= 400:
        return PermanentError(f"HTTP {status}", url=url, status=status)
    return PermanentError(f"HTTP {status}", url=url, status=status)

"""Разбор HTML и извлечение структурированных данных (день 2)."""

from __future__ import annotations

import logging
from urllib.parse import urldefrag, urljoin, urlsplit

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HTTP_SCHEMES = {"http", "https"}


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _empty_result(url: str) -> dict:
    return {
        "url": url,
        "title": "",
        "text": "",
        "links": [],
        "metadata": {"title": "", "description": "", "keywords": []},
        "images": [],
        "headings": [],
        "tables": [],
        "lists": [],
    }


def _is_http_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme in _HTTP_SCHEMES and bool(parsed.netloc)


class HTMLParser:
    """Парсер HTML на BeautifulSoup и lxml.

    Некорректная разметка не прерывает разбор: метод возвращает то,
    что удалось извлечь, и пишет предупреждение в лог.
    """

    def __init__(self, *, same_host_only: bool = False) -> None:
        """Создаёт парсер.

        :param same_host_only: оставлять только ссылки на тот же хост,
            что и у разбираемой страницы.
        """
        self.same_host_only = same_host_only

    async def parse_html(self, html: str, url: str) -> dict:
        """Разбирает HTML и возвращает словарь с извлечёнными данными."""
        result = _empty_result(url)
        if not html or not html.strip():
            logger.warning("Пустой HTML, разбирать нечего: %s", url)
            return result

        soup = self._make_soup(html, url)
        if soup is None:
            return result

        if "</html>" not in html.lower() and "</body>" not in html.lower():
            logger.warning("HTML выглядит неполным, верну частичный разбор: %s", url)

        steps = (
            ("metadata", lambda: self.extract_metadata(soup)),
            ("text", lambda: self.extract_text(soup)),
            ("links", lambda: self.extract_links(soup, url)),
            ("images", lambda: self.extract_images(soup, url)),
            ("headings", lambda: self.extract_headings(soup)),
            ("tables", lambda: self.extract_tables(soup)),
            ("lists", lambda: self.extract_lists(soup)),
        )
        for name, extract in steps:
            try:
                result[name] = extract()
            except Exception as exc:
                logger.warning(
                    "Ошибка извлечения %s на %s: %s. Остальные поля сохранены.",
                    name,
                    url,
                    type(exc).__name__,
                )

        result["title"] = result["metadata"].get("title", "")
        logger.info(
            "Страница разобрана: %s (заголовок %r, ссылок %d, символов %d)",
            url,
            result["title"],
            len(result["links"]),
            len(result["text"]),
        )
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Возвращает абсолютные http(s)-ссылки со страницы, без повторов."""
        base_host = urlsplit(base_url).netloc.lower()
        links: list[str] = []
        seen: set[str] = set()
        for tag in soup.find_all("a", href=True):
            href = str(tag.get("href", "")).strip()
            if not href or href.startswith("#"):
                continue
            try:
                absolute = urldefrag(urljoin(base_url, href)).url
            except ValueError as exc:
                logger.warning(
                    "Не удалось собрать ссылку %r на %s: %s",
                    href,
                    base_url,
                    type(exc).__name__,
                )
                continue
            if not _is_http_url(absolute):
                logger.warning("Пропущена некорректная ссылка %r на %s", href, base_url)
                continue
            if self.same_host_only and urlsplit(absolute).netloc.lower() != base_host:
                continue
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        """Возвращает текст страницы или элементов, найденных CSS-селектором.

        Скрипты и стили в общий текст не попадают.
        """
        if selector:
            try:
                nodes = soup.select(selector)
            except Exception as exc:
                logger.warning(
                    "Некорректный селектор %r: %s",
                    selector,
                    type(exc).__name__,
                )
                return ""
            if not nodes:
                logger.warning("Селектор %r ничего не нашёл", selector)
                return ""
            return _clean_text(" ".join(node.get_text(" ", strip=True) for node in nodes))

        clone = BeautifulSoup(str(soup), "lxml")
        for tag in clone.find_all(["script", "style", "noscript", "template"]):
            tag.decompose()
        root = clone.body or clone
        return _clean_text(root.get_text(" ", strip=True))

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Возвращает title, description и keywords."""
        if soup.title is not None:
            title = _clean_text(soup.title.get_text(" ", strip=True))
        else:
            title = ""
        keywords_raw = _meta_content(soup, "keywords")
        keywords = [_clean_text(part) for part in keywords_raw.split(",") if part.strip()]
        return {
            "title": title,
            "description": _meta_content(soup, "description"),
            "keywords": keywords,
        }

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
        """Возвращает изображения: абсолютный src и alt."""
        images: list[dict[str, str]] = []
        seen: set[str] = set()
        for tag in soup.find_all("img"):
            src = str(tag.get("src", "")).strip()
            if not src:
                continue
            try:
                absolute = urldefrag(urljoin(base_url, src)).url
            except ValueError as exc:
                logger.warning(
                    "Не удалось собрать адрес изображения %r на %s: %s",
                    src,
                    base_url,
                    type(exc).__name__,
                )
                continue
            if not _is_http_url(absolute) or absolute in seen:
                if not _is_http_url(absolute):
                    logger.warning(
                        "Пропущен некорректный адрес изображения %r на %s",
                        src,
                        base_url,
                    )
                continue
            seen.add(absolute)
            images.append({"src": absolute, "alt": _clean_text(str(tag.get("alt") or ""))})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict[str, str]]:
        """Возвращает заголовки h1, h2 и h3 в порядке документа."""
        headings: list[dict[str, str]] = []
        for tag in soup.find_all(["h1", "h2", "h3"]):
            text = _clean_text(tag.get_text(" ", strip=True))
            if text:
                headings.append({"level": tag.name, "text": text})
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        """Возвращает таблицы: список таблиц, каждая — список строк из ячеек."""
        tables: list[list[list[str]]] = []
        for table in soup.find_all("table"):
            rows: list[list[str]] = []
            for tr in table.find_all("tr"):
                if tr.find_parent("table") is not table:
                    continue
                cells = [
                    _clean_text(cell.get_text(" ", strip=True))
                    for cell in tr.find_all(["th", "td"], recursive=False)
                ]
                if any(cells):
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        """Возвращает списки ul и ol: тип и тексты непосредственных пунктов."""
        lists: list[dict] = []
        for tag in soup.find_all(["ul", "ol"]):
            items: list[str] = []
            for item in tag.find_all("li", recursive=False):
                text = _list_item_text(item)
                if text:
                    items.append(text)
            if items:
                lists.append({"type": tag.name, "items": items})
        return lists

    def _make_soup(self, html: str, url: str) -> BeautifulSoup | None:
        for parser_name in ("lxml", "html.parser"):
            try:
                return BeautifulSoup(html, parser_name)
            except Exception as exc:
                logger.warning(
                    "Парсер %s не разобрал %s: %s",
                    parser_name,
                    url,
                    type(exc).__name__,
                )
        return None


def _meta_content(soup: BeautifulSoup, key: str) -> str:
    wanted = key.lower()
    for tag in soup.find_all("meta"):
        name = str(tag.get("name") or "").lower()
        if name == wanted:
            return _clean_text(str(tag.get("content") or ""))
    for tag in soup.find_all("meta"):
        prop = str(tag.get("property") or "").lower()
        if prop == f"og:{wanted}":
            return _clean_text(str(tag.get("content") or ""))
    return ""


def _list_item_text(item: BeautifulSoup) -> str:
    parts: list[str] = []
    for child in item.children:
        name = getattr(child, "name", None)
        if name in {"ul", "ol"}:
            continue
        if hasattr(child, "get_text"):
            text = child.get_text(" ", strip=True)
        else:
            text = str(child).strip()
        if text:
            parts.append(text)
    return _clean_text(" ".join(parts))

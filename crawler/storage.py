"""Асинхронное сохранение страниц в JSON, CSV и SQLite (день 6)."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import aiofiles

logger = logging.getLogger(__name__)

RECORD_FIELDS = (
    "url",
    "title",
    "text",
    "links",
    "metadata",
    "crawled_at",
    "status_code",
    "content_type",
)


def build_record(
    data: dict,
    *,
    status_code: int = 0,
    content_type: str = "",
    crawled_at: datetime | None = None,
) -> dict:
    """Собирает словарь страницы в общем для всех хранилищ виде.

    Ссылки и метаданные копируются, чтобы дальнейшие правки исходного
    разбора не меняли уже поставленную в буфер запись. ``crawled_at``
    остаётся объектом ``datetime`` до момента записи на диск.
    """
    moment = data.get("crawled_at", crawled_at) or datetime.now(timezone.utc)
    if not isinstance(moment, datetime):
        moment = datetime.fromisoformat(str(moment))
    links = data.get("links") or []
    metadata = data.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {"value": metadata}
    return {
        "url": str(data.get("url") or ""),
        "title": str(data.get("title") or ""),
        "text": str(data.get("text") or ""),
        "links": [str(item) for item in links],
        "metadata": dict(metadata),
        "crawled_at": moment,
        "status_code": int(data.get("status_code", status_code) or 0),
        "content_type": str(data.get("content_type", content_type) or ""),
    }


def _export(record: dict) -> dict:
    """Готовит запись к JSON, CSV и SQL: дату переводит в текст ISO."""
    exported = dict(record)
    moment = exported.get("crawled_at")
    if isinstance(moment, datetime):
        exported["crawled_at"] = moment.isoformat()
    return exported


def _restore(record: dict) -> dict:
    """Возвращает дату и вложенные поля после чтения с диска."""
    restored = dict(record)
    moment = restored.get("crawled_at")
    if isinstance(moment, str) and moment:
        restored["crawled_at"] = datetime.fromisoformat(moment)
    status = restored.get("status_code")
    if isinstance(status, str) and status:
        restored["status_code"] = int(status)
    for key in ("links", "metadata"):
        value = restored.get(key)
        if isinstance(value, str) and value:
            restored[key] = json.loads(value)
    return restored


class DataStorage(ABC):
    """Общий буфер записи. Наследники только выкладывают пачку на диск.

    ``save`` не выпускает ошибку наружу: сбой записи пишется в лог, пачка
    повторяется, а обход страниц продолжается.
    """

    def __init__(
        self,
        *,
        batch_size: int = 1,
        save_retries: int = 2,
        retry_delay: float = 0.05,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size должен быть >= 1")
        if save_retries < 0:
            raise ValueError("save_retries должен быть >= 0")
        if retry_delay < 0:
            raise ValueError("retry_delay не может быть отрицательным")
        self.batch_size = batch_size
        self.save_retries = save_retries
        self.retry_delay = retry_delay
        self.saved_count = 0
        self.error_count = 0
        self.write_batches = 0
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._closed = False

    async def save(self, data: dict) -> None:
        """Кладёт страницу в буфер и сбрасывает его, когда пачка набралась."""
        if self._closed:
            logger.warning("Хранилище закрыто, запись %s пропущена", data.get("url", ""))
            return
        record = build_record(data)
        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.batch_size:
                await self._flush_locked()

    async def flush(self) -> None:
        """Записывает то, что ещё лежит в буфере."""
        async with self._lock:
            await self._flush_locked()

    async def close(self) -> None:
        """Сбрасывает буфер. Повторный вызов ничего не делает."""
        await self.flush()
        self._closed = True

    def stats(self) -> dict:
        """Сколько записей легло на диск, сколько пачек не удалось и сколько ждёт."""
        return {
            "saved": self.saved_count,
            "errors": self.error_count,
            "pending": len(self._buffer),
            "batches": self.write_batches,
        }

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        delay = self.retry_delay
        for attempt in range(self.save_retries + 1):
            try:
                await self._write_batch(batch)
            except Exception as exc:
                logger.warning(
                    "Ошибка сохранения, попытка %d/%d: %s",
                    attempt + 1,
                    self.save_retries + 1,
                    type(exc).__name__,
                )
                if attempt >= self.save_retries:
                    self.error_count += len(batch)
                    logger.error(
                        "Не удалось записать %d шт. Обход продолжается.",
                        len(batch),
                    )
                    return
                if delay > 0:
                    await asyncio.sleep(delay)
                    delay *= 2
                continue
            self.saved_count += len(batch)
            self.write_batches += 1
            logger.info("Сохранена пачка из %d страниц", len(batch))
            return

    @abstractmethod
    async def _write_batch(self, batch: list[dict]) -> None:
        """Записывает уже собранную пачку. Ошибку можно поднять: её повторят."""

    async def load(self) -> list[dict]:
        """Читает всё, что хранилище уже записало."""
        raise NotImplementedError


class JSONStorage(DataStorage):
    """Пишет страницы в JSON через aiofiles.

    Без ``indent`` каждая страница — отдельная строка файла. Так файл можно
    дополнять пачками и не читать его целиком. С ``indent`` файл становится
    одним отформатированным массивом: его удобно открыть глазами, но на
    большой обход лучше оставить построчный режим.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        indent: int | None = None,
        encoding: str = "utf-8",
        overwrite: bool = False,
        batch_size: int = 1,
        save_retries: int = 2,
        retry_delay: float = 0.05,
    ) -> None:
        super().__init__(
            batch_size=batch_size,
            save_retries=save_retries,
            retry_delay=retry_delay,
        )
        if indent is not None and indent < 0:
            raise ValueError("indent не может быть отрицательным")
        self.path = Path(path)
        self.indent = indent
        self.encoding = encoding
        self._records: list[dict] = []
        if overwrite and self.path.exists():
            self.path.unlink()

    async def _write_batch(self, batch: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.indent is None:
            payload = "".join(
                json.dumps(_export(row), ensure_ascii=False) + "\n" for row in batch
            )
            async with aiofiles.open(self.path, "a", encoding=self.encoding) as handle:
                await handle.write(payload)
            return
        updated = self._records + [_export(row) for row in batch]
        text = json.dumps(updated, ensure_ascii=False, indent=self.indent)
        async with aiofiles.open(self.path, "w", encoding=self.encoding) as handle:
            await handle.write(text)
            await handle.write("\n")
        self._records = updated

    async def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        async with aiofiles.open(self.path, encoding=self.encoding) as handle:
            raw = await handle.read()
        raw = raw.strip()
        if not raw:
            return []
        if self.indent is None:
            rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
        else:
            parsed = json.loads(raw)
            rows = parsed if isinstance(parsed, list) else [parsed]
        return [_restore(row) for row in rows]


class CSVStorage(DataStorage):
    """Дописывает страницы в CSV. Заголовки берутся из первой записи.

    Запятые, кавычки и переводы строк отдаются модулю ``csv``: он сам
    заключает такие поля в кавычки. На диск строка уходит через aiofiles.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        encoding: str = "utf-8",
        overwrite: bool = False,
        batch_size: int = 1,
        save_retries: int = 2,
        retry_delay: float = 0.05,
    ) -> None:
        super().__init__(
            batch_size=batch_size,
            save_retries=save_retries,
            retry_delay=retry_delay,
        )
        self.path = Path(path)
        self.encoding = encoding
        self._fields: list[str] = []
        self._header_written = False
        if overwrite and self.path.exists():
            self.path.unlink()

    async def _write_batch(self, batch: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self._fields:
            self._fields = _csv_fields(batch[0])
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(
            buffer,
            fieldnames=self._fields,
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        if not self._header_written:
            writer.writeheader()
        for row in batch:
            exported = _export(row)
            exported["links"] = json.dumps(exported["links"], ensure_ascii=False)
            exported["metadata"] = json.dumps(exported["metadata"], ensure_ascii=False)
            writer.writerow(exported)
        async with aiofiles.open(self.path, "a", encoding=self.encoding, newline="") as handle:
            await handle.write(buffer.getvalue())
        self._header_written = True

    async def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        async with aiofiles.open(self.path, encoding=self.encoding, newline="") as handle:
            raw = await handle.read()
        if not raw.strip():
            return []
        reader = csv.DictReader(io.StringIO(raw, newline=""))
        return [_restore(row) for row in reader]


class SQLiteStorage(DataStorage):
    """Кладет страницы в SQLite пачками через aiosqlite.

    Адрес — первичный ключ, поэтому повторный обход обновляет строку, а не
    плодит копии. По коду ответа и времени обхода есть индексы.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        overwrite: bool = False,
        batch_size: int = 20,
        save_retries: int = 2,
        retry_delay: float = 0.05,
    ) -> None:
        super().__init__(
            batch_size=batch_size,
            save_retries=save_retries,
            retry_delay=retry_delay,
        )
        self.path = Path(path)
        self._db = None
        if overwrite and self.path.exists():
            self.path.unlink()

    async def init_db(self) -> None:
        """Создаёт таблицу страниц и индексы, если их ещё нет."""
        await self._ensure_db()

    async def close(self) -> None:
        await self.flush()
        if self._db is not None:
            await self._db.close()
            self._db = None
        self._closed = True

    async def _ensure_db(self):
        if self._db is not None:
            return self._db
        import aiosqlite

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                links TEXT NOT NULL,
                metadata TEXT NOT NULL,
                crawled_at TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                content_type TEXT NOT NULL
            )
            """
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_status_code ON pages(status_code)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at)"
        )
        await self._db.commit()
        return self._db

    async def _write_batch(self, batch: list[dict]) -> None:
        db = await self._ensure_db()
        rows = [_sql_row(row) for row in batch]
        await db.executemany(
            """
            INSERT INTO pages (
                url, title, text, links, metadata, crawled_at, status_code, content_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title = excluded.title,
                text = excluded.text,
                links = excluded.links,
                metadata = excluded.metadata,
                crawled_at = excluded.crawled_at,
                status_code = excluded.status_code,
                content_type = excluded.content_type
            """,
            rows,
        )
        await db.commit()

    async def load(self) -> list[dict]:
        import aiosqlite

        own = self._db is None
        db = self._db if self._db is not None else await aiosqlite.connect(self.path)
        try:
            cursor = await db.execute(
                """
                SELECT url, title, text, links, metadata, crawled_at, status_code, content_type
                FROM pages
                ORDER BY url
                """
            )
            fetched = await cursor.fetchall()
        finally:
            if own:
                await db.close()
        rows = []
        for url, title, text, links, metadata, crawled_at, status_code, content_type in fetched:
            rows.append(
                _restore(
                    {
                        "url": url,
                        "title": title,
                        "text": text,
                        "links": links,
                        "metadata": metadata,
                        "crawled_at": crawled_at,
                        "status_code": status_code,
                        "content_type": content_type,
                    }
                )
            )
        return rows

    async def index_names(self) -> list[str]:
        """Имена индексов таблицы страниц. Нужны, чтобы проверить схему."""
        import aiosqlite

        own = self._db is None
        db = self._db if self._db is not None else await aiosqlite.connect(self.path)
        try:
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'pages'"
            )
            fetched = await cursor.fetchall()
        finally:
            if own:
                await db.close()
        return sorted(name for (name,) in fetched if name)


def _csv_fields(record: dict) -> list[str]:
    extras = [key for key in record if key not in RECORD_FIELDS]
    return [key for key in RECORD_FIELDS if key in record] + extras


def _sql_row(record: dict) -> tuple:
    exported = _export(record)
    return (
        exported["url"],
        exported["title"],
        exported["text"],
        json.dumps(exported["links"], ensure_ascii=False),
        json.dumps(exported["metadata"], ensure_ascii=False),
        exported["crawled_at"],
        int(exported["status_code"]),
        exported["content_type"],
    )

"""Журнал в консоль и в файл с ротацией (день 7)."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(
    *,
    level: str = "INFO",
    log_file: str | Path | None = None,
    max_bytes: int = 1_000_000,
    backup_count: int = 3,
    logger_name: str = "",
) -> logging.Logger:
    """Настраивает журнал: консоль и, если задан путь, файл с ротацией.

    Пустой ``logger_name`` настраивает корневой журнал. Имя ограничивает
    настройку одним журналом и не трогает остальные.
    """
    resolved = getattr(logging, str(level).upper(), None)
    if not isinstance(resolved, int):
        raise ValueError(f"Неизвестный уровень логирования: {level}")
    target = logging.getLogger(logger_name)
    target.handlers.clear()
    target.setLevel(resolved)
    target.propagate = logger_name == ""
    formatter = logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    target.addHandler(console)
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        rotating = RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        rotating.setFormatter(formatter)
        target.addHandler(rotating)
    return target

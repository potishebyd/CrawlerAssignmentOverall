"""Чтение настроек краулера из YAML или JSON (день 7)."""

from __future__ import annotations

import json
from pathlib import Path


def load_config(path: str | Path) -> dict:
    """Читает словарь настроек. Формат выбирается по расширению файла."""
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    suffix = file.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        import yaml

        data = yaml.safe_load(text) or {}
    elif suffix == ".json":
        data = json.loads(text or "{}")
    else:
        raise ValueError("Конфигурация должна быть .yaml, .yml или .json")
    if not isinstance(data, dict):
        raise ValueError("В корне конфигурации должен быть словарь")
    return data


def config_to_kwargs(data: dict) -> dict:
    """Переводит ключи файла в аргументы ``AdvancedCrawler``."""
    start_urls = list(data.get("start_urls") or [])
    if not start_urls:
        raise ValueError("В конфигурации нужен непустой start_urls")
    storage_cfg = data.get("storage") or {}
    if not isinstance(storage_cfg, dict):
        storage_cfg = {}
    output = data.get("output") or storage_cfg.get("path")
    rate = data.get("requests_per_second", data.get("rate_limit"))
    retry_cfg = data.get("retry") or {}
    if not isinstance(retry_cfg, dict):
        retry_cfg = {}
    return {
        "start_urls": [str(url) for url in start_urls],
        "max_pages": int(data.get("max_pages", 100)),
        "max_depth": int(data.get("max_depth", 1)),
        "max_concurrent": int(data.get("max_concurrent", 10)),
        "requests_per_second": None if rate in (None, "") else float(rate),
        "respect_robots": bool(data.get("respect_robots", False)),
        "same_domain_only": bool(data.get("same_domain_only", True)),
        "use_sitemap": bool(data.get("use_sitemap", False)),
        "sitemaps": [str(url) for url in data.get("sitemaps") or []],
        "exclude_patterns": list(data.get("exclude_patterns") or []),
        "include_patterns": list(data.get("include_patterns") or []),
        "user_agent": data.get("user_agent"),
        "output": None if output in (None, "") else str(output),
        "storage_format": storage_cfg.get("format"),
        "batch_size": int(storage_cfg.get("batch_size", 20)),
        "log_file": data.get("log_file"),
        "log_level": str(data.get("log_level", "INFO")),
        "show_progress": bool(data.get("show_progress", True)),
        "min_delay": float(data.get("min_delay", 0.0)),
        "max_retries": int(data.get("max_retries", retry_cfg.get("max_retries", 3))),
        "retry_base_delay": float(data.get("retry_base_delay", retry_cfg.get("base_delay", 0.25))),
        "retry_backoff_factor": float(
            data.get("retry_backoff_factor", retry_cfg.get("backoff_factor", 2.0))
        ),
        "retry_max_delay": float(data.get("retry_max_delay", retry_cfg.get("max_delay", 30.0))),
    }

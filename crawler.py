"""Запуск из корня проекта: python crawler.py --urls https://example.com

Файл называется так же, как пакет ``crawler``. Поэтому пакет загружается
явно из каталога, а не через обычный import, который нашёл бы этот скрипт.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_package():
    root = Path(__file__).resolve().parent
    package_dir = root / "crawler"
    init = package_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "crawler",
        init,
        submodule_search_locations=[str(package_dir)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Не найден пакет crawler")
    module = importlib.util.module_from_spec(spec)
    sys.modules["crawler"] = module
    spec.loader.exec_module(module)
    return module


if __name__ == "__main__":
    _load_package()
    from crawler.cli import main as cli_main

    raise SystemExit(cli_main())

# Скрипт лежит рядом с пакетом и перехватывает `import crawler`.
# Подменяем его настоящим пакетом, чтобы `from crawler import ...` работал.
_load_package()

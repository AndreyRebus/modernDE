# mybot/splash.py
from __future__ import annotations
import json
import random
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from .config import SPLASH_DIR

def _norm(name: str) -> str:
    # нормализуем: без пробелов/подчёркиваний/дефисов, в нижний регистр
    return re.sub(r"[\s_\-]+", "", name).lower()

@lru_cache(maxsize=1)
def _manifest_map() -> Dict[str, List[str]]:
    """Читает manifest.json один раз и строит индекс champ->список абсолютных путей."""
    path = SPLASH_DIR / "manifest.json"
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # нормализуем ключи и превращаем относительные пути в абсолютные (внутри SPLASH_DIR)
    idx: Dict[str, List[str]] = {}
    for champion, files in (raw or {}).items():
        if not isinstance(files, list):
            continue
        norm_key = _norm(str(champion))
        abs_files = [str((SPLASH_DIR / fn).resolve()) for fn in files]
        idx[norm_key] = abs_files
    return idx

def pick_random_splash(champion: str) -> Optional[str]:
    """O(1): берём заранее подготовленный список и выбираем случайный файл."""
    files = _manifest_map().get(_norm(champion))
    if not files:
        return None
    return random.choice(files)

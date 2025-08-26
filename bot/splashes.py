import io
import json
import os
import pathlib
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- настройки ----------
OUT = pathlib.Path("data/splashes")
OUT.mkdir(parents=True, exist_ok=True)

# Локаль (en_US — безопасно: ключи чемпионов и имена совпадают с файлами)
LOCALE = "en_US"

# Как писать пути в index.json:
#   - "relative" (по умолчанию): только имена файлов, например "Aatrox_0.jpg"
#   - "absolute": абсолютные пути с префиксом MOUNT_PREFIX (для контейнера)
INDEX_MODE = os.getenv("SPLASH_INDEX_MODE", "relative").strip().lower()  # relative | absolute
MOUNT_PREFIX = os.getenv("SPLASH_MOUNT_PREFIX", "/app/data/splashes").rstrip("/")

# Паттерн безопасных имён для удаления «осиротевших» файлов
SAFE_DELETE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]+_\d+\.jpg$")

TIMEOUT = 20
MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10 МБ
# -------------------------------

def to_index_entry(local_file: pathlib.Path) -> str:
    """Как представлять путь в index.json."""
    name = local_file.name
    if INDEX_MODE == "absolute":
        return f"{MOUNT_PREFIX}/{name}"
    return name  # relative

def get_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "lol-splash-sync/1.0"})
    return s

def latest_version(session: requests.Session) -> str:
    r = session.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()[0]

def load_champions(session: requests.Session, ver: str) -> dict:
    r = session.get(
        f"https://ddragon.leagueoflegends.com/cdn/{ver}/data/{LOCALE}/champion.json",
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["data"]

def load_champion_detail(session: requests.Session, ver: str, champ_key: str) -> dict:
    r = session.get(
        f"https://ddragon.leagueoflegends.com/cdn/{ver}/data/{LOCALE}/champion/{champ_key}.json",
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["data"][champ_key]

def splash_url(champ_key: str, skin_num: int) -> str:
    return f"https://ddragon.leagueoflegends.com/cdn/img/champion/splash/{champ_key}_{skin_num}.jpg"

# ---------- Метаданные исходника (ETag/Last-Modified) ----------
def meta_path(fp: pathlib.Path) -> pathlib.Path:
    return fp.with_name(fp.name + ".meta")

def read_meta(fp: pathlib.Path) -> dict | None:
    p = meta_path(fp)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def write_meta(fp: pathlib.Path, headers: requests.structures.CaseInsensitiveDict, fallback_len: int) -> None:
    data = {
        "etag": headers.get("ETag"),
        "content_length": int(headers.get("Content-Length") or fallback_len or 0),
        "last_modified": headers.get("Last-Modified"),
    }
    meta_path(fp).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def needs_update(session: requests.Session, file_path: pathlib.Path, url: str) -> bool:
    if not file_path.exists():
        return True

    local_meta = read_meta(file_path)

    try:
        h = session.head(url, timeout=TIMEOUT)
        if h.status_code == 200:
            etag = h.headers.get("ETag")
            clen = h.headers.get("Content-Length")
            last_mod = h.headers.get("Last-Modified")

            if local_meta:
                if etag and etag == local_meta.get("etag"):
                    return False
                if clen and int(clen) == int(local_meta.get("content_length") or -1):
                    return False
                if last_mod and last_mod == local_meta.get("last_modified"):
                    return False
            return True
    except requests.RequestException:
        pass
    return False
# ----------------------------------------------------------------

# ---------- Сжатие JPEG ----------
def compress_jpeg_to_max_bytes(data: bytes, max_bytes: int = MAX_IMAGE_BYTES) -> tuple[bytes, bool]:
    if len(data) <= max_bytes:
        return data, False

    try:
        from PIL import Image
    except ImportError as e:
        raise RuntimeError("Для сжатия требуется Pillow: pip install pillow") from e

    img = Image.open(io.BytesIO(data))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    def save_with_quality(im, q: int) -> bytes:
        buf = io.BytesIO()
        im.save(
            buf,
            format="JPEG",
            quality=int(q),
            optimize=True,
            progressive=True,
            subsampling="4:2:0",
        )
        return buf.getvalue()

    # 1) Бинарный поиск по качеству
    low, high = 40, 95
    best = None
    while low <= high:
        mid = (low + high) // 2
        candidate = save_with_quality(img, mid)
        if len(candidate) <= max_bytes:
            best = candidate
            low = mid + 1
        else:
            high = mid - 1

    if best:
        return best, True

    # 2) Уменьшение размеров + повторный поиск
    from math import sqrt
    current = img
    ratio_guess = sqrt(max_bytes / len(data)) * 0.98
    if ratio_guess >= 1.0:
        ratio_guess = 0.95
    for _ in range(10):
        w, h = current.size
        nw, nh = max(1, int(w * ratio_guess)), max(1, int(h * ratio_guess))
        if (nw, nh) == current.size:
            nw, nh = max(1, w - 1), max(1, h - 1)
        current = current.resize((nw, nh), Image.LANCZOS)

        low, high = 40, 95
        best = None
        while low <= high:
            mid = (low + high) // 2
            candidate = save_with_quality(current, mid)
            if len(candidate) <= max_bytes:
                best = candidate
                low = mid + 1
            else:
                high = mid - 1
        if best:
            return best, True

        ratio_guess *= 0.9

    fallback = save_with_quality(current, 30)
    if len(fallback) > max_bytes:
        while len(fallback) > max_bytes and min(current.size) > 128:
            w, h = current.size
            current = current.resize((max(1, int(w * 0.85)), max(1, int(h * 0.85))), Image.LANCZOS)
            fallback = save_with_quality(current, 30)
    return fallback, True
# ----------------------------------------------------------------

def main():
    session = get_session()

    ver = latest_version(session)
    champs = load_champions(session, ver)

    # Набор ожидаемых файлов (локальные пути) и индекс {champ_key -> [entry,...]}
    expected_files: set[pathlib.Path] = set()
    index_dict: dict[str, list[str]] = {}

    print(f"Использую версию DDragon: {ver}")
    print(f"INDEX_MODE={INDEX_MODE}, MOUNT_PREFIX={MOUNT_PREFIX if INDEX_MODE=='absolute' else '(relative)'}")

    for champ_key in champs:  # champ_key: 'Aatrox', 'Ahri', ...
        detail = load_champion_detail(session, ver, champ_key)
        paths_for_champ: list[str] = []

        for skin in detail["skins"]:
            num = skin["num"]
            url = splash_url(champ_key, num)
            local = OUT / f"{champ_key}_{num}.jpg"

            if needs_update(session, local, url):
                existed = local.exists()
                try:
                    r = session.get(url, timeout=TIMEOUT)
                    if r.status_code == 200 and r.content:
                        data, compressed = compress_jpeg_to_max_bytes(r.content, MAX_IMAGE_BYTES)
                        local.write_bytes(data)
                        write_meta(local, r.headers, fallback_len=len(r.content))
                        size_mb = len(data) / (1024 * 1024)
                        action = "UPDATE" if existed else "ADD"
                        print(f"✔ {local.name} — {action}{' (сжато)' if compressed else ''}, {size_mb:.2f} МБ")
                    else:
                        print(f"✖ Не удалось скачать {url}: статус {r.status_code}")
                        continue
                except requests.RequestException as e:
                    print(f"✖ Не удалось скачать {url}: {e}")
                    continue

            expected_files.add(local.resolve())
            paths_for_champ.append(to_index_entry(local))

        # ключ — champ_key (стабильный, англ.)
        index_dict[champ_key] = paths_for_champ

    # Удаляем «осиротевшие» файлы
    deleted = 0
    for p in OUT.glob("*.jpg"):
        if p.resolve() not in expected_files and SAFE_DELETE_PATTERN.match(p.name):
            try:
                p.unlink()
                mp = meta_path(p)
                if mp.exists():
                    mp.unlink(missing_ok=True)
                deleted += 1
                print(f"🗑 Удалён устаревший файл: {p.name}")
            except OSError as e:
                print(f"⚠ Не удалось удалить {p}: {e}")

    # Сохраняем индекс (атомарно)
    index_path = OUT / "index.json"
    tmp_path = index_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(index_dict, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(index_path)

    print(f"\nГотово. Удалено: {deleted}. Индекс: {index_path}")
    if INDEX_MODE == "relative":
        print("Индекс содержит относительные пути (имена файлов). В боте объединяйте с SPLASH_DIR.")
    else:
        print(f"Индекс содержит абсолютные пути с префиксом: {MOUNT_PREFIX}")

if __name__ == "__main__":
    main()

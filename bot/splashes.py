import json
import pathlib
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- настройки ----------
OUT = pathlib.Path("data/splashes")
OUT.mkdir(parents=True, exist_ok=True)

# Локаль для имён чемпионов в словаре (ru_RU -> русские имена; en_US -> английские)
LOCALE = "en_US"

# Паттерн безопасных имён файлов (чтобы случайно не удалить чужие файлы)
SAFE_DELETE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]+_\d+\.jpg$")

TIMEOUT = 20
# -------------------------------

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
    # URL без версии — у сплэшей стабильный путь
    return f"https://ddragon.leagueoflegends.com/cdn/img/champion/splash/{champ_key}_{skin_num}.jpg"

def needs_update(session: requests.Session, file_path: pathlib.Path, url: str) -> bool:
    """Нужно ли скачать/обновить файл:
       - файла нет
       - либо отличается размер от Content-Length на CDN (быстрая проверка изменения)
    """
    if not file_path.exists():
        return True
    try:
        h = session.head(url, timeout=TIMEOUT)
        if h.status_code == 200:
            clen = h.headers.get("Content-Length")
            if clen is not None and file_path.stat().st_size != int(clen):
                return True
        # если HEAD недоступен или без Content-Length — считаем, что обновление не нужно
    except requests.RequestException:
        pass
    return False

def main():
    session = get_session()

    ver = latest_version(session)
    champs = load_champions(session, ver)

    # Ожидаемые файлы и словарь "Имя чемпиона -> список путей"
    expected_files = set()
    index_dict: dict[str, list[str]] = {}

    print(f"Использую версию DDragon: {ver}")

    for champ_key in champs:
        detail = load_champion_detail(session, ver, champ_key)
        champ_name = detail["name"]  # имя чемпиона по локали
        paths_for_champ = []

        for skin in detail["skins"]:
            num = skin["num"]
            url = splash_url(champ_key, num)
            fname = OUT / f"{champ_key}_{num}.jpg"

            # Скачать или обновить при необходимости
            if needs_update(session, fname, url):
                try:
                    r = session.get(url, timeout=TIMEOUT)
                    if r.status_code == 200 and r.content:
                        fname.write_bytes(r.content)
                        action = "ADD" if not fname.exists() else "UPDATE"  # (после записи exists() всегда True)
                        # но корректнее определить по наличию до скачивания:
                    print(f"✔ {fname.name} — скачано/обновлено")
                except requests.RequestException as e:
                    print(f"✖ Не удалось скачать {url}: {e}")
                    # пропускаем файл, но не добавляем в expected_files / index_dict
                    continue
            else:
                # уже актуально
                pass

            expected_files.add(fname.resolve())
            paths_for_champ.append(str(fname.resolve()))

        index_dict[champ_name] = paths_for_champ

    # Удаляем «осиротевшие» *.jpg, которых больше нет в актуальном списке
    deleted = 0
    for p in OUT.glob("*.jpg"):
        # удаляем только если файл НЕ ожидается и выглядит как наш сплэш
        if p.resolve() not in expected_files and SAFE_DELETE_PATTERN.match(p.name):
            try:
                p.unlink()
                deleted += 1
                print(f"🗑 Удалён устаревший файл: {p.name}")
            except OSError as e:
                print(f"⚠ Не удалось удалить {p}: {e}")

    # Сохраняем словарь (индекс)
    index_path = OUT / "index.json"
    index_path.write_text(json.dumps(index_dict, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nГотово. Удалено: {deleted}. Индекс: {index_path}")

if __name__ == "__main__":
    main()

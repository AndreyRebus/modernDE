import argparse
import json
import time
from collections import defaultdict
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TIMEOUT_JSON = 20
TIMEOUT_IMG = 30


def make_session():
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.headers.update({"User-Agent": "lol-splashes/1.0"})
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def get_json(s, url):
    r = s.get(url, timeout=TIMEOUT_JSON)
    r.raise_for_status()
    return r.json()


def download_image(s, url, dest: Path, force: bool):
    if dest.exists() and not force:
        return "exists"

    r = s.get(url, timeout=TIMEOUT_IMG, stream=True)
    if r.status_code == 404:
        return "notfound"
    r.raise_for_status()

    ctype = r.headers.get("Content-Type", "")
    if "image" not in ctype:
        raise RuntimeError(f"ожидали картинку, получили {ctype} ({url})")

    tmp = dest.with_suffix(dest.suffix + ".part")
    with tmp.open("wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    tmp.replace(dest)
    return "downloaded"


def build_manifest(out_dir: Path, absolute_paths: bool):
    manifest = defaultdict(list)
    kept_files = set()

    with make_session() as s:
        latest = get_json(s, "https://ddragon.leagueoflegends.com/api/versions.json")[0]

        champ_list = get_json(
            s, f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion.json"
        )["data"]

        for champ_key in champ_list:
            champ = get_json(
                s,
                f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion/{champ_key}.json",
            )["data"][champ_key]

            for skin in champ["skins"]:
                yield champ_key, skin, s, out_dir, absolute_paths, kept_files


def main():
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/data/splashes"),
        help="куда сохранять файлы (папка будет создана)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="перекачивать, даже если файл уже есть",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="пауза между скачиваниями, сек",
    )
    parser.add_argument(
        "--absolute",
        action="store_true",
        help="писать в manifest.json абсолютные пути (по умолчанию — относительные имена файлов)",
    )
    parser.add_argument(
        "--prune",
        action="store_true",
        help="удалить лишние .jpg, которых нет в актуальном манифесте",
    )
    args = parser.parse_args()

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = defaultdict(list)
    kept_files = set()

    try:
        with make_session() as s:
            latest = get_json(s, "https://ddragon.leagueoflegends.com/api/versions.json")[0]

            champ_list = get_json(
                s, f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion.json"
            )["data"]

            for champ_key in champ_list:
                champ = get_json(
                    s,
                    f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion/{champ_key}.json",
                )["data"][champ_key]

                for skin in champ["skins"]:
                    num = skin["num"]
                    url = f"https://ddragon.leagueoflegends.com/cdn/img/champion/splash/{champ_key}_{num}.jpg"
                    fname = out_dir / f"{champ_key}_{num}.jpg"

                    try:
                        status = download_image(s, url, fname, args.force)
                    except Exception as e:
                        print(f"✖ {fname.name}: ошибка — {e}")
                        continue

                    if status == "downloaded":
                        print(f"✔ скачано {fname.name}")
                        time.sleep(args.sleep)
                    elif status == "exists":
                        print(f"= уже есть {fname.name}")
                    elif status == "notfound":
                        print(f"⚠ нет на сервере: {fname.name}")
                        continue

                    kept_files.add(fname.resolve())
                    if args.absolute:
                        manifest[champ_key].append(str(fname.resolve()))
                    else:
                        manifest[champ_key].append(fname.name)

    except requests.RequestException as e:
        print(f"Сетевая ошибка: {e}")
        return

    manifest_path = out_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"Готово! Списки сплэш-артов сохранены в {manifest_path}")

    if args.prune:
        removed = 0
        for jpg in out_dir.glob("*.jpg"):
            if jpg.resolve() not in kept_files:
                try:
                    jpg.unlink()
                    removed += 1
                except Exception as e:
                    print(f"Не удалось удалить {jpg.name}: {e}")
        if removed:
            print(f"Удалено лишних файлов: {removed}")
        else:
            print("Лишних файлов не найдено.")


if __name__ == "__main__":
    main()
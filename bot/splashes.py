import requests, pathlib, json
from collections import defaultdict

OUT = pathlib.Path("data/splashes")
OUT.mkdir(parents=True, exist_ok=True)

manifest: dict[str, list[str]] = defaultdict(list)

# 1. Берём последнюю версию
latest = requests.get(
    "https://ddragon.leagueoflegends.com/api/versions.json"
).json()[0]

champ_list = requests.get(
    f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion.json"
).json()["data"]

for champ_key in champ_list:
    champ = requests.get(
        f"https://ddragon.leagueoflegends.com/cdn/{latest}/data/en_US/champion/{champ_key}.json"
    ).json()["data"][champ_key]

    for skin in champ["skins"]:
        url = f"https://ddragon.leagueoflegends.com/cdn/img/champion/splash/{champ_key}_{skin['num']}.jpg"
        fname = OUT / f"{champ_key}_{skin['num']}.jpg"

        # Качаем, если файла нет
        if not fname.exists():
            img = requests.get(url, timeout=20)
            fname.write_bytes(img.content)
            print(f"✔ {fname.name}")

        # В любом случае добавляем путь в манифест
        manifest[champ_key].append(str(fname.resolve()))

# 2. Записываем словарь на диск
with (OUT / "manifest.json").open("w", encoding="utf-8") as f:
    json.dump(manifest, f, ensure_ascii=False, indent=2)

print("Готово! Списки сплэш-артов сохранены в data/splashes/manifest.json")

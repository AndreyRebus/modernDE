import requests, pathlib, json

OUT = pathlib.Path("data/splashes")
OUT.mkdir(exist_ok=True)

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
        if not fname.exists():                       # не качаем повторно
            img = requests.get(url, timeout=20)
            fname.write_bytes(img.content)
            print(f"✔ {fname.name}")

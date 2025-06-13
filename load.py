import os, re, io, time, datetime, urllib.parse, requests, boto3
import pandas as pd

def fetch_matches_to_s3(
    riot_id: str,
    start_date: datetime.date,
    end_date: datetime.date | None = None,
    *,
    api_key: str | None = None,
    platform_routing: str = "ru1",
    regional_routing: str = "europe",
    bucket_name: str = "test-s3test",
    s3_prefix: str = "stage_load_raw_data",
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    rate_delay: float = 1.2,
) -> str:
    """
    Выгружает матчи игрока за указанный период в Parquet и кладёт файл в S3.
    Возвращает итоговый object key.
    """

    # -------------------- даты --------------------
    if end_date is None:
        end_date = start_date
    start_dt = datetime.datetime.combine(start_date, datetime.time())
    if end_date == datetime.date.today():
        end_dt = datetime.datetime.now()
    else:
        end_dt = datetime.datetime.combine(end_date + datetime.timedelta(days=1),
                                           datetime.time())
    start_ts, end_ts = int(start_dt.timestamp()), int(end_dt.timestamp())

    # -------------------- ключи и заголовки --------------------
    api_key = api_key or os.getenv("RIOT_API_KEY")
    if not api_key:
        raise ValueError("RIOT_API_KEY не задан.")
    headers = {"X-Riot-Token": api_key}

    # -------------------- очистка riot-id --------------------
    riot_id_clean = re.sub(r"[\u2066-\u2069]", "", riot_id)
    game_name, tagline = riot_id_clean.split("#", maxsplit=1)

    # -------------------- получаем PUUID --------------------
    acct_url = (
        f"https://{regional_routing}.api.riotgames.com/riot/account/v1/"
        f"accounts/by-riot-id/{urllib.parse.quote(game_name)}/{urllib.parse.quote(tagline)}"
    )
    acct_data = requests.get(acct_url, headers=headers).json()
    if "puuid" not in acct_data:
        raise RuntimeError(f"Не удалось получить PUUID: {acct_data}")
    puuid = acct_data["puuid"]

    # -------------------- список матчей --------------------
    ids_url = (
        f"https://{regional_routing}.api.riotgames.com/lol/match/v5/"
        f"matches/by-puuid/{puuid}/ids?startTime={start_ts}&endTime={end_ts}&count=100"
    )
    match_ids = requests.get(ids_url, headers=headers).json()

    # -------------------- грузим JSON каждого матча --------------------
    raw = []
    for mid in match_ids:
        murl = f"https://{regional_routing}.api.riotgames.com/lol/match/v5/matches/{mid}"
        raw.append(requests.get(murl, headers=headers).json())
        time.sleep(rate_delay)

    # -------------------- нормализация в DataFrame --------------------
    meta_cols = [
        "metadata.matchId",
        "info.gameCreation",
        "info.gameDuration",
        "info.gameMode",
        "info.queueId",
        "info.gameVersion",
    ]
    parts = []
    for m in raw:
        base = {col: pd.json_normalize(m)[col][0] for col in meta_cols}
        for p in m["info"]["participants"]:
            parts.append({**base, **{f"participant.{k}": v for k, v in p.items()}})

    participants_df = pd.DataFrame(parts)
    participants_df["source_nickname"] = riot_id_clean

    # -------------------- S3 / ЯОС --------------------
    aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (aws_access_key_id and aws_secret_access_key):
        raise ValueError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY не заданы.")

    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="ru-central1",
    )
    s3 = session.resource("s3", endpoint_url="https://storage.yandexcloud.net")

    safe_riot_id = riot_id_clean.replace("#", "_")
    object_key = f"{s3_prefix}/{safe_riot_id}_{start_date}_{end_date}.parquet"

    buffer = io.BytesIO()
    participants_df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    s3.Object(bucket_name, object_key).upload_fileobj(buffer)

    return object_key


if __name__=="__main__":
    import datetime
key = fetch_matches_to_s3(
    riot_id="Monty Gard#RU1",
    start_date=datetime.date(2025, 6, 14),
    api_key="RGAPI-c2d86601-5f95-47b4-9f39-81b04f5deed8",
    aws_access_key_id="YCAJExz5vm-sE9r_95JbsXcir",
    aws_secret_access_key="YCN0qHEGV4-9vYQ5BqY7ZpoSdnmoqhyBR5YPivcV",
)
print(f"✅ Файл {key} успешно загружен!")
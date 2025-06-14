# load_lol_matches_once_per_day.py
# pip install --upgrade requests pandas boto3 pyarrow trino

import os, re, io, time, datetime, urllib.parse, requests, boto3
from dotenv import load_dotenv

load_dotenv()
import pandas as pd
from trino import dbapi


def fetch_matches_once_per_day(
    riot_id: str,
    load_date: datetime.date,                 # ← ровно один календарный день
    *,
    api_key: str | None = None,
    platform_routing: str = "ru1",
    regional_routing: str = "europe",
    bucket_name: str = "test-s3test",
    s3_prefix: str = "stage_load_raw_data",
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    rate_delay: float = 1.2,
    trino_host: str = "5.129.208.115",
    trino_port: int = 8080,
    trino_user: str = "trino",
    trino_catalog: str = "iceberg",
    trino_schema: str = "lol_raw",
    trino_table: str = "data_api_mining",
) -> str | None:
    """
    Выгружает матчи игрока за конкретную дату. Если файлы за этот день
    уже существуют в S3, пропускает работу и возвращает None.
    """

    # ─── ключи AWS ───
    aws_access_key_id  = aws_access_key_id  or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (aws_access_key_id and aws_secret_access_key):
        raise ValueError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY не заданы.")

    session = boto3.session.Session(
        aws_access_key_id     = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name           = "ru-central1",
    )
    s3 = session.resource("s3", endpoint_url="https://storage.yandexcloud.net")

    # ─── имя подпапки ───
    folder_date  = load_date.isoformat()                    # YYYY-MM-DD
    riot_id_clean = re.sub(r"[\u2066-\u2069]", "", riot_id)
    safe_riot_id = riot_id_clean.replace("#", "_")
    s3_folder    = f"{s3_prefix}/{folder_date}/{safe_riot_id}/"

    # ─── проверка: уже загружено? ───
    bucket = s3.Bucket(bucket_name)
    already_loaded = any(obj.key.startswith(s3_folder) for obj in bucket.objects.filter(Prefix=s3_folder))
    if already_loaded:
        print(f"⏩ День {folder_date} для {riot_id} уже загружен — пропускаем.")
        return None

    # ───  диапазон времени этого дня ───
    start_ts = int(datetime.datetime.combine(load_date, datetime.time()).timestamp())
    end_ts   = int((datetime.datetime.combine(load_date, datetime.time()) +
                    datetime.timedelta(days=1)).timestamp())

    # ─── Riot API ───
    api_key = api_key or os.getenv("RIOT_API_KEY")
    if not api_key:
        raise ValueError("RIOT_API_KEY не задан.")
    headers = {"X-Riot-Token": api_key}

    game_name, tagline = riot_id_clean.split("#", 1)
    acct_url = (f"https://{regional_routing}.api.riotgames.com/riot/account/v1/"
                f"accounts/by-riot-id/{urllib.parse.quote(game_name)}/"
                f"{urllib.parse.quote(tagline)}")
    puuid = requests.get(acct_url, headers=headers).json().get("puuid")
    if not puuid:
        raise RuntimeError("Не удалось получить PUUID")

    ids_url = (f"https://{regional_routing}.api.riotgames.com/lol/match/v5/"
               f"matches/by-puuid/{puuid}/ids?startTime={start_ts}&endTime={end_ts}&count=100")
    match_ids = requests.get(ids_url, headers=headers).json()

    # ─── скачиваем и нормализуем ───
    meta_cols = ["metadata.matchId", "info.gameCreation", "info.gameDuration",
                 "info.gameMode", "info.queueId", "info.gameVersion"]
    parts = []
    for mid in match_ids:
        m = requests.get(
            f"https://{regional_routing}.api.riotgames.com/lol/match/v5/matches/{mid}",
            headers=headers).json()
        base = {c: pd.json_normalize(m)[c][0] for c in meta_cols}
        for p in m["info"]["participants"]:
            parts.append({**base,
                          **{f"participant.{k}": v for k, v in p.items()}})
        time.sleep(rate_delay)

    df = pd.DataFrame(parts)
    df["source_nickname"] = riot_id_clean

    # ─── сохраняем в S3 ───
    object_key = (
        f"{s3_folder}{safe_riot_id}_{load_date}_{load_date}.parquet"
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    s3.Object(bucket_name, object_key).upload_fileobj(buf)

    # ─── регистрируем в Iceberg ───
    location_folder = f"s3://{bucket_name}/{s3_folder.rstrip('/')}"
    sql = f"""
        ALTER TABLE {trino_catalog}.{trino_schema}.{trino_table}
        EXECUTE add_files(
            location => '{location_folder}',
            format   => 'PARQUET'
        )
    """

    with dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=trino_catalog,
        schema=trino_schema,
        http_scheme="http",
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql.strip())
        cur.fetchall()

    return object_key


# ───────────── пример использования ─────────────
if __name__ == "__main__":
    import datetime
    for riot in ["Monty Gard#RU1", "Breaksthesilence#RU1"]:
        for day in (datetime.date(2025, 6, 9),
                    datetime.date(2025, 6, 10),
                    datetime.date(2025, 6, 11)):
            key = fetch_matches_once_per_day(
                riot_id   = riot,
                load_date = day,
            )
            if key:
                print(f"✅ Файл {key} успешно загружен и зарегистрирован!")

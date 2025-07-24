"""record_notifier_bot.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Бот на **aiogram 3**: берёт данные из Iceberg через **Trino dbapi.connect**
точно тем же способом, что и в `load_lol_matches_once_per_day.py`.
Команда /check шлёт сообщения‑рекорды в чат.

Зависимости:  
`pip install aiogram aiogram-dialog pandas python-dotenv trino urllib3`
"""

from __future__ import annotations

import asyncio
import os
from typing import Dict, List

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram_dialog import setup_dialogs
from dotenv import load_dotenv
from trino import dbapi
from trino.auth import BasicAuthentication
import urllib3

# ────────────────── env & tls ──────────────────
load_dotenv()                                   # так же, как в примере
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # exact line

# ────────────────── cfg ──────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (export или .env)")

TRINO_HOST = os.getenv("TRINO_HOST", "5.129.208.115")
TRINO_PORT = int(os.getenv("TRINO_PORT", 8443))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "").strip()
if not TRINO_PASSWORD:
    raise RuntimeError("TRINO_PASSWORD не задан (export или .env)")

TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "dbt_model")
TRINO_TABLE = os.getenv("TRINO_TABLE", "concat_record")

# ────────────────── templates (не менялись) ──────────────────
TEMPLATES: Dict[str, str] = {
  "dmg_to_champs": "{nickname}, потрясающе! В матче {matchId} ты нанёс {value} урона по чемпионам — твой лучший результат за последний месяц.",
  "dmg_to_champs_meta": "{nickname}, новый личный рекорд по урону в чемпионов установлен в матче {matchId}!",

  "dmg_total": "{nickname}, настоящая армия урона! {value} суммарного урона в матче {matchId} — выше всех твоих игр за месяц.",
  "dmg_total_meta": "{nickname}, ты превзошёл себя и обновил месячный рекорд суммарного урона в матче {matchId}.",

  "dmg_turrets": "{nickname}, башни дрожали! В матче {matchId} ты нанёс {value} урона по башням — новый максимум месяца.",
  "dmg_turrets_meta": "{nickname}, рекордный урон по башням! Ты вновь оказался лучшим в матче {matchId}.",

  "dmg_objectives": "{nickname}, охотник на цели! {value} урона по объективам в матче {matchId} — лучший показатель за месяц.",
  "dmg_objectives_meta": "{nickname}, ты обновил месячный рекорд по урону по целям — матч {matchId}.",

  "gold_earned": "{nickname}, золотая жила! {value} золота в матче {matchId} — больше, чем в любой игре за месяц.",
  "gold_earned_meta": "{nickname}, новый месячный максимум заработанного золота установлен в матче {matchId}.",

  "kills": "{nickname}, машина убийств! {value} киллов в матче {matchId} — рекорд последних четырёх недель.",
  "kills_meta": "{nickname}, ты поднял планку киллов ещё выше в матче {matchId}.",

  "assists": "{nickname}, герой поддержки! {value} ассистов в матче {matchId} — твой лучший командный вклад за месяц.",
  "assists_meta": "{nickname}, новый месячный рекорд по ассистам установлен в матче {matchId}.",

  "cs": "{nickname}, фермер-профи! {value} добитых миньонов в матче {matchId} — лучший фарм месяца.",
  "cs_meta": "{nickname}, ты улучшил месячный рекорд по CS в матче {matchId}.",

  "jungle_kills": "{nickname}, властелин нейтралов! {value} баронов и драконов в матче {matchId} — новый максимум месяца.",
  "jungle_kills_meta": "{nickname}, месячный рекорд крупных нейтралов снова твой — матч {matchId}.",

  "turret_kills": "{nickname}, разрушитель! Ты снёс {value} башен в матче {matchId} — лучший результат месяца.",
  "turret_kills_meta": "{nickname}, ты обновил месячный рекорд по башням в матче {matchId}.",

  "inhib_kills": "{nickname}, ломатель ингибов! {value} ингибиторов в матче {matchId} — рекорд последних недель.",
  "inhib_kills_meta": "{nickname}, новый рекорд по ингибиторам установлен в матче {matchId}.",

  "pinks": "{nickname}, властелин вижна! {value} контроль-вардов куплено в матче {matchId} — максимум месяца.",
  "pinks_meta": "{nickname}, новый месячный рекорд по купленным пинкам — матч {matchId}.",

  "vision_score": "{nickname}, светоч карты! Vision Score {value} в матче {matchId} — выше, чем когда-либо за месяц.",
  "vision_score_meta": "{nickname}, ты превзошёл себя по Vision Score в матче {matchId}.",

  "cc_time": "{nickname}, мастер контроля! {value} секунд CC в матче {matchId} — лучший показатель месяца.",
  "cc_time_meta": "{nickname}, месячный рекорд времени контроля побит в матче {matchId}.",

  "dmg_mitigated": "{nickname}, железная стена! Ты поглотил {value} урона в матче {matchId} — новый личный максимум.",
  "dmg_mitigated_meta": "{nickname}, ты обновил рекорд по поглощённому урону в матче {matchId}.",

  "first_blood_kill": "{nickname}, стремительный старт! Ты взял First Blood в матче {matchId} — редкое достижение за месяц.",
  "first_blood_kill_meta": "{nickname}, первый кровавый удар месяца совершен в матче {matchId}.",

  "immortal": "{nickname}, безупречно! Ты закончил матч {matchId} без смертей при солидном вкладе — первый такой за месяц.",
  "immortal_meta": "{nickname}, новый безсмертный рекорд установлен в матче {matchId}.",

  "triple_kills": "{nickname}, трипл-шторм! {value} Triple Kills в матче {matchId} — рекорд месяца.",
  "triple_kills_meta": "{nickname}, рекорд тройных убийств вновь побит — матч {matchId}.",

  "quadra_kills": "{nickname}, квадра-герой! {value} Quadra Kills в матче {matchId} — лучший показатель месяца.",
  "quadra_kills_meta": "{nickname}, новый месячный максимум квадр установлен в матче {matchId}.",

  "penta_kills": "{nickname}, PENTAKILL! Ты сделал пенту в матче {matchId} — первая за последний месяц.",
  "penta_kills_meta": "{nickname}, твоя пента в матче {matchId} ставит новую планку месяца!",

  "heals_team": "{nickname}, целитель! Ты восстановил союзникам {value} здоровья в матче {matchId} — рекорд месяца.",
  "heals_team_meta": "{nickname}, новый максимум исцеления команды — матч {matchId}.",

  "shields_team": "{nickname}, непробиваемый! Ты подарил щитов на {value} урона в матче {matchId} — лучший вклад месяца.",
  "shields_team_meta": "{nickname}, рекорд по щитам обновлён в матче {matchId}.",

  "longest_life": "{nickname}, феникс! Ты прожил {value} с без смерти в матче {matchId} — самая длинная жизнь месяца.",
  "longest_life_meta": "{nickname}, новая самая долгая жизнь месяца — матч {matchId}.",

  "cspm": "{nickname}, CS-машина! Фарм {value} CS/мин в матче {matchId} — рекорд месяца.",
  "cspm_meta": "{nickname}, рекордная скорость фарма установлена в матче {matchId}.",

  "interceptor": "{nickname}, король стилов! {value} украденных целей в матче {matchId} — новый максимум месяца.",
  "interceptor_meta": "{nickname}, рекорд по стиллам снова твой — матч {matchId}.",

  "wards_killed": "{nickname}, охотник на варды! Ты уничтожил {value} вардов в матче {matchId} — лучший результат месяца.",
  "wards_killed_meta": "{nickname}, ты обновил месячный максимум снятых вардов в матче {matchId}.",

  "wards_placed": "{nickname}, проводник света! Ты поставил {value} вардов в матче {matchId} — рекорд последних недель.",
  "wards_placed_meta": "{nickname}, новый месячный рекорд по установленным вардам — матч {matchId}.",

  "dpm": "{nickname}, метеорит! DPM {value} в матче {matchId} — самый высокий за месяц.",
  "dpm_meta": "{nickname}, ты поднял рекорд урона-в-минуту в матче {matchId}.",

  "gpm": "{nickname}, банкир! GPM {value} в матче {matchId} — лучший финансовый темп месяца.",
  "gpm_meta": "{nickname}, новый максимум золота-в-минуту установлен в матче {matchId}.",

  "enemy_jungle": "{nickname}, лесной захватчик! Ты выфармил {value} вражеских крипов в матче {matchId} — рекорд месяца.",
  "enemy_jungle_meta": "{nickname}, рекорд чужого леса побит в матче {matchId}.",

  "neutral_kills": "{nickname}, лорд нейтралов! Ты забрал {value} нейтралов в матче {matchId} — новый максимум месяца.",
  "neutral_kills_meta": "{nickname}, месячный рекорд нейтралов обновлён — матч {matchId}.",

  "phys_dmg": "{nickname}, физ-гром! {value} физического урона в матче {matchId} — рекорд месяца.",
  "phys_dmg_meta": "{nickname}, новый максимум физического урона установлен в матче {matchId}.",

  "magic_dmg": "{nickname}, магистр арканы! {value} магического урона в матче {matchId} — лучший результат месяца.",
  "magic_dmg_meta": "{nickname}, рекорд магического урона снова твой — матч {matchId}.",

  "true_dmg": "{nickname}, истинный калибр! {value} true-урона в матче {matchId} — новый максимум месяца.",
  "true_dmg_meta": "{nickname}, месячный рекорд истинного урона побит в матче {matchId}.",

  "dmg_taken": "{nickname}, тяжёлый танк! Ты принял {value} урона в матче {matchId} — больше, чем когда-либо за месяц.",
  "dmg_taken_meta": "{nickname}, новый максимум полученного урона зафиксирован в матче {matchId}.",

  "largest_crit": "{nickname}, легенда критов! Крит на {value} в матче {matchId} — самый мощный за месяц.",
  "largest_crit_meta": "{nickname}, критический рекорд обновлён в матче {matchId}.",

  "double_kills": "{nickname}, двойной удар! {value} Double Kills в матче {matchId} — рекорд месяца.",
  "double_kills_meta": "{nickname}, новый максимум двойных убийств установлен в матче {matchId}.",

  "sprees": "{nickname}, снежный ком! Серия из {value} убийств в матче {matchId} — самый длинный стрик месяца.",
  "sprees_meta": "{nickname}, рекорд серии убийств снова твой — матч {matchId}.",

  "gold_unspent": "{nickname}, скряга! Ты закончил матч {matchId} с {value} непр потраченного золота — необычный рекорд месяца.",
  "gold_unspent_meta": "{nickname}, новый максимум непотраченного золота — матч {matchId}.",

  "flash_casts": "{nickname}, флэш-боец! Ты использовал «Вспышку» {value} раз в матче {matchId} — больше, чем в любой игре месяца.",
  "flash_casts_meta": "{nickname}, рекорд по использованию «Вспышки» снова обновлён в матче {matchId}.",

  "undying_ratio": "{nickname}, почти бессмертен! Всего {value}% времени в смерти в матче {matchId} — лучший показатель месяца.",
  "undying_ratio_meta": "{nickname}, минимальная доля времени смерти обновлена в матче {matchId}.",

  "champ_level": "{nickname}, мастер опыта! Ты достиг уровня {value} в матче {matchId} — выше, чем в любых играх за месяц.",
  "champ_level_meta": "{nickname}, новый месячный рекорд по уровню чемпиона установлен в матче {matchId}.",

  "guard_angel": "{nickname}, ангел-хранитель! Общее лечение+щиты {value} в матче {matchId} — лучший вклад в команду за месяц.",
  "guard_angel_meta": "{nickname}, рекорд объединённого лечения и щитов снова твой — матч {matchId}.",

  "source_nickname": "{nickname}, твой ник {value} попал в список сегодняшних рекордсменов!"
}


METRICS: List[str] = list(TEMPLATES.keys())

# ────────────────── db helper (идентичен примеру) ──────────────────

def fetch_data() -> pd.DataFrame:
    """Читаем *только нужные колонки* — тем самым обходим проблему
    "Unsupported Trino column type" на вложенных полях,
    сохраняя при этом тот же способ подключения к Trino.
    """

    load_dotenv()

    host = os.getenv("TRINO_HOST", "5.129.208.115")
    port = int(os.getenv("TRINO_PORT", 8443))
    user = os.getenv("TRINO_USER", "admin")
    catalog =  "iceberg"
    schema =  "dbt_model"
    table =  "concat_record"
    pwd = os.getenv("TRINO_PASSWORD")
    if not pwd:
        raise RuntimeError("TRINO_PASSWORD не задан")

    # Берём только столбцы, которые реально нужны боту
    cols_needed = ["source_nickname"] + METRICS + [f"{m}_meta" for m in METRICS]
    cols_sql = ", ".join(cols_needed)
    
    sql = f"SELECT {cols_sql} FROM {catalog}.{schema}.{table}"
    print(sql)
    with dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        auth=BasicAuthentication(user, pwd),
        verify=False,
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return pd.DataFrame(rows, columns=cols)

# ────────────────── logic ──────────────────

def generate_messages(row: pd.Series) -> List[str]:
    out: List[str] = []
    nick = row.get("source_nickname", "<nick>")
    for m in METRICS:
        val = row.get(m)
        if val is None:
            continue
        try:
            if float(val) == 0.0:
                continue
        except (TypeError, ValueError):
            continue
        mid = row.get(f"{m}_meta") or "<matchId>"
        out.append(TEMPLATES[m].format(nickname=nick, matchId=mid, value=val))
    return out

# ────────────────── aiogram ──────────────────

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
setup_dialogs(dp)

@dp.message(Command("check"))
async def cmd_check(m):
    await m.answer("⏳ Читаю данные из Iceberg…")
    try:
        df = fetch_data()
    except Exception as e:
        await m.answer(f"❌ Ошибка выборки: {e}")
        return

    sent = 0
    for _, row in df.iterrows():
        for txt in generate_messages(row):
            await m.answer(txt)
            sent += 1
    await m.answer("🏁 Рекордов нет." if sent == 0 else f"🏆 Отправлено {sent} рекорд(ов)!")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

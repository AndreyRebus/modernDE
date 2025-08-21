# bot/kda_charts.py
from __future__ import annotations

import unicodedata
import os
import re
import math
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
import plotly.graph_objects as go

from .trino_client import query_df

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kda_charts")

# Конфиги
KDA_TABLE = os.getenv("KDA_TABLE", "iceberg.dbt_model.avg_kd")
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", "bot/data/image"))
IMAGE_DIR.mkdir(parents=True, exist_ok=True)

# Порядок недель (обрати внимание: здесь длинное тире U+2013)
WEEK_ORDER = ["Week –4", "Week –3", "Week –2", "Week –1", "This Week"]

# Санитайзер для имён файлов
SAFE = re.compile(r"[^A-Za-z0-9_.\-]+", re.UNICODE)

def _safe_name(nick: str) -> str:
    # нормализуем юникод, чтобы буквы были в одном представлении
    name = unicodedata.normalize("NFC", str(nick))

    # заменим разделители на подчеркивание
    name = name.replace("#", "_")

    # пробелы -> _
    name = re.sub(r"\s+", "_", name, flags=re.UNICODE)

    # запретные для файловой системы символы -> _
    # (слэши, двоеточия и пр.)
    name = re.sub(r'[\/\\:*?"<>|]', "_", name)

    # оставим только буквенно-цифровые (вкл. кириллицу), точку, дефис и _
    name = re.sub(r"[^\w.\-]", "_", name, flags=re.UNICODE)

    # схлопнем подряд идущие _ и обрежем по краям
    name = re.sub(r"_+", "_", name).strip("._-")

    return name or "player"

def _mean_safe(values: list[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    return sum(nums) / len(nums) if nums else None

def _fmt_num(x: Optional[float], ndigits: int = 3) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{round(x, ndigits)}"

def fetch_kda() -> pd.DataFrame:
    # Тянем все строки; фильтрацию по неделям делаем на стороне pandas
    sql = f"""
        SELECT
            nickname,
            week_label,
            matches,
            avg_kda_per_match,
            kda_overall
        FROM {KDA_TABLE}
    """
    log.info("SQL: %s", sql.strip())
    df = query_df(sql)
    # Нормализуем типы
    for col in ["matches"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["avg_kda_per_match", "kda_overall"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def prep_player_series(df: pd.DataFrame) -> dict[str, dict]:
    """
    Преобразуем в словарь:
      { nickname: {"weeks": [..5..], "current": float|None, "delta": float|None} }
    """
    # оставим только нужные недели
    df = df[df["week_label"].isin(WEEK_ORDER)].copy()

    # сортируем по порядку недель
    df["week_order"] = df["week_label"].apply(lambda w: WEEK_ORDER.index(w))
    df.sort_values(["nickname", "week_order"], inplace=True)

    out: dict[str, dict] = {}
    for nick, g in df.groupby("nickname", sort=False):
        # список в фиксированном порядке
        by_week = {row["week_label"]: row.get("avg_kda_per_match") for _, row in g.iterrows()}
        series = [by_week.get(w) for w in WEEK_ORDER]

        current = series[-1]  # This Week
        prev_mean = _mean_safe(series[:-1])

        if current is None or (isinstance(current, float) and math.isnan(current)) or prev_mean is None:
            delta = None
        else:
            delta = round(current - prev_mean, 2)

        out[nick] = {
            "series": series,
            "current": current if (current is not None and not math.isnan(current)) else None,
            "delta": delta,
        }
    return out

def draw_chart(nickname: str, kda_values: list[Optional[float]], current: Optional[float], delta: Optional[float], out_path: Path):
    # определяем стрелку и цвет
    if delta is None:
        arrow = "→"
        color = "gray"
        delta_txt = "±0"
    else:
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        color = "green" if delta > 0 else "red" if delta < 0 else "gray"
        delta_txt = f"{delta:+}"

    current_txt = _fmt_num(current, 3)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=WEEK_ORDER,
            y=kda_values,
            mode="lines+markers",
            line=dict(width=3),
            marker=dict(size=10),
            connectgaps=False,   # если есть None — разрыв
            name="KDA",
        )
    )

    # Заголовок (имя)
    fig.add_annotation(
        text=f"<b>{nickname}</b>",
        x=0.5, y=1.15,
        showarrow=False,
        font=dict(size=24),
        xref="paper", yref="paper"
    )

    # Текущий KDA
    fig.add_annotation(
        text=f"<b>KDA: {current_txt}</b>",
        x=0.5, y=1.0,
        showarrow=False,
        font=dict(size=32),
        xref="paper", yref="paper"
    )

    # Дельта
    fig.add_annotation(
        text=f"<span style='color:{color}; font-size:26px'>{arrow} ({delta_txt})</span>",
        x=0.5, y=0.85,
        showarrow=False,
        xref="paper", yref="paper"
    )

    fig.update_layout(
        width=700,
        height=400,
        margin=dict(l=20, r=20, t=60, b=40),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis_title="Week",
        yaxis_title="Average KDA",
    )

    # Сохраняем PNG (нужен kaleido)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(out_path), format="jpg", scale=2)   # 1400x800

def main():
    log.info("IMAGE_DIR = %s", IMAGE_DIR.resolve())

    # ── очистка каталога с картинками ──
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    removed = 0
    for pat in ("*.jpg", "*.jpeg", "*.png"):
        for p in IMAGE_DIR.glob(pat):
            try:
                p.unlink()
                removed += 1
            except Exception as e:
                log.warning("Cannot remove %s: %s", p, e)
    log.info("Cleared %d files in %s", removed, IMAGE_DIR.resolve())
    # ───────────────────────────────────
    df = fetch_kda()
    if df.empty:
        log.warning("No data in %s", KDA_TABLE)
        return

    players = prep_player_series(df)
    log.info("Players to render: %d", len(players))

    for nick, payload in players.items():
        safe = _safe_name(nick)
        out_path = IMAGE_DIR / f"{safe}.jpg"
        try:
            draw_chart(
                nickname=nick,
                kda_values=payload["series"],
                current=payload["current"],
                delta=payload["delta"],
                out_path=out_path,
            )
            log.info("Saved chart: %s", out_path)
        except Exception:
            log.exception("Failed to render for %s", nick)

if __name__ == "__main__":
    main()

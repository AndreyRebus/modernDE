from __future__ import annotations
import math
from typing import Dict, List, Tuple
import pandas as pd

from .templates import TEMPLATES, METRIC_COLS

def _split_meta(raw: str | None):
    if not raw or not isinstance(raw, str):
        return "<match>", "<champion>"
    if "-_-" in raw:
        match_id, champ = raw.split("-_-", 1)
        return match_id or "<match>", champ or "<champion>"
    return raw, "<champion>"

def build_messages(df: pd.DataFrame) -> List[Dict]:
    sent_pairs: set[Tuple[str, str, str]] = set()
    counts: Dict[str, int] = {}
    out: List[Dict] = []

    for _, row in df.iterrows():
        nick = row.get("source_nickname")
        if isinstance(nick, pd.Series):
            nick = nick.iloc[0]
        if not isinstance(nick, str) or not nick:
            continue

        for metric in METRIC_COLS:
            val = row.get(metric)
            if val in (None, "", "0") or (
                isinstance(val, (int, float)) and (val == 0 or math.isnan(val))
            ):
                continue

            match_id, champion = _split_meta(row.get(f"{metric}_meta"))

            if counts.get(champion, 0) >= 3:  # лимит 3 ачивки на чемпиона
                continue

            key = (nick, metric, match_id)
            if key in sent_pairs:
                continue
            sent_pairs.add(key)

            if isinstance(val, float) and val.is_integer():
                val = int(val)

            num = match_id.removeprefix("RU_")
            match_link = (
                f'<a href="https://www.leagueofgraphs.com/match/ru/{num}">'
                f'{match_id}</a>'
            )

            text = TEMPLATES[metric].format(
                nickname=nick, matchId=match_link, champion=champion, value=val
            )
            out.append({"text": text, "champion": champion})
            counts[champion] = counts.get(champion, 0) + 1
    return out
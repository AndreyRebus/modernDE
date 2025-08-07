{{ config(materialized='table') }}



{% set names = [
        "Monty Gard#RU1",
        "Breaksthesilence#RU1",
        "2pilka#RU1",
        "Gruntq#RU1",
        "Шaзам#RU1",
        "Prooaknor#RU1",
    ] %}

WITH base AS (                       -- данные по матчам
    SELECT
        "metadata.matchid"      AS match_id,
        source_nickname,
        "participant.kills"     AS kills,
        "participant.assists"   AS assists,
        "participant.deaths"    AS deaths,
        "info.gamecreation"     AS game_creation_ts
    FROM {{ source('lol_raw','data_api_mining') }}
    WHERE source_nickname IN (
        {% for n in names %}'{{ n }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
),

per_game AS (                         -- KDA каждой игры
    SELECT
        source_nickname,
        ((kills + assists) * 1.0) / GREATEST(deaths, 1)  AS kda,
        date(from_unixtime(game_creation_ts / 1000))     AS game_date
    FROM base
),

current_week_start AS (               -- понедельник текущей недели
    SELECT date_trunc('week', current_date) AS week_start
),

filtered AS (                         -- матчи за 5 ПОЛНЫХ недель до текущей
    SELECT
        pg.source_nickname,
        date_trunc('week', pg.game_date)    AS week_start,
        pg.kda
    FROM per_game pg
    CROSS JOIN current_week_start cws
    WHERE pg.game_date <  cws.week_start                 -- исключаем незавершённую неделю
      AND pg.game_date >= cws.week_start - INTERVAL '35' DAY
),

avg_weekly_kda AS (                   -- средний KDA по неделям
    SELECT
        source_nickname,
        week_start,
        ROUND(AVG(kda), 2)            AS avg_kda
    FROM filtered
    GROUP BY source_nickname, week_start
)

SELECT *
FROM avg_weekly_kda
ORDER BY week_start DESC, source_nickname            -- сначала свежие недели, затем ники

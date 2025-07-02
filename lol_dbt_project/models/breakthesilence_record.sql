-- Monty Gard – Daily Record Detection
-- Выводит одну строку с новыми рекордами за ТЕКУЩУЮ дату.
-- Для каждой метрики пара столбцов:
--   <metric>        – новое значение, если побит исторический рекорд, иначе NULL
--   <metric>_meta   – "matchId-_-championName" при срабатывании, иначе NULL
{% set nickname = 'Breaksthesilence#RU' %}
{% set metrics = [
    {'name': 'dmg_to_champs',  'agg': 'max'},
    {'name': 'dmg_total',      'agg': 'max'},
    {'name': 'dmg_turrets',    'agg': 'max'},
    {'name': 'dmg_objectives', 'agg': 'max'},
    {'name': 'gold_earned',    'agg': 'max'},
    {'name': 'kills',          'agg': 'max'},
    {'name': 'assists',        'agg': 'max'},
    {'name': 'cs',             'agg': 'max'},
    {'name': 'jungle_kills',   'agg': 'max'},
    {'name': 'turret_kills',   'agg': 'max'},
    {'name': 'inhib_kills',    'agg': 'max'},
    {'name': 'pinks',          'agg': 'max'},
    {'name': 'vision_score',   'agg': 'max'},
    {'name': 'cc_time',        'agg': 'max'},
    {'name': 'dmg_mitigated',  'agg': 'max'},
    {'name': 'first_blood_kill','agg': 'max'},
    {'name': 'immortal',       'agg': 'max'},
    {'name': 'triple_kills',   'agg': 'max'},
    {'name': 'quadra_kills',   'agg': 'max'},
    {'name': 'penta_kills',    'agg': 'max'},
    {'name': 'heals_team',     'agg': 'max'},
    {'name': 'shields_team',   'agg': 'max'},
    {'name': 'longest_life',   'agg': 'max'},
    {'name': 'cspm',           'agg': 'max'},
    {'name': 'interceptor',    'agg': 'max'},
    {'name': 'wards_killed',   'agg': 'max'},
    {'name': 'wards_placed',   'agg': 'max'},
    {'name': 'dpm',            'agg': 'max'},
    {'name': 'gpm',            'agg': 'max'},
    {'name': 'enemy_jungle',   'agg': 'max'},
    {'name': 'neutral_kills',  'agg': 'max'},
    {'name': 'phys_dmg',       'agg': 'max'},
    {'name': 'magic_dmg',      'agg': 'max'},
    {'name': 'true_dmg',       'agg': 'max'},
    {'name': 'dmg_taken',      'agg': 'max'},
    {'name': 'largest_crit',   'agg': 'max'},
    {'name': 'double_kills',   'agg': 'max'},
    {'name': 'sprees',         'agg': 'max'},
    {'name': 'gold_unspent',   'agg': 'max'},
    {'name': 'flash_casts',    'agg': 'max'},
    {'name': 'undying_ratio',  'agg': 'min'},
    {'name': 'champ_level',    'agg': 'max'},
    {'name': 'guard_angel',    'agg': 'max'}
] %}

-- === Base rows ===
WITH base AS (
    SELECT
        "metadata.matchid"                                         AS match_id,
        source_nickname,
        "participant.championname"                                 AS champion_name,
        -- raw metrics
        "participant.totaldamagedealttochampions"                  AS dmg_to_champs,
        "participant.totaldamagedealt"                             AS dmg_total,
        "participant.damagedealttoturrets"                         AS dmg_turrets,
        "participant.damagedealttoobjectives"                      AS dmg_objectives,
        "participant.goldearned"                                   AS gold_earned,
        "participant.goldspent"                                    AS gold_spent,
        "participant.kills"                                        AS kills,
        "participant.assists"                                      AS assists,
        "participant.deaths"                                       AS deaths,
        "participant.totalminionskilled"                           AS cs,
        "participant.dragonkills"                                  AS dragon_kills,
        "participant.baronkills"                                   AS baron_kills,
        "participant.turretkills"                                  AS turret_kills,
        "participant.inhibitorkills"                               AS inhib_kills,
        "participant.visionwardsboughtingame"                      AS pinks,
        "participant.visionscore"                                  AS vision_score,
        "participant.timeccingothers"                              AS cc_time,
        "participant.damageselfmitigated"                          AS dmg_mitigated,
        "participant.firstbloodkill"                               AS first_blood_kill,
        "participant.triplekills"                                  AS triple_kills,
        "participant.quadrakills"                                  AS quadra_kills,
        "participant.pentakills"                                   AS penta_kills,
        "participant.totalhealsonteammates"                        AS heals_team,
        "participant.totaldamageshieldedonteammates"               AS shields_team,
        "participant.longesttimespentliving"                       AS longest_life,
        "info.gameduration"                                        AS game_duration,
        "participant.objectivesstolen"                             AS obj_stolen,
        "participant.objectivesstolenassists"                      AS obj_stolen_ast,
        "participant.wardskilled"                                  AS wards_killed,
        "participant.wardsplaced"                                  AS wards_placed,
        "participant.neutralminionskilled"                         AS neutral_kills,
        "participant.physicaldamagedealttochampions"               AS phys_dmg,
        "participant.magicdamagedealttochampions"                  AS magic_dmg,
        "participant.truedamagedealttochampions"                   AS true_dmg,
        "participant.totaldamagetaken"                             AS dmg_taken,
        "participant.largestcriticalstrike"                        AS largest_crit,
        "participant.doublekills"                                  AS double_kills,
        "participant.killingsprees"                                AS sprees,
        "participant.totalenemyjungleminionskilled"                AS enemy_jungle,
        (CASE WHEN "participant.summoner1id" = 4 THEN "participant.summoner1casts" ELSE 0 END +
         CASE WHEN "participant.summoner2id" = 4 THEN "participant.summoner2casts" ELSE 0 END) AS flash_casts,
        "participant.totaltimespentdead"                           AS time_dead,
        "participant.champlevel"                                   AS champ_level,
        "info.gamecreation"                                        AS game_creation_ts
    FROM {{ source('lol_raw','data_api_mining') }}
    WHERE source_nickname = '{{ nickname }}'
    AND CONCAT("participant.riotidgamename", "participant.riotidtagline") = replace('{{ nickname }}', '#', '')
),

aggregated AS (
    SELECT
        *,
        (gold_earned - gold_spent)                                   AS gold_unspent,
        (dragon_kills + baron_kills)                                 AS jungle_kills,
        (obj_stolen + obj_stolen_ast)                                AS interceptor,
        (heals_team + shields_team)                                  AS guard_angel,
        (cs * 60.0 / NULLIF(game_duration,0))                        AS cspm,
        (dmg_total / NULLIF(game_duration,0))                        AS dpm,
        (gold_earned / NULLIF(game_duration,0))                      AS gpm,
        (time_dead / NULLIF(game_duration,0))                        AS undying_ratio,
        CASE WHEN deaths = 0 AND (kills + assists) >= 10 THEN 1 ELSE 0 END AS immortal
    FROM base
),

today_games AS (
    SELECT *
    FROM aggregated
    WHERE date(from_unixtime(game_creation_ts / 1000)) = current_date - INTERVAL '1' DAY
),

today_metrics AS (
    SELECT
        {% for m in metrics %}
        {{ m['agg'] }}({{ m['name'] }}) AS {{ m['name'] }},
        {{ m['agg'] }}_by(match_id, {{ m['name'] }}) AS {{ m['name'] }}_match_id,
        {{ m['agg'] }}_by(champion_name, {{ m['name'] }}) AS {{ m['name'] }}_champion{% if not loop.last %},
        {% endif %}
        {% endfor %}
    FROM today_games
),

historical_best AS (
    SELECT
        {% for m in metrics %}
        {{ m['agg'] }}({{ m['name'] }}) AS {{ m['name'] }}{% if not loop.last %},
        {% endif %}
        {% endfor %}
    FROM aggregated
    WHERE date(from_unixtime(game_creation_ts / 1000)) BETWEEN current_date - INTERVAL '32' DAY AND current_date - INTERVAL '2' DAY
),

comparison AS (
    SELECT
        {% for m in metrics %}
        CASE WHEN t.{{ m['name'] }} {{ '>' if m['agg'] == 'max' else '<' }} h.{{ m['name'] }} THEN t.{{ m['name'] }} END AS {{ m['name'] }},
        CASE WHEN t.{{ m['name'] }} {{ '>' if m['agg'] == 'max' else '<' }} h.{{ m['name'] }} THEN concat(t.{{ m['name'] }}_match_id, '-_-', t.{{ m['name'] }}_champion) END AS {{ m['name'] }}_meta{% if not loop.last %},
        {% endif %}
        {% endfor %}
    FROM today_metrics t
    CROSS JOIN historical_best h
)

SELECT *, '{{ nickname }}' as source_nickname FROM comparison
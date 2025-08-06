{{ config(materialized='table') }}

{% set tables = [
    '2pilka_record',
    'breakthesilence_record',
    'gruntq_record',
    'monty_record',
    'prooaknor_record',
    'shazam_record'
] %}

WITH weekly_kda AS (
    SELECT 
        "participant.riotidgamename" AS player_name,
        "participant.riotidtagline" AS player_tag,
        date_trunc('week', from_unixtime("info.gamecreation" / 1000)) AS week_start,
        (SUM("participant.kills") + SUM("participant.assists")) / NULLIF(SUM("participant.deaths"), 0) AS kda
    FROM {{ source('lol_raw','data_api_mining') }}
    WHERE CONCAT("participant.riotidgamename", '#', "participant.riotidtagline") = 'Monty Gard#RU1' AND source_nickname = 'Monty Gard#RU1'  AND from_unixtime("info.gamecreation" / 1000) >= current_date - INTERVAL '5' WEEK
    GROUP BY 1, 2, 3
    ORDER BY week_start DESC
    LIMIT 5
)
SELECT 
    CONCAT(player_name, '#', player_tag) AS nickname,
    week_start,
    ROUND(kda, 2) AS avg_kda
FROM weekly_kda
ORDER BY week_start

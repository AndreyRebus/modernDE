{{ config(
    materialized='table'
) }}

{# список Riot IDs можно переопределить через --vars #}
{% set riot_ids = var('riot_ids', [
    'Monty Gard#RU1',
    'Breaksthesilence#RU1',
    '2pilka#RU1',
    'Gruntq#RU1',
    'Шaзам#RU1',
    'Prooaknor#RU1'
]) %}

with nicknames as (
    select nickname
    from (
        values
        {% for n in riot_ids -%}
            ('{{ n }}'){% if not loop.last %},{% endif %}
        {%- endfor %}
    ) as t(nickname)
),

-- последние 5 недель: This Week (0) и 4 прошлые (1..4)
weeks as (
    select
        w as weeks_ago,
        case when w = 0 then 'This Week'
             else concat('Week –', cast(w as varchar))
        end as week_label,
        cast(date_add('week', -w, cast(date_trunc('week', current_date) as date)) as date) as week_start,
        cast(date_add('day', 6, cast(date_add('week', -w, cast(date_trunc('week', current_date) as date)) as date)) as date) as week_end
    from (select sequence(0, 4) as s)
    cross join unnest(s) as t(w)
),

-- каркас всех комбинаций (ник x неделя), чтобы показать недели даже без матчей
frame as (
    select n.nickname, w.week_label, w.week_start, w.week_end
    from nicknames n
    cross join weeks w
),

-- сырые строки матчей только для наших ников и в нужных неделях
rows as (
    select
        f.nickname,
        f.week_label,
        t."metadata.matchid"                                   as match_id,
        t."participant.kills"                                  as kills,
        t."participant.deaths"                                 as deaths,
        t."participant.assists"                                as assists
    from frame f
    join {{ source('lol_raw','data_api_mining') }} t
      on t.source_nickname = f.nickname
     and concat(t."participant.riotidgamename", t."participant.riotidtagline")
         = replace(f.nickname, '#', '')
     and t.event_date between f.week_start and f.week_end
),

-- KDA на уровне матча за выбранную неделю
per_match as (
    select
        nickname,
        week_label,
        match_id,
        kills,
        deaths,
        assists,
        cast(kills + assists as double)
          / cast(greatest(deaths, 1) as double)                as kda_per_match
    from rows
),

-- агрегаты по (ник, неделя)
agg as (
    select
        nickname,
        week_label,
        count(distinct match_id)                                as matches,
        avg(kda_per_match)                                      as avg_kda_per_match,
        -- «суммарный» KDA: (Σ(K+A)) / max(1, ΣD)
        cast(sum(kills + assists) as double)
          / cast(greatest(sum(deaths), 1) as double)            as kda_overall
    from per_match
    group by 1, 2
)

-- джойним к каркасу, чтобы отобразить все 5 недель по каждому нику
select
    f.nickname,
    f.week_label,
    coalesce(a.matches, 0)                                      as matches,
    a.avg_kda_per_match,
    a.kda_overall
from frame f
left join agg a
  on a.nickname = f.nickname
 and a.week_label = f.week_label
order by f.nickname, f.week_start

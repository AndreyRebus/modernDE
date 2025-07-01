
WITH base AS (
    SELECT
        "metadata.matchid"                         AS match_id,
        source_nickname,
        -- ========== метрики из participant/* и info/* ==========
        "participant.totaldamagedealttochampions"  AS dmg_to_champs,
        "participant.totaldamagedealt"             AS dmg_total,
        "participant.damagedealttoturrets"         AS dmg_turrets,
        "participant.damagedealttoobjectives"      AS dmg_objectives,

        "participant.goldearned"                   AS gold_earned,
        "participant.goldspent"                    AS gold_spent,

        "participant.kills"                        AS kills,
        "participant.assists"                      AS assists,
        "participant.deaths"                       AS deaths,

        "participant.totalminionskilled"           AS cs,
        "participant.dragonkills"                  AS dragon_kills,
        "participant.baronkills"                   AS baron_kills,
        "participant.turretkills"                  AS turret_kills,
        "participant.inhibitorkills"               AS inhib_kills,

        "participant.visionwardsboughtingame"      AS pinks,
        "participant.visionscore"                  AS vision_score,
        "participant.timeccingothers"              AS cc_time,
        "participant.damageselfmitigated"          AS dmg_mitigated,

        "participant.firstbloodkill"               AS first_blood_kill,

        "participant.triplekills"                  AS triple_kills,
        "participant.quadrakills"                  AS quadra_kills,
        "participant.pentakills"                   AS penta_kills,

        "participant.totalhealsonteammates"        AS heals_team,
        "participant.totaldamageshieldedonteammates" AS shields_team,
        "participant.longesttimespentliving"       AS longest_life,

        "info.gameduration"                        AS game_duration,

        "participant.objectivesstolen"             AS obj_stolen,
        "participant.objectivesstolenassists"      AS obj_stolen_ast,

        "participant.wardskilled"                  AS wards_killed,
        "participant.wardsplaced"                  AS wards_placed,
        "participant.neutralminionskilled"         AS neutral_kills,

        "participant.physicaldamagedealttochampions" AS phys_dmg,
        "participant.magicdamagedealttochampions"    AS magic_dmg,
        "participant.truedamagedealttochampions"     AS true_dmg,
        "participant.totaldamagetaken"               AS dmg_taken,
        "participant.largestcriticalstrike"          AS largest_crit,

        "participant.doublekills"                  AS double_kills,
        "participant.killingsprees"                AS sprees,

        "participant.totalenemyjungleminionskilled" AS enemy_jungle,

        -- Flash-касты (spellId = 4)
        (CASE WHEN "participant.summoner1id" = 4 THEN "participant.summoner1casts" ELSE 0 END +
         CASE WHEN "participant.summoner2id" = 4 THEN "participant.summoner2casts" ELSE 0 END) AS flash_casts,

        "participant.totaltimespentdead"           AS time_dead,
        "participant.champlevel"                   AS champ_level
    FROM {{source("lol_raw","data_api_mining")}}
    WHERE source_nickname = 'Monty Gard#RU1'
      AND CONCAT("participant.riotidgamename", "participant.riotidtagline") = 'Monty GardRU1'
),

aggregated AS (
    SELECT
        *,
        (gold_earned - gold_spent) AS gold_unspent
    FROM base
)

SELECT
    source_nickname,
    -- ===== 1–20 =====
    MAX(dmg_to_champs)                                    AS best_killer_titan,      -- 1
    MAX(dmg_total)                                        AS best_armada_damage,     -- 2
    MAX(dmg_turrets)                                      AS best_tower_demolisher,  -- 3
    MAX(dmg_objectives)                                   AS best_objective_breaker, -- 4
    MAX(gold_earned)                                      AS best_gold_rain,         -- 5
    MAX(kills)                                            AS best_serial_killer,     -- 6
    MAX(assists)                                          AS best_team_brain,        -- 7
    MAX(cs)                                               AS best_farmer_cs,         -- 8
    MAX(dragon_kills + baron_kills)                       AS best_jungle_king,       -- 9
    MAX(turret_kills)                                     AS best_towerfall,         -- 10
    MAX(inhib_kills)                                      AS best_inhib_breaker,     -- 11
    MAX(pinks)                                            AS best_ward_hunter,       -- 12
    MAX(vision_score)                                     AS best_vision_light,      -- 13
    MAX(cc_time)                                          AS best_cc_master,         -- 14
    MAX(dmg_mitigated)                                    AS best_iron_wall,         -- 15
    MAX(CAST(first_blood_kill AS integer))                AS has_first_blood,        -- 16
    MAX(CASE WHEN deaths = 0 AND (kills + assists) >= 10 THEN 1 ELSE 0 END) AS has_immortal, -- 17
    MAX(triple_kills)                                     AS best_triple_hit,        -- 18
    MAX(quadra_kills)                                     AS best_quadra_champ,      -- 19
    MAX(penta_kills)                                      AS best_penta_master,      -- 20

    -- ===== 21–35 =====
    MAX(heals_team)                                       AS best_life_field,        -- 21
    MAX(shields_team)                                     AS best_unbreak_shield,    -- 22
    MAX(longest_life)                                     AS best_phoenix_life,      -- 23
    MAX(cs * 60.0 / NULLIF(game_duration, 0))             AS best_cspm,              -- 24
    MAX(obj_stolen + obj_stolen_ast)                      AS best_interceptor,       -- 25
    MAX(wards_killed)                                     AS best_map_cleaner,       -- 27
    MAX(wards_placed)                                     AS best_oracle,            -- 28
    MAX(dmg_total / NULLIF(game_duration, 0))             AS best_dpm,               -- 29
    MAX(gold_earned / NULLIF(game_duration, 0))           AS best_gpm,               -- 30
    MAX(enemy_jungle)                                     AS best_jungle_invader,    -- 31
    MAX(neutral_kills)                                    AS best_neutral_lord,      -- 32

    -- ===== 33–47 (без пингов) =====
    MAX(phys_dmg)                                         AS best_phys_thunder,      -- 33
    MAX(magic_dmg)                                        AS best_arcane_master,     -- 34
    MAX(true_dmg)                                         AS best_true_caliber,      -- 35
    MAX(dmg_taken)                                        AS best_heavy_tank,        -- 36
    MAX(largest_crit)                                     AS best_crit_legend,       -- 37
    MAX(double_kills)                                     AS best_double_striker,    -- 38
    MAX(sprees)                                           AS best_snowballer_spree,  -- 39
    MAX(gold_unspent)                                     AS best_banker,            -- 40
    MAX(flash_casts)                                      AS best_flash_fighter,     -- 41
    MIN(time_dead / NULLIF(game_duration, 0))             AS best_undying_ratio,     -- 42
    MAX(champ_level)                                      AS best_exp_master_level,  -- 43
    MAX(heals_team + shields_team)                        AS best_guardian_angel     -- 44
FROM aggregated
GROUP BY source_nickname

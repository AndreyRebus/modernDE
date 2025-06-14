-- init.sql
CREATE TABLE iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(5500),
    property_value VARCHAR(5500),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(5500),
    previous_metadata_location VARCHAR(5500),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE SCHEMA iceberg.lol_raw;


CREATE OR REPLACE TABLE iceberg.lol_raw.data_api_mining (
    -- ====== корневые метаданные матча =====================================
    "metadata.matchId"                   VARCHAR,
    "info.gameCreation"                  BIGINT,
    "info.gameDuration"                  BIGINT,
    "info.gameMode"                      VARCHAR,
    "info.queueId"                       BIGINT,
    "info.gameVersion"                   VARCHAR,

    -- ====== счёт игрока ===================================================
    "participant.PlayerScore0"           BIGINT,
    "participant.PlayerScore1"           BIGINT,
    "participant.PlayerScore10"          BIGINT,
    "participant.PlayerScore11"          BIGINT,
    "participant.PlayerScore2"           BIGINT,
    "participant.PlayerScore3"           BIGINT,
    "participant.PlayerScore4"           BIGINT,
    "participant.PlayerScore5"           BIGINT,
    "participant.PlayerScore6"           BIGINT,
    "participant.PlayerScore7"           BIGINT,
    "participant.PlayerScore8"           BIGINT,
    "participant.PlayerScore9"           BIGINT,

    -- ====== прочие простые поля (без изменений) ===========================
    "participant.allInPings"             BIGINT,
    "participant.assistMePings"          BIGINT,
    "participant.assists"                BIGINT,
    "participant.baronKills"             BIGINT,
    "participant.basicPings"             BIGINT,

    -- ====== ИСПРАВЛЕНО: карты ---> MAP(...) ===============================
    "participant.challenges"             MAP(VARCHAR, DOUBLE),
    "participant.missions"               MAP(VARCHAR, BIGINT),

    -- ====== обычные числовые/строковые поля ===============================
    "participant.champExperience"        BIGINT,
    "participant.champLevel"             BIGINT,
    "participant.championId"             BIGINT,
    "participant.championName"           VARCHAR,
    "participant.championSkinId"         BIGINT,
    "participant.championTransform"      BIGINT,
    "participant.commandPings"           BIGINT,
    "participant.consumablesPurchased"   BIGINT,
    "participant.damageDealtToBuildings" BIGINT,
    "participant.damageDealtToObjectives"BIGINT,
    "participant.damageDealtToTurrets"   BIGINT,
    "participant.damageSelfMitigated"    BIGINT,
    "participant.dangerPings"            BIGINT,
    "participant.deaths"                 BIGINT,
    "participant.detectorWardsPlaced"    BIGINT,
    "participant.doubleKills"            BIGINT,
    "participant.dragonKills"            BIGINT,
    "participant.eligibleForProgression" BOOLEAN,
    "participant.enemyMissingPings"      BIGINT,
    "participant.enemyVisionPings"       BIGINT,
    "participant.firstBloodAssist"       BOOLEAN,
    "participant.firstBloodKill"         BOOLEAN,
    "participant.firstTowerAssist"       BOOLEAN,
    "participant.firstTowerKill"         BOOLEAN,
    "participant.gameEndedInEarlySurrender" BOOLEAN,
    "participant.gameEndedInSurrender"   BOOLEAN,
    "participant.getBackPings"           BIGINT,
    "participant.goldEarned"             BIGINT,
    "participant.goldSpent"              BIGINT,
    "participant.holdPings"              BIGINT,
    "participant.individualPosition"     VARCHAR,
    "participant.inhibitorKills"         BIGINT,
    "participant.inhibitorTakedowns"     BIGINT,
    "participant.inhibitorsLost"         BIGINT,
    "participant.item0"                  BIGINT,
    "participant.item1"                  BIGINT,
    "participant.item2"                  BIGINT,
    "participant.item3"                  BIGINT,
    "participant.item4"                  BIGINT,
    "participant.item5"                  BIGINT,
    "participant.item6"                  BIGINT,
    "participant.itemsPurchased"         BIGINT,
    "participant.killingSprees"          BIGINT,
    "participant.kills"                  BIGINT,
    "participant.lane"                   VARCHAR,
    "participant.largestCriticalStrike"  BIGINT,
    "participant.largestKillingSpree"    BIGINT,
    "participant.largestMultiKill"       BIGINT,
    "participant.longestTimeSpentLiving" BIGINT,
    "participant.magicDamageDealt"       BIGINT,
    "participant.magicDamageDealtToChampions" BIGINT,
    "participant.magicDamageTaken"       BIGINT,
    "participant.needVisionPings"        BIGINT,
    "participant.neutralMinionsKilled"   BIGINT,
    "participant.nexusKills"             BIGINT,
    "participant.nexusLost"              BIGINT,
    "participant.nexusTakedowns"         BIGINT,
    "participant.objectivesStolen"       BIGINT,
    "participant.objectivesStolenAssists"BIGINT,
    "participant.onMyWayPings"           BIGINT,
    "participant.participantId"          BIGINT,
    "participant.pentaKills"             BIGINT,

    -- ====== ИСПРАВЛЕНО: perks ---> сложная ROW ============================
    "participant.perks"                  ROW(
        statPerks ROW(
            offense BIGINT,
            flex    BIGINT,
            defense BIGINT
        ),
        styles ARRAY(
            ROW(
                style       BIGINT,
                description VARCHAR,
                selections  ARRAY(
                    ROW(
                        perk BIGINT,
                        var1 BIGINT,
                        var2 BIGINT,
                        var3 BIGINT
                    )
                )
            )
        )
    ),

    "participant.physicalDamageDealt"             BIGINT,
    "participant.physicalDamageDealtToChampions"  BIGINT,
    "participant.physicalDamageTaken"             BIGINT,
    "participant.placement"                       BIGINT,
    "participant.playerAugment1"                  BIGINT,
    "participant.playerAugment2"                  BIGINT,
    "participant.playerAugment3"                  BIGINT,
    "participant.playerAugment4"                  BIGINT,
    "participant.playerAugment5"                  BIGINT,
    "participant.playerAugment6"                  BIGINT,
    "participant.playerSubteamId"                 BIGINT,
    "participant.profileIcon"                     BIGINT,
    "participant.pushPings"                       BIGINT,
    "participant.puuid"                           VARCHAR,
    "participant.quadraKills"                     BIGINT,
    "participant.retreatPings"                    BIGINT,
    "participant.riotIdGameName"                  VARCHAR,
    "participant.riotIdTagline"                   VARCHAR,
    "participant.role"                            VARCHAR,
    "participant.sightWardsBoughtInGame"          BIGINT,
    "participant.spell1Casts"                     BIGINT,
    "participant.spell2Casts"                     BIGINT,
    "participant.spell3Casts"                     BIGINT,
    "participant.spell4Casts"                     BIGINT,
    "participant.subteamPlacement"                BIGINT,
    "participant.summoner1Casts"                  BIGINT,
    "participant.summoner1Id"                     BIGINT,
    "participant.summoner2Casts"                  BIGINT,
    "participant.summoner2Id"                     BIGINT,
    "participant.summonerId"                      VARCHAR,
    "participant.summonerLevel"                   BIGINT,

    -- ====== ИСПРАВЛЕНО: строки вместо double =============================
    "participant.summonerName"                    VARCHAR,
    "participant.teamEarlySurrendered"            BOOLEAN,
    "participant.teamId"                          BIGINT,
    "participant.teamPosition"                    VARCHAR,

    -- ====== конец обычных полей ==========================================
    "participant.timeCCingOthers"                 BIGINT,
    "participant.timePlayed"                      BIGINT,
    "participant.totalAllyJungleMinionsKilled"    BIGINT,
    "participant.totalDamageDealt"                BIGINT,
    "participant.totalDamageDealtToChampions"     BIGINT,
    "participant.totalDamageShieldedOnTeammates"  BIGINT,
    "participant.totalDamageTaken"                BIGINT,
    "participant.totalEnemyJungleMinionsKilled"   BIGINT,
    "participant.totalHeal"                       BIGINT,
    "participant.totalHealsOnTeammates"           BIGINT,
    "participant.totalMinionsKilled"              BIGINT,
    "participant.totalTimeCCDealt"                BIGINT,
    "participant.totalTimeSpentDead"              BIGINT,
    "participant.totalUnitsHealed"                BIGINT,
    "participant.tripleKills"                     BIGINT,
    "participant.trueDamageDealt"                 BIGINT,
    "participant.trueDamageDealtToChampions"      BIGINT,
    "participant.trueDamageTaken"                 BIGINT,
    "participant.turretKills"                     BIGINT,
    "participant.turretTakedowns"                 BIGINT,
    "participant.turretsLost"                     BIGINT,
    "participant.unrealKills"                     BIGINT,
    "participant.visionClearedPings"              BIGINT,
    "participant.visionScore"                     BIGINT,
    "participant.visionWardsBoughtInGame"         BIGINT,
    "participant.wardsKilled"                     BIGINT,
    "participant.wardsPlaced"                     BIGINT,
    "participant.win"                             BOOLEAN,
    "source_nickname"                             VARCHAR     
)
WITH (
    format = 'PARQUET'
);

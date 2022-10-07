WITH "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS (
    SELECT
      POINT_IN_TIME,
      "cust_id",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 300) / 1800) AS __FB_LAST_TILE_INDEX,
      __FB_LAST_TILE_INDEX - 1 AS __FB_FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "cust_id"
        FROM REQUEST_TABLE
    )
), "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS (
    SELECT
      POINT_IN_TIME,
      "cust_id",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 300) / 1800) AS __FB_LAST_TILE_INDEX,
      __FB_LAST_TILE_INDEX - 4 AS __FB_FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "cust_id"
        FROM REQUEST_TABLE
    )
), "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS (
    SELECT
      POINT_IN_TIME,
      "cust_id",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 300) / 1800) AS __FB_LAST_TILE_INDEX,
      __FB_LAST_TILE_INDEX - 48 AS __FB_FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "cust_id"
        FROM REQUEST_TABLE
    )
), _FB_AGGREGATED AS (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."cust_id",
      REQ."A",
      REQ."B",
      REQ."C",
      "T0"."agg_w1800_sum_8b878f7930698eb4e97cf8e756044109f968dc7a" AS "agg_w1800_sum_8b878f7930698eb4e97cf8e756044109f968dc7a",
      "T1"."agg_w7200_sum_8b878f7930698eb4e97cf8e756044109f968dc7a" AS "agg_w7200_sum_8b878f7930698eb4e97cf8e756044109f968dc7a",
      "T2"."agg_w86400_sum_8b878f7930698eb4e97cf8e756044109f968dc7a" AS "agg_w86400_sum_8b878f7930698eb4e97cf8e756044109f968dc7a"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
        SELECT
          REQ.POINT_IN_TIME,
          REQ."cust_id",
          SUM(value_sum_8b878f7930698eb4e97cf8e756044109f968dc7a) AS "agg_w1800_sum_8b878f7930698eb4e97cf8e756044109f968dc7a"
        FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
        INNER JOIN sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd AS TILE
          ON (FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1) OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1))
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX
          AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        GROUP BY
          REQ.POINT_IN_TIME,
          REQ."cust_id"
    ) AS T0
      ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME
      AND REQ."cust_id" = T0."cust_id"
    LEFT JOIN (
        SELECT
          REQ.POINT_IN_TIME,
          REQ."cust_id",
          SUM(value_sum_8b878f7930698eb4e97cf8e756044109f968dc7a) AS "agg_w7200_sum_8b878f7930698eb4e97cf8e756044109f968dc7a"
        FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS REQ
        INNER JOIN sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd AS TILE
          ON (FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4) OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4))
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX
          AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        GROUP BY
          REQ.POINT_IN_TIME,
          REQ."cust_id"
    ) AS T1
      ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME
      AND REQ."cust_id" = T1."cust_id"
    LEFT JOIN (
        SELECT
          REQ.POINT_IN_TIME,
          REQ."cust_id",
          SUM(value_sum_8b878f7930698eb4e97cf8e756044109f968dc7a) AS "agg_w86400_sum_8b878f7930698eb4e97cf8e756044109f968dc7a"
        FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
        INNER JOIN sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd AS TILE
          ON (FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48) OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48))
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX
          AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        GROUP BY
          REQ.POINT_IN_TIME,
          REQ."cust_id"
    ) AS T2
      ON REQ.POINT_IN_TIME = T2.POINT_IN_TIME
      AND REQ."cust_id" = T2."cust_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  AGG."A",
  AGG."B",
  AGG."C",
  "agg_w86400_sum_8b878f7930698eb4e97cf8e756044109f968dc7a" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG

WITH "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 1 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 4 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
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
    "T0"."_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
    "T1"."_fb_internal_cust_id_window_w7200_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w7200_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
    "T2"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w7200_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."cust_id" = T1."cust_id"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."cust_id" = T2."cust_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  AGG."A",
  AGG."B",
  AGG."C",
  "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG

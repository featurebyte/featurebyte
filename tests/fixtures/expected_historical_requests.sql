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
    "T0"."_fb_internal_window_w1800_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f" AS "_fb_internal_window_w1800_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f",
    "T1"."_fb_internal_window_w7200_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f" AS "_fb_internal_window_w7200_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f",
    "T2"."_fb_internal_window_w86400_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f" AS "_fb_internal_window_w86400_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."cust_id",
      SUM(value_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f) AS "_fb_internal_window_w1800_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f"
    FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
    INNER JOIN TILE_F1800_M300_B600_B839AFCB06ADBAEDCA89907891465110B151C88E AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1)
      )
      AND REQ."cust_id" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."cust_id",
      SUM(value_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f) AS "_fb_internal_window_w7200_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f"
    FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id" AS REQ
    INNER JOIN TILE_F1800_M300_B600_B839AFCB06ADBAEDCA89907891465110B151C88E AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4)
      )
      AND REQ."cust_id" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."cust_id"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."cust_id" = T1."cust_id"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."cust_id",
      SUM(value_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f) AS "_fb_internal_window_w86400_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f"
    FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
    INNER JOIN TILE_F1800_M300_B600_B839AFCB06ADBAEDCA89907891465110B151C88E AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
      )
      AND REQ."cust_id" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."cust_id"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."cust_id" = T2."cust_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  AGG."A",
  AGG."B",
  AGG."C",
  "_fb_internal_window_w86400_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG

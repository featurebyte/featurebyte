WITH "REQUEST_TABLE_W1800_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 1 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W7200_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 4 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W86400_F1800_BS600_M300_NEW_CUST_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "NEW_CUST_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "NEW_CUST_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."NEW_CUST_ID",
    REQ."A",
    REQ."B",
    REQ."C",
    "T0"."agg_w1800_sum_fba233e0f502088c233315a322f4c51e939072c0" AS "agg_w1800_sum_fba233e0f502088c233315a322f4c51e939072c0",
    "T1"."agg_w7200_sum_fba233e0f502088c233315a322f4c51e939072c0" AS "agg_w7200_sum_fba233e0f502088c233315a322f4c51e939072c0",
    "T2"."agg_w86400_sum_fba233e0f502088c233315a322f4c51e939072c0" AS "agg_w86400_sum_fba233e0f502088c233315a322f4c51e939072c0"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID",
      SUM(value_sum_fba233e0f502088c233315a322f4c51e939072c0) AS "agg_w1800_sum_fba233e0f502088c233315a322f4c51e939072c0"
    FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_NEW_CUST_ID" AS REQ
    INNER JOIN TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1)
      )
      AND REQ."NEW_CUST_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T0."NEW_CUST_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID",
      SUM(value_sum_fba233e0f502088c233315a322f4c51e939072c0) AS "agg_w7200_sum_fba233e0f502088c233315a322f4c51e939072c0"
    FROM "REQUEST_TABLE_W7200_F1800_BS600_M300_NEW_CUST_ID" AS REQ
    INNER JOIN TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4)
      )
      AND REQ."NEW_CUST_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T1."NEW_CUST_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID",
      SUM(value_sum_fba233e0f502088c233315a322f4c51e939072c0) AS "agg_w86400_sum_fba233e0f502088c233315a322f4c51e939072c0"
    FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_NEW_CUST_ID" AS REQ
    INNER JOIN TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
      )
      AND REQ."NEW_CUST_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."NEW_CUST_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."NEW_CUST_ID" = T2."NEW_CUST_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."NEW_CUST_ID",
  AGG."A",
  AGG."B",
  AGG."C",
  "agg_w86400_sum_fba233e0f502088c233315a322f4c51e939072c0" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG

SELECT
  "CUSTOMER_ID",
  CAST('_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35' AS VARCHAR) AS "AGGREGATION_RESULT_NAME",
  "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "VALUE"
FROM (
  WITH REQUEST_TABLE AS (
    SELECT DISTINCT
      CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
      "cust_id" AS "CUSTOMER_ID"
    FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
    WHERE
      INDEX >= CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
        ) / 3600
      ) AS BIGINT) - 2
      AND INDEX < CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
        ) / 3600
      ) AS BIGINT)
  ), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM REQUEST_TABLE
    )
  ), _FB_AGGREGATED AS (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        SUM(sum_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35) / SUM(count_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35) AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.count_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35,
          TILE.sum_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35
        FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.count_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35,
          TILE.sum_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35
        FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      )
      GROUP BY
        "POINT_IN_TIME",
        "CUSTOMER_ID"
    ) AS T0
      ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  )
  SELECT
    AGG."POINT_IN_TIME",
    AGG."CUSTOMER_ID",
    "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
  FROM _FB_AGGREGATED AS AGG
)

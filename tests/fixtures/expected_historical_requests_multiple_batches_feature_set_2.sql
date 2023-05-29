CREATE TABLE "__TEMP_646f1b781d1e7970788b32ec_2" AS
WITH "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_ROW_INDEX_FOR_JOIN",
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_lookup_cust_value_1_input_4" AS "_fb_internal_lookup_cust_value_1_input_4",
    "T0"."_fb_internal_lookup_cust_value_2_input_4" AS "_fb_internal_lookup_cust_value_2_input_4",
    "T1"."_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T2"."_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "cust_value_1" AS "_fb_internal_lookup_cust_value_1_input_4",
      "cust_value_2" AS "_fb_internal_lookup_cust_value_2_input_4"
    FROM (
      SELECT
        "cust_id" AS "cust_id",
        "cust_value_1" AS "cust_value_1",
        "cust_value_2" AS "cust_value_2"
      FROM "db"."public"."dimension_table"
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
)
SELECT
  AGG."__FB_ROW_INDEX_FOR_JOIN",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  (
    "_fb_internal_lookup_cust_value_1_input_4" + "_fb_internal_lookup_cust_value_2_input_4"
  ) AS "MY FEATURE",
  "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG

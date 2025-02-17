WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891.INDEX,
    sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891."cust_id",
    value_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            32 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "input_col_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      SUM("input_col_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891") AS value_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id"
  ) AS sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891
), "REQUEST_TABLE_W86400_O28800_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 8 AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 8 - 24 AS __FB_FIRST_TILE_INDEX
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
    "T0"."_fb_internal_CUSTOMER_ID_window_w86400_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891_o28800" AS "_fb_internal_CUSTOMER_ID_window_w86400_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891_o28800"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(value_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891) AS "_fb_internal_CUSTOMER_ID_window_w86400_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891_o28800"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.value_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891
      FROM "REQUEST_TABLE_W86400_O28800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 24) = FLOOR(TILE.INDEX / 24)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.value_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891
      FROM "REQUEST_TABLE_W86400_O28800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 24) - 1 = FLOOR(TILE.INDEX / 24)
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
  CAST("_fb_internal_CUSTOMER_ID_window_w86400_sum_2cbc66fbb52aa9d8124dcb5cfbf1af0d9762e891_o28800" AS DOUBLE) AS "a_sum_24h_offset_8h"
FROM _FB_AGGREGATED AS AGG

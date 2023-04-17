WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_47938f0bfcde2a5c7d483ce1926aa72900653d65.INDEX,
    avg_47938f0bfcde2a5c7d483ce1926aa72900653d65."cust_id",
    sum_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65,
    count_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("a") AS sum_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65,
        COUNT("a") AS count_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            *
          FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "a" AS "a",
              "b" AS "b",
              (
                "a" + "b"
              ) AS "c"
            FROM "db"."public"."event_table"
          )
          WHERE
            "ts" >= CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ)
            AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
        )
      )
      GROUP BY
        tile_index,
        "cust_id"
    )
  ) AS avg_47938f0bfcde2a5c7d483ce1926aa72900653d65
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
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
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T1"."_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T2"."_fb_internal_item_count_None_input_1" AS "_fb_internal_item_count_None_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65) / SUM(count_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65) AS "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
    FROM (
      SELECT
        *,
        FLOOR(__FB_LAST_TILE_INDEX / 2) AS __FB_LAST_TILE_INDEX_DIV_NUM_TILES
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID"
    ) AS REQ
    INNER JOIN (
      SELECT
        *,
        FLOOR(INDEX / 2) AS __FB_TILE_INDEX_DIV_NUM_TILES
      FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
    ) AS TILE
      ON (
        REQ.__FB_LAST_TILE_INDEX_DIV_NUM_TILES = __FB_TILE_INDEX_DIV_NUM_TILES
        OR REQ.__FB_LAST_TILE_INDEX_DIV_NUM_TILES - 1 = __FB_TILE_INDEX_DIV_NUM_TILES
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65) / SUM(count_value_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65) AS "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
    FROM (
      SELECT
        *,
        FLOOR(__FB_LAST_TILE_INDEX / 48) AS __FB_LAST_TILE_INDEX_DIV_NUM_TILES
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID"
    ) AS REQ
    INNER JOIN (
      SELECT
        *,
        FLOOR(INDEX / 48) AS __FB_TILE_INDEX_DIV_NUM_TILES
      FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
    ) AS TILE
      ON (
        REQ.__FB_LAST_TILE_INDEX_DIV_NUM_TILES = __FB_TILE_INDEX_DIV_NUM_TILES
        OR REQ.__FB_LAST_TILE_INDEX_DIV_NUM_TILES - 1 = __FB_TILE_INDEX_DIV_NUM_TILES
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."order_id" AS "order_id",
      COUNT(*) AS "_fb_internal_item_count_None_input_1"
    FROM "REQUEST_TABLE_order_id" AS REQ
    INNER JOIN (
      SELECT
        "order_id" AS "order_id",
        "item_id" AS "item_id",
        "item_name" AS "item_name",
        "item_type" AS "item_type"
      FROM "db"."public"."item_table"
    ) AS ITEM
      ON REQ."order_id" = ITEM."order_id"
    GROUP BY
      REQ."order_id"
  ) AS T2
    ON REQ."order_id" = T2."order_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "a_48h_average",
  "_fb_internal_item_count_None_input_1" AS "order_size"
FROM _FB_AGGREGATED AS AGG

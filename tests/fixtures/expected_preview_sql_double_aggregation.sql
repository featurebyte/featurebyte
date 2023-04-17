WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e.INDEX,
    avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e."cust_id",
    sum_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e,
    count_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-03-21 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("ord_size") AS sum_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e,
        COUNT("ord_size") AS count_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-03-21 09:15:00' AS TIMESTAMPNTZ))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            *
          FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "order_id" AS "order_id",
              "order_method" AS "order_method",
              (
                "_fb_internal_item_count_None_input_1" + 123
              ) AS "ord_size"
            FROM (
              SELECT
                REQ."ts",
                REQ."cust_id",
                REQ."order_id",
                REQ."order_method",
                "T0"."_fb_internal_item_count_None_input_1" AS "_fb_internal_item_count_None_input_1"
              FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "order_id" AS "order_id",
                  "order_method" AS "order_method"
                FROM "db"."public"."event_table"
              ) AS REQ
              LEFT JOIN (
                SELECT
                  ITEM."order_id" AS "order_id",
                  COUNT(*) AS "_fb_internal_item_count_None_input_1"
                FROM (
                  SELECT
                    "order_id" AS "order_id",
                    "item_id" AS "item_id",
                    "item_name" AS "item_name",
                    "item_type" AS "item_type"
                  FROM "db"."public"."item_table"
                ) AS ITEM
                GROUP BY
                  ITEM."order_id"
              ) AS T0
                ON REQ."order_id" = T0."order_id"
            )
          )
          WHERE
            "ts" >= CAST('2022-03-21 09:15:00' AS TIMESTAMPNTZ)
            AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
        )
      )
      GROUP BY
        tile_index,
        "cust_id"
    )
  ) AS avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 720 AS "__FB_FIRST_TILE_INDEX"
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
    "T0"."_fb_internal_window_w2592000_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e" AS "_fb_internal_window_w2592000_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e) / SUM(count_value_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e) AS "_fb_internal_window_w2592000_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e"
    FROM (
      SELECT
        *,
        FLOOR(__FB_LAST_TILE_INDEX / 720) AS __FB_LAST_TILE_INDEX_DIV_NUM_TILES
      FROM "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID"
    ) AS REQ
    INNER JOIN (
      SELECT
        *,
        FLOOR(INDEX / 720) AS __FB_TILE_INDEX_DIV_NUM_TILES
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
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w2592000_avg_b3d693b2f24a3b72386f1591e8cbdd4803cbf72e" AS "order_size_30d_avg"
FROM _FB_AGGREGATED AS AGG

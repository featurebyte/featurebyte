WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec.INDEX,
    avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec."cust_id",
    sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec,
    count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
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
            720 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
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
          (
            "_fb_internal_order_id_item_count_None_order_id_None_input_2" + 123
          ) AS "input_col_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec"
        FROM (
          SELECT
            REQ."ts",
            REQ."cust_id",
            REQ."order_id",
            REQ."order_method",
            "T0"."_fb_internal_order_id_item_count_None_order_id_None_input_2" AS "_fb_internal_order_id_item_count_None_order_id_None_input_2"
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
              COUNT(*) AS "_fb_internal_order_id_item_count_None_order_id_None_input_2"
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
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      SUM("input_col_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec") AS sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec,
      COUNT("input_col_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec") AS count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id"
  ) AS avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
), "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 720 AS __FB_FIRST_TILE_INDEX
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
    "T0"."_fb_internal_CUSTOMER_ID_window_w2592000_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec" AS "_fb_internal_CUSTOMER_ID_window_w2592000_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec) / SUM(count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec) AS "_fb_internal_CUSTOMER_ID_window_w2592000_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec,
        TILE.sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
      FROM "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 720) = FLOOR(TILE.INDEX / 720)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec,
        TILE.sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
      FROM "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 720) - 1 = FLOOR(TILE.INDEX / 720)
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
  CAST("_fb_internal_CUSTOMER_ID_window_w2592000_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec" AS DOUBLE) AS "order_size_30d_avg"
FROM _FB_AGGREGATED AS AGG

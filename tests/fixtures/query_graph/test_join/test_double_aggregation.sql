WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      (
        "_fb_internal_order_id_item_count_None_order_id_None_input_1" + 123
      ) AS "input_col_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec"
    FROM (
      SELECT
        REQ."ts",
        REQ."cust_id",
        REQ."order_id",
        REQ."order_method",
        "T0"."_fb_internal_order_id_item_count_None_order_id_None_input_1" AS "_fb_internal_order_id_item_count_None_order_id_None_input_1"
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
          COUNT(*) AS "_fb_internal_order_id_item_count_None_order_id_None_input_1"
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
    "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
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

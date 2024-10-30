SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "order_id" AS "order_id",
  "order_method" AS "order_method",
  (
    "_fb_internal_order_id_item_count_None_order_id_None_input_1" + 123
  ) AS "ord_size"
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

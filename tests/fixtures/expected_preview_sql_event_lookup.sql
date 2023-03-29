WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    1000 AS "ORDER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."ORDER_ID",
    CASE
      WHEN REQ."POINT_IN_TIME" < "T0"."ts"
      THEN NULL
      ELSE "T0"."_fb_internal_lookup_order_method_input_1"
    END AS "_fb_internal_lookup_order_method_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "order_id" AS "ORDER_ID",
      "order_method" AS "_fb_internal_lookup_order_method_input_1",
      "ts"
    FROM (
      SELECT
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "order_id" AS "order_id",
        "order_method" AS "order_method"
      FROM "db"."public"."event_table"
    )
  ) AS T0
    ON REQ."ORDER_ID" = T0."ORDER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."ORDER_ID",
  "_fb_internal_lookup_order_method_input_1" AS "Order Method"
FROM _FB_AGGREGATED AS AGG

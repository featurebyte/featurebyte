WITH REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS (
  SELECT
    REQ."CUSTOMER_ID",
    T0."a_48h_average",
    SYSDATE() AS POINT_IN_TIME
  FROM (
    SELECT
      1001 AS "CUSTOMER_ID"
    UNION ALL
    SELECT
      1002 AS "CUSTOMER_ID"
    UNION ALL
    SELECT
      1003 AS "CUSTOMER_ID"
  ) AS REQ
  LEFT JOIN online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6 AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."a_48h_average",
    "T0"."count_None_99a214e3edd7fa51" AS "count_None_99a214e3edd7fa51"
  FROM REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS REQ
  LEFT JOIN (
    SELECT
      REQ."order_id",
      COUNT(*) AS "count_None_99a214e3edd7fa51"
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
  ) AS T0
    ON REQ."order_id" = T0."order_id"
)
SELECT
  AGG."CUSTOMER_ID",
  AGG."a_48h_average",
  "count_None_99a214e3edd7fa51" AS "order_size"
FROM _FB_AGGREGATED AS AGG

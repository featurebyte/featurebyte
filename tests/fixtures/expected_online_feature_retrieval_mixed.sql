WITH ONLINE_MY_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."order_id",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM ONLINE_MY_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."order_id",
    REQ."POINT_IN_TIME",
    "T0"."agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489",
    "T0"."agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489",
    "T1"."count_None_99a214e3edd7fa51" AS "count_None_99a214e3edd7fa51"
  FROM ONLINE_MY_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489",
      "agg_w7200_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489"
    FROM online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."order_id" AS "order_id",
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
  ) AS T1
    ON REQ."order_id" = T1."order_id"
)
SELECT
  AGG."CUSTOMER_ID",
  AGG."order_id",
  "agg_w172800_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489" AS "a_48h_average",
  "count_None_99a214e3edd7fa51" AS "order_size"
FROM _FB_AGGREGATED AS AGG

WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
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
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM ONLINE_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T0"."_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T1"."_fb_internal_item_count_None_input_1" AS "_fb_internal_item_count_None_input_1"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
      "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
    FROM online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
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
  ) AS T1
    ON REQ."order_id" = T1."order_id"
)
SELECT
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "a_48h_average",
  "_fb_internal_item_count_None_input_1" AS "order_size"
FROM _FB_AGGREGATED AS AGG

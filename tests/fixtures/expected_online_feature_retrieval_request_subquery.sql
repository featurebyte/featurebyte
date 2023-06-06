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
    "T0"."_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T0"."_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T1"."_fb_internal_item_count_None_order_id_None_input_1" AS "_fb_internal_item_count_None_order_id_None_input_1"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
      "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        """CUSTOMER_ID""" AS "CUSTOMER_ID",
        "'_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
        "'_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
      FROM (
        SELECT
          "CUSTOMER_ID",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb', '_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb', '_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'))
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."order_id" AS "order_id",
      COUNT(*) AS "_fb_internal_item_count_None_order_id_None_input_1"
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
  "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_48h_average",
  "_fb_internal_item_count_None_order_id_None_input_1" AS "order_size"
FROM _FB_AGGREGATED AS AGG

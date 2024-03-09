CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."CUSTOMER_ID",
    REQ."order_id",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."CUSTOMER_ID",
    REQ."order_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
    "T0"."_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
      "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
    FROM (
      SELECT
        "CUSTOMER_ID",
        "'_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35'" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
        "'_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35'" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35"
      FROM (
        SELECT
          "CUSTOMER_ID",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              "AGGREGATION_RESULT_NAME",
              "LATEST_VERSION"
            FROM (VALUES
              ('_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35', 1),
              ('_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35', 0)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35', '_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35', '_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35'))
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."CUSTOMER_ID",
  AGG."order_id",
  "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_000000000000000000000000_1" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."CUSTOMER_ID",
    REQ."order_id",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM ONLINE_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."CUSTOMER_ID",
    REQ."order_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_order_id_item_count_None_order_id_None_input_2" AS "_fb_internal_order_id_item_count_None_order_id_None_input_2"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."order_id" AS "order_id",
      COUNT(*) AS "_fb_internal_order_id_item_count_None_order_id_None_input_2"
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
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."CUSTOMER_ID",
  AGG."order_id",
  "_fb_internal_order_id_item_count_None_order_id_None_input_2" AS "order_size"
FROM _FB_AGGREGATED AS AGG;

SELECT
  REQ."CUSTOMER_ID",
  REQ."order_id",
  T0."a_48h_average",
  T1."order_size"
FROM REQUEST_TABLE_1234 AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX"
LEFT JOIN "__TEMP_000000000000000000000000_1" AS T1
  ON REQ."__FB_TABLE_ROW_INDEX" = T1."__FB_TABLE_ROW_INDEX"

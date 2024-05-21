WITH ONLINE_REQUEST_TABLE AS (
  SELECT
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
    REQ."CUSTOMER_ID",
    REQ."order_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
    "T0"."_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
    "T1"."_fb_internal_order_id_item_count_None_order_id_None_input_2" AS "_fb_internal_order_id_item_count_None_order_id_None_input_2"
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
              ('_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35', _fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35_VERSION_PLACEHOLDER),
              ('_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35', _fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
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
  ) AS T1
    ON REQ."order_id" = T1."order_id"
)
SELECT
  AGG."CUSTOMER_ID",
  AGG."order_id",
  CAST("_fb_internal_CUSTOMER_ID_window_w172800_avg_f37862722c21105449ad882409cf62a1ff7f5b35" AS DOUBLE) AS "a_48h_average",
  CAST("_fb_internal_order_id_item_count_None_order_id_None_input_2" AS BIGINT) AS "order_size"
FROM _FB_AGGREGATED AS AGG

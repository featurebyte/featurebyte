WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_forward_sum_a_biz_id_None_input_1" AS "_fb_internal_forward_sum_a_biz_id_None_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."BUSINESS_ID" AS "BUSINESS_ID",
      SUM(TABLE."a") AS "_fb_internal_forward_sum_a_biz_id_None_input_1"
    FROM "REQUEST_TABLE_POINT_IN_TIME_BUSINESS_ID" AS REQ
    INNER JOIN (
      SELECT
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "a" AS "a",
        "b" AS "b",
        (
          "a" + "b"
        ) AS "c"
      FROM "db"."public"."event_table"
    ) AS TABLE
      ON DATE_PART(EPOCH_SECOND, TABLE."timestamp_col") > REQ."POINT_IN_TIME"
      AND DATE_PART(EPOCH_SECOND, TABLE."timestamp_col") <= FLOOR(DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME") + 604800.0)
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."BUSINESS_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."BUSINESS_ID" = T0."BUSINESS_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_forward_sum_a_biz_id_None_input_1" AS "target_node"
FROM _FB_AGGREGATED AS AGG

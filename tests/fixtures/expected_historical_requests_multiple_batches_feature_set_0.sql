CREATE TABLE "__TEMP_646f1b781d1e7970788b32ec_0" AS
WITH "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE
), "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "MEMBERSHIP_STATUS"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_ROW_INDEX_FOR_JOIN",
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_item_count_None_order_id_None_input_1" AS "_fb_internal_item_count_None_order_id_None_input_1",
    "T1"."_fb_internal_as_at_count_None_membership_status_None_input_2" AS "_fb_internal_as_at_count_None_membership_status_None_input_2"
  FROM REQUEST_TABLE AS REQ
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
  ) AS T0
    ON REQ."order_id" = T0."order_id"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS" AS "MEMBERSHIP_STATUS",
      COUNT(*) AS "_fb_internal_as_at_count_None_membership_status_None_input_2"
    FROM "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS REQ
    INNER JOIN (
      SELECT
        *,
        LEAD("effective_ts") OVER (PARTITION BY "cust_id" ORDER BY "effective_ts") AS "__FB_END_TS"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
      )
    ) AS SCD
      ON REQ."MEMBERSHIP_STATUS" = SCD."membership_status"
      AND (
        SCD."effective_ts" <= REQ."POINT_IN_TIME"
        AND (
          SCD."__FB_END_TS" > REQ."POINT_IN_TIME" OR SCD."__FB_END_TS" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME"
    AND REQ."MEMBERSHIP_STATUS" = T1."MEMBERSHIP_STATUS"
)
SELECT
  AGG."__FB_ROW_INDEX_FOR_JOIN",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_as_at_count_None_membership_status_None_input_2" AS "asat_feature",
  "_fb_internal_item_count_None_order_id_None_input_1" AS "order_size"
FROM _FB_AGGREGATED AS AGG

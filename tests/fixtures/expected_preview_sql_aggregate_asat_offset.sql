WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "MEMBERSHIP_STATUS"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_7d_input_1" AS "_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_7d_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS" AS "MEMBERSHIP_STATUS",
      COUNT(*) AS "_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_7d_input_1"
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
        SCD."effective_ts" <= DATEADD(MICROSECOND, -604800000000.0, REQ."POINT_IN_TIME")
        AND (
          SCD."__FB_END_TS" > DATEADD(MICROSECOND, -604800000000.0, REQ."POINT_IN_TIME")
          OR SCD."__FB_END_TS" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
    AND REQ."MEMBERSHIP_STATUS" = T0."MEMBERSHIP_STATUS"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_7d_input_1" AS BIGINT) AS "asat_feature_offset_7d"
FROM _FB_AGGREGATED AS AGG

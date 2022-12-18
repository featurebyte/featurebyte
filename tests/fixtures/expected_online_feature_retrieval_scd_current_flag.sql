WITH REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS (
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
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    "T0"."membership_status_a18d6f89f8538bdb" AS "membership_status_a18d6f89f8538bdb"
  FROM REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "membership_status" AS "membership_status_a18d6f89f8538bdb"
    FROM (
      SELECT
        "effective_ts" AS "effective_ts",
        "cust_id" AS "cust_id",
        "membership_status" AS "membership_status"
      FROM "db"."public"."customer_profile_table"
      WHERE
        is_record_current = TRUE
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  "membership_status_a18d6f89f8538bdb" AS "Current Membership Status"
FROM _FB_AGGREGATED AS AGG

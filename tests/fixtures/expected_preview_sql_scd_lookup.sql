WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."membership_status_a18d6f89f8538bdb" AS "membership_status_a18d6f89f8538bdb"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R."membership_status" AS "membership_status_a18d6f89f8538bdb"
    FROM (
      SELECT
        "__FB_KEY_COL",
        "__FB_LAST_TS",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL" ORDER BY "__FB_TS_COL" NULLS LAST, "__FB_EFFECTIVE_TS_COL" NULLS LAST) AS "__FB_LAST_TS",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            "POINT_IN_TIME" AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID"
            FROM REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            "event_timestamp" AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        "effective_ts" AS "effective_ts",
        "cust_id" AS "cust_id",
        "membership_status" AS "membership_status"
      FROM "db"."public"."customer_profile_table"
    ) AS R
      ON L."__FB_LAST_TS" = R."event_timestamp" AND L."__FB_KEY_COL" = R."cust_id"
  ) AS REQ
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "membership_status_a18d6f89f8538bdb" AS "Current Membership Status"
FROM _FB_AGGREGATED AS AGG

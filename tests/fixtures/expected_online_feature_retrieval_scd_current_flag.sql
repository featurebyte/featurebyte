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
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1"
  FROM (
    SELECT
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      R."membership_status" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "CUSTOMER_ID",
        "POINT_IN_TIME"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
          "CUSTOMER_ID",
          "POINT_IN_TIME",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "CUSTOMER_ID" AS "CUSTOMER_ID",
            "POINT_IN_TIME" AS "POINT_IN_TIME"
          FROM (
            SELECT
              REQ."CUSTOMER_ID",
              REQ."POINT_IN_TIME"
            FROM ONLINE_REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "CUSTOMER_ID",
            NULL AS "POINT_IN_TIME"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
            WHERE
              "event_timestamp" IS NOT NULL
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        ANY_VALUE("effective_ts") AS "effective_ts",
        "cust_id",
        ANY_VALUE("membership_status") AS "membership_status"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
        WHERE
          "event_timestamp" IS NOT NULL
      )
      GROUP BY
        "event_timestamp",
        "cust_id"
    ) AS R
      ON L."__FB_LAST_TS" = R."event_timestamp" AND L."__FB_KEY_COL_0" = R."cust_id"
  ) AS REQ
)
SELECT
  AGG."CUSTOMER_ID",
  "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1" AS "Current Membership Status"
FROM _FB_AGGREGATED AS AGG

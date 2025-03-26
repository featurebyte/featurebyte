SELECT
  L."ts" AS "ts",
  L."cust_id" AS "cust_id",
  L."order_id" AS "order_id",
  L."order_method" AS "order_method",
  R."membership_status" AS "latest_membership_status"
FROM (
  SELECT
    "__FB_KEY_COL_0",
    "__FB_LAST_TS",
    "ts",
    "cust_id",
    "order_id",
    "order_method"
  FROM (
    SELECT
      "__FB_KEY_COL_0",
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
      "ts",
      "cust_id",
      "order_id",
      "order_method",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
        "cust_id" AS "__FB_KEY_COL_0",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        2 AS "__FB_TS_TIE_BREAKER_COL",
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "order_id" AS "order_id",
        "order_method" AS "order_method"
      FROM (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "order_id" AS "order_id",
          "order_method" AS "order_method"
        FROM "db"."public"."event_table"
      )
      UNION ALL
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
        "cust_id" AS "__FB_KEY_COL_0",
        "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "ts",
        NULL AS "cust_id",
        NULL AS "order_id",
        NULL AS "order_method"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
        WHERE
          "effective_timestamp" IS NOT NULL
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
      "effective_timestamp" IS NOT NULL
  )
  GROUP BY
    "effective_timestamp",
    "cust_id"
) AS R
  ON L."__FB_LAST_TS" = R."effective_timestamp" AND L."__FB_KEY_COL_0" = R."cust_id"

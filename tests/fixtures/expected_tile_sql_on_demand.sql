SELECT
  index,
  "cust_id",
  SUM(CAST("latest_membership_status" IS NULL AS INTEGER)) AS value_na_count_f8628aa072d8cf7c6746201e41a74890b1a9b8a8
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      __FB_ENTITY_TABLE_SQL_PLACEHOLDER
    )
    SELECT
      R.*
    FROM __FB_ENTITY_TABLE_NAME
    INNER JOIN (
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
            LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
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
    ) AS R
      ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
      AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
      AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
  )
)
GROUP BY
  index,
  "cust_id"

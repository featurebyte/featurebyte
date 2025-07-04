SELECT
  COUNT(*) AS "count"
FROM (
  SELECT
    CAST("event_partition_col" AS VARCHAR) AS "event_partition_col",
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "event_id" AS "event_id"
  FROM "sf_database"."sf_schema"."EVENT"
  WHERE
    (
      "event_partition_col" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
      AND "event_partition_col" <= TO_CHAR(CAST('2025-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
    )
    AND (
      "ts" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
      AND "ts" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
    )
);

CREATE TABLE "__FB_TEMPORARY_TABLE_000000000000000000000000" AS
SELECT
  "event_partition_col",
  "ts",
  "cust_id",
  "event_id"
FROM (
  SELECT
    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "event_partition_col",
    "ts",
    "cust_id",
    "event_id"
  FROM (
    SELECT
      CAST("event_partition_col" AS VARCHAR) AS "event_partition_col",
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "event_id" AS "event_id"
    FROM "sf_database"."sf_schema"."EVENT"
    WHERE
      (
        "event_partition_col" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
        AND "event_partition_col" <= TO_CHAR(CAST('2025-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
      )
      AND (
        "ts" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
        AND "ts" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
      )
  )
)
WHERE
  "prob" <= 0.15000000000000002
ORDER BY
  "prob"
LIMIT 10;

SELECT
  IFF(
    CAST(L."ts" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
    OR CAST(L."ts" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
    NULL,
    L."ts"
  ) AS "ts",
  L."cust_id" AS "cust_id",
  L."event_id" AS "event_id",
  R."scd_value" AS "scd_value_latest"
FROM (
  SELECT
    "__FB_KEY_COL_0",
    "__FB_LAST_TS",
    "__FB_TS_COL",
    "ts",
    "cust_id",
    "event_id"
  FROM (
    SELECT
      "__FB_KEY_COL_0",
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
      "__FB_TS_COL",
      "ts",
      "cust_id",
      "event_id",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP) AS "__FB_TS_COL",
        "cust_id" AS "__FB_KEY_COL_0",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        2 AS "__FB_TS_TIE_BREAKER_COL",
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "event_id" AS "event_id"
      FROM (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "event_id" AS "event_id"
        FROM "__FB_TEMPORARY_TABLE_000000000000000000000000"
      )
      UNION ALL
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "effective_ts") AS TIMESTAMP) AS "__FB_TS_COL",
        "scd_cust_id" AS "__FB_KEY_COL_0",
        "effective_ts" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "ts",
        NULL AS "cust_id",
        NULL AS "event_id"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "end_ts" AS "end_ts",
          "scd_cust_id" AS "scd_cust_id",
          "scd_value" AS "scd_value"
        FROM "sf_database"."sf_schema"."SCD"
        WHERE
          "effective_ts" IS NOT NULL
      )
    )
  )
  WHERE
    "__FB_EFFECTIVE_TS_COL" IS NULL
) AS L
LEFT JOIN (
  SELECT
    "effective_ts",
    ANY_VALUE("end_ts") AS "end_ts",
    "scd_cust_id",
    ANY_VALUE("scd_value") AS "scd_value"
  FROM (
    SELECT
      "effective_ts" AS "effective_ts",
      "end_ts" AS "end_ts",
      "scd_cust_id" AS "scd_cust_id",
      "scd_value" AS "scd_value"
    FROM "sf_database"."sf_schema"."SCD"
    WHERE
      "effective_ts" IS NOT NULL
  )
  GROUP BY
    "effective_ts",
    "scd_cust_id"
) AS R
  ON L."__FB_LAST_TS" = R."effective_ts"
  AND L."__FB_KEY_COL_0" = R."scd_cust_id"
  AND (
    L."__FB_TS_COL" < TO_TIMESTAMP(R."end_ts", 'YYYY|MM|DD|HH24:MI:SS')
    OR R."end_ts" IS NULL
  )
WHERE
  "ts" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
  AND "ts" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
LIMIT 10;

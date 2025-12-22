SELECT
  COUNT(*) AS "count"
FROM (
  SELECT
    "col_int" AS "col_int",
    "col_float" AS "col_float",
    "col_char" AS "col_char",
    CAST("col_text" AS VARCHAR) AS "col_text",
    "col_binary" AS "col_binary",
    "col_boolean" AS "col_boolean",
    "event_timestamp" AS "event_timestamp",
    "created_at" AS "created_at",
    CAST("cust_id" AS VARCHAR) AS "cust_id",
    CAST("partition_col" AS VARCHAR) AS "partition_col"
  FROM "db"."schema"."dev_table"
  WHERE
    (
      "partition_col" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
      AND "partition_col" <= TO_CHAR(CAST('2025-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
    )
    AND (
      "event_timestamp" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
      AND "event_timestamp" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
    )
);

CREATE TABLE "__FB_TEMPORARY_TABLE_000000000000000000000000" AS
SELECT
  "col_int",
  "col_float",
  "col_char",
  "col_text",
  "col_binary",
  "col_boolean",
  "event_timestamp",
  "created_at",
  "cust_id",
  "partition_col"
FROM (
  SELECT
    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "col_int",
    "col_float",
    "col_char",
    "col_text",
    "col_binary",
    "col_boolean",
    "event_timestamp",
    "created_at",
    "cust_id",
    "partition_col"
  FROM (
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      "event_timestamp" AS "event_timestamp",
      "created_at" AS "created_at",
      CAST("cust_id" AS VARCHAR) AS "cust_id",
      CAST("partition_col" AS VARCHAR) AS "partition_col"
    FROM "db"."schema"."dev_table"
    WHERE
      (
        "partition_col" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
        AND "partition_col" <= TO_CHAR(CAST('2025-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
      )
      AND (
        "event_timestamp" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
        AND "event_timestamp" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
      )
  )
)
WHERE
  "prob" <= 0.75
ORDER BY
  "prob"
LIMIT 50;

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
  "prob" <= 0.15000000000000002
ORDER BY
  "prob"
LIMIT 10;

CREATE TABLE "__FB_TEMPORARY_TABLE_000000000000000000000001" AS
SELECT
  "col_int" AS "col_int",
  "col_float" AS "col_float",
  "col_char" AS "col_char",
  CAST("col_text" AS VARCHAR) AS "col_text",
  "col_binary" AS "col_binary",
  "col_boolean" AS "col_boolean",
  "event_timestamp" AS "event_timestamp",
  "created_at" AS "created_at",
  CAST("cust_id" AS VARCHAR) AS "cust_id"
FROM "__FB_TEMPORARY_TABLE_000000000000000000000000"
WHERE
  "event_timestamp" >= CAST('2023-01-01T00:00:00' AS TIMESTAMP)
  AND "event_timestamp" < CAST('2025-01-01T00:00:00' AS TIMESTAMP)
LIMIT 10;

WITH "casted_data" AS (
  SELECT
    CAST("col_int" AS VARCHAR) AS "col_int",
    CAST("col_float" AS VARCHAR) AS "col_float",
    CAST("col_char" AS VARCHAR) AS "col_char",
    CAST("col_text" AS VARCHAR) AS "col_text",
    CAST("col_binary" AS VARCHAR) AS "col_binary",
    CAST("col_boolean" AS VARCHAR) AS "col_boolean",
    CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
    CAST("created_at" AS VARCHAR) AS "created_at",
    CAST("cust_id" AS VARCHAR) AS "cust_id"
  FROM "__FB_TEMPORARY_TABLE_000000000000000000000001"
), counts__0 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__0",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__0"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "col_int" IS NULL THEN '__MISSING__' ELSE "col_int" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "col_int",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "col_int"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 1
    ) AS cat_counts
  ) AS count_dict
), counts__2 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__2",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__2",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__2"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "col_char" IS NULL THEN '__MISSING__' ELSE "col_char" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "col_char",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "col_char"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__3 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__3",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__3",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__3"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "col_text" IS NULL THEN '__MISSING__' ELSE "col_text" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "col_text",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "col_text"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__5 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__5",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__5"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "col_boolean" IS NULL THEN '__MISSING__' ELSE "col_boolean" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "col_boolean",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "col_boolean"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 1
    ) AS cat_counts
  ) AS count_dict
), counts__8 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__8",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__8",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__8"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "cust_id" IS NULL THEN '__MISSING__' ELSE "cust_id" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "cust_id",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "cust_id"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    COUNT(DISTINCT "col_int") AS "unique__0",
    (
      1.0 - COUNT("col_int") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__0",
    NULL AS "%empty__0",
    AVG(CAST("col_int" AS DOUBLE)) AS "mean__0",
    STDDEV(CAST("col_int" AS DOUBLE)) AS "std__0",
    MIN("col_int") AS "min__0",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "col_int") AS "25%__0",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "col_int") AS "50%__0",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "col_int") AS "75%__0",
    MAX("col_int") AS "max__0",
    NULL AS "min TZ offset__0",
    NULL AS "max TZ offset__0",
    COUNT(DISTINCT "col_float") AS "unique__1",
    (
      1.0 - COUNT("col_float") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__1",
    NULL AS "%empty__1",
    AVG(CAST("col_float" AS DOUBLE)) AS "mean__1",
    STDDEV(CAST("col_float" AS DOUBLE)) AS "std__1",
    MIN("col_float") AS "min__1",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "col_float") AS "25%__1",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "col_float") AS "50%__1",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "col_float") AS "75%__1",
    MAX("col_float") AS "max__1",
    NULL AS "min TZ offset__1",
    NULL AS "max TZ offset__1",
    COUNT(DISTINCT "col_char") AS "unique__2",
    (
      1.0 - COUNT("col_char") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__2",
    COUNT_IF("col_char" = '') AS "%empty__2",
    NULL AS "mean__2",
    NULL AS "std__2",
    NULL AS "min__2",
    NULL AS "25%__2",
    NULL AS "50%__2",
    NULL AS "75%__2",
    NULL AS "max__2",
    NULL AS "min TZ offset__2",
    NULL AS "max TZ offset__2",
    COUNT(DISTINCT "col_text") AS "unique__3",
    (
      1.0 - COUNT("col_text") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__3",
    COUNT_IF("col_text" = '') AS "%empty__3",
    NULL AS "mean__3",
    NULL AS "std__3",
    NULL AS "min__3",
    NULL AS "25%__3",
    NULL AS "50%__3",
    NULL AS "75%__3",
    NULL AS "max__3",
    NULL AS "min TZ offset__3",
    NULL AS "max TZ offset__3",
    COUNT(DISTINCT "col_binary") AS "unique__4",
    (
      1.0 - COUNT("col_binary") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__4",
    NULL AS "%empty__4",
    NULL AS "mean__4",
    NULL AS "std__4",
    NULL AS "min__4",
    NULL AS "25%__4",
    NULL AS "50%__4",
    NULL AS "75%__4",
    NULL AS "max__4",
    NULL AS "min TZ offset__4",
    NULL AS "max TZ offset__4",
    COUNT(DISTINCT "col_boolean") AS "unique__5",
    (
      1.0 - COUNT("col_boolean") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__5",
    NULL AS "%empty__5",
    NULL AS "mean__5",
    NULL AS "std__5",
    NULL AS "min__5",
    NULL AS "25%__5",
    NULL AS "50%__5",
    NULL AS "75%__5",
    NULL AS "max__5",
    NULL AS "min TZ offset__5",
    NULL AS "max TZ offset__5",
    COUNT(DISTINCT "event_timestamp") AS "unique__6",
    (
      1.0 - COUNT("event_timestamp") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__6",
    NULL AS "%empty__6",
    NULL AS "mean__6",
    NULL AS "std__6",
    MIN(
      IFF(
        CAST("event_timestamp" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
        OR CAST("event_timestamp" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "event_timestamp"
      )
    ) AS "min__6",
    NULL AS "25%__6",
    NULL AS "50%__6",
    NULL AS "75%__6",
    MAX(
      IFF(
        CAST("event_timestamp" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
        OR CAST("event_timestamp" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "event_timestamp"
      )
    ) AS "max__6",
    NULL AS "min TZ offset__6",
    NULL AS "max TZ offset__6",
    COUNT(DISTINCT "created_at") AS "unique__7",
    (
      1.0 - COUNT("created_at") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__7",
    NULL AS "%empty__7",
    NULL AS "mean__7",
    NULL AS "std__7",
    MIN(
      IFF(
        CAST("created_at" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
        OR CAST("created_at" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "created_at"
      )
    ) AS "min__7",
    NULL AS "25%__7",
    NULL AS "50%__7",
    NULL AS "75%__7",
    MAX(
      IFF(
        CAST("created_at" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
        OR CAST("created_at" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "created_at"
      )
    ) AS "max__7",
    NULL AS "min TZ offset__7",
    NULL AS "max TZ offset__7",
    COUNT(DISTINCT "cust_id") AS "unique__8",
    (
      1.0 - COUNT("cust_id") / NULLIF(COUNT(*), 0)
    ) * 100 AS "%missing__8",
    COUNT_IF("cust_id" = '') AS "%empty__8",
    NULL AS "mean__8",
    NULL AS "std__8",
    NULL AS "min__8",
    NULL AS "25%__8",
    NULL AS "50%__8",
    NULL AS "75%__8",
    NULL AS "max__8",
    NULL AS "min TZ offset__8",
    NULL AS "max TZ offset__8"
  FROM "__FB_TEMPORARY_TABLE_000000000000000000000001"
), joined_tables_0 AS (
  SELECT
    *
  FROM stats
  LEFT JOIN counts__0
    ON 1 = 1
  LEFT JOIN counts__2
    ON 1 = 1
  LEFT JOIN counts__3
    ON 1 = 1
  LEFT JOIN counts__5
    ON 1 = 1
  LEFT JOIN counts__8
    ON 1 = 1
)
SELECT
  'INT' AS "dtype__0",
  "unique__0",
  "%missing__0",
  "%empty__0",
  NULL AS "entropy__0",
  "top__0",
  "freq__0",
  "mean__0",
  "std__0",
  "min__0",
  "25%__0",
  "50%__0",
  "75%__0",
  "max__0",
  "min TZ offset__0",
  "max TZ offset__0",
  'FLOAT' AS "dtype__1",
  "unique__1",
  "%missing__1",
  "%empty__1",
  NULL AS "entropy__1",
  NULL AS "top__1",
  NULL AS "freq__1",
  "mean__1",
  "std__1",
  "min__1",
  "25%__1",
  "50%__1",
  "75%__1",
  "max__1",
  "min TZ offset__1",
  "max TZ offset__1",
  'CHAR' AS "dtype__2",
  "unique__2",
  "%missing__2",
  "%empty__2",
  "entropy__2",
  "top__2",
  "freq__2",
  "mean__2",
  "std__2",
  "min__2",
  "25%__2",
  "50%__2",
  "75%__2",
  "max__2",
  "min TZ offset__2",
  "max TZ offset__2",
  'VARCHAR' AS "dtype__3",
  "unique__3",
  "%missing__3",
  "%empty__3",
  "entropy__3",
  "top__3",
  "freq__3",
  "mean__3",
  "std__3",
  "min__3",
  "25%__3",
  "50%__3",
  "75%__3",
  "max__3",
  "min TZ offset__3",
  "max TZ offset__3",
  'BINARY' AS "dtype__4",
  "unique__4",
  "%missing__4",
  "%empty__4",
  NULL AS "entropy__4",
  NULL AS "top__4",
  NULL AS "freq__4",
  "mean__4",
  "std__4",
  "min__4",
  "25%__4",
  "50%__4",
  "75%__4",
  "max__4",
  "min TZ offset__4",
  "max TZ offset__4",
  'BOOL' AS "dtype__5",
  "unique__5",
  "%missing__5",
  "%empty__5",
  NULL AS "entropy__5",
  "top__5",
  "freq__5",
  "mean__5",
  "std__5",
  "min__5",
  "25%__5",
  "50%__5",
  "75%__5",
  "max__5",
  "min TZ offset__5",
  "max TZ offset__5",
  'TIMESTAMP' AS "dtype__6",
  "unique__6",
  "%missing__6",
  "%empty__6",
  NULL AS "entropy__6",
  NULL AS "top__6",
  NULL AS "freq__6",
  "mean__6",
  "std__6",
  "min__6",
  "25%__6",
  "50%__6",
  "75%__6",
  "max__6",
  "min TZ offset__6",
  "max TZ offset__6",
  'TIMESTAMP' AS "dtype__7",
  "unique__7",
  "%missing__7",
  "%empty__7",
  NULL AS "entropy__7",
  NULL AS "top__7",
  NULL AS "freq__7",
  "mean__7",
  "std__7",
  "min__7",
  "25%__7",
  "50%__7",
  "75%__7",
  "max__7",
  "min TZ offset__7",
  "max TZ offset__7",
  'VARCHAR' AS "dtype__8",
  "unique__8",
  "%missing__8",
  "%empty__8",
  "entropy__8",
  "top__8",
  "freq__8",
  "mean__8",
  "std__8",
  "min__8",
  "25%__8",
  "50%__8",
  "75%__8",
  "max__8",
  "min TZ offset__8",
  "max TZ offset__8"
FROM joined_tables_0;

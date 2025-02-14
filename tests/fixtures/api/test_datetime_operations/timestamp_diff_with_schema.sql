SELECT
  "col_int" AS "col_int",
  "col_float" AS "col_float",
  "col_char" AS "col_char",
  CAST("col_text" AS VARCHAR) AS "col_text",
  "col_binary" AS "col_binary",
  "col_boolean" AS "col_boolean",
  CAST("date" AS VARCHAR) AS "date",
  "store_id" AS "store_id",
  CAST("another_timestamp_col" AS VARCHAR) AS "another_timestamp_col",
  (
    DATEDIFF(
      MICROSECOND,
      "another_timestamp_col",
      CONVERT_TIMEZONE('Etc/UTC', 'UTC', TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS'))
    ) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT)
  ) AS "new_col"
FROM "sf_database"."sf_schema"."time_series_table"
LIMIT 10

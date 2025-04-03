SELECT
  "col_int" AS "col_int",
  "col_float" AS "col_float",
  "col_char" AS "col_char",
  CAST("col_text" AS VARCHAR) AS "col_text",
  "col_binary" AS "col_binary",
  "col_boolean" AS "col_boolean",
  CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
  "cust_id" AS "cust_id",
  CAST("col_int" AS TIMESTAMP) AS "converted_timestamp",
  DATE_PART(hour, CAST("col_int" AS TIMESTAMP)) AS "converted_timestamp_hour"
FROM "sf_database"."sf_schema"."sf_table"
LIMIT 10

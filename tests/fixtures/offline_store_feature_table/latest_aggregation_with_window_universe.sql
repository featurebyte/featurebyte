SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM (
  SELECT
    "col_int" AS "col_int",
    "col_float" AS "col_float",
    "col_char" AS "col_char",
    "col_text" AS "col_text",
    "col_binary" AS "col_binary",
    "col_boolean" AS "col_boolean",
    "event_timestamp" AS "event_timestamp",
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."sf_table"
  WHERE
    "event_timestamp" >= CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
    ) / 1800) * 1800 + 300 - 600 - 7776000 AS TIMESTAMP)
    AND "event_timestamp" < CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
    ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
)
WHERE
  "cust_id" IS NOT NULL

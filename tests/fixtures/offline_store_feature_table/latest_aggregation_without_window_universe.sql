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
    "event_timestamp" >= __fb_last_materialized_timestamp
    AND "event_timestamp" < CAST(CONVERT_TIMEZONE(
      'UTC',
      F_INDEX_TO_TIMESTAMP(
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
        ) / 1800) AS BIGINT),
        300,
        600,
        30
      )
    ) AS TIMESTAMP)
)
WHERE
  "cust_id" IS NOT NULL

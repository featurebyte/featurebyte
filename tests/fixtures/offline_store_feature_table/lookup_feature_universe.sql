SELECT DISTINCT
  "col_text" AS "cust_id"
FROM (
  SELECT
    "col_int" AS "col_int",
    "col_float" AS "col_float",
    "col_text" AS "col_text",
    "col_binary" AS "col_binary",
    "col_boolean" AS "col_boolean",
    "effective_timestamp" AS "effective_timestamp",
    "end_timestamp" AS "end_timestamp",
    "date_of_birth" AS "date_of_birth",
    "created_at" AS "created_at",
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."scd_table"
  WHERE
    "effective_timestamp" >= __fb_last_materialized_timestamp
    AND "effective_timestamp" < __fb_current_feature_timestamp
)
WHERE
  "col_text" IS NOT NULL

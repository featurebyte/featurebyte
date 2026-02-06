SELECT DISTINCT
  "col_boolean" AS "gender"
FROM (
  SELECT
    "col_boolean" AS "col_boolean"
  FROM "sf_database"."sf_schema"."scd_table"
  WHERE
    "effective_timestamp" >= __fb_last_materialized_timestamp
    AND "effective_timestamp" < __fb_current_feature_timestamp
)
WHERE
  "col_boolean" IS NOT NULL

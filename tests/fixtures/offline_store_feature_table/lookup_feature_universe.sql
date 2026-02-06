SELECT DISTINCT
  "col_text" AS "cust_id"
FROM (
  SELECT
    "col_text" AS "col_text"
  FROM "sf_database"."sf_schema"."scd_table"
  WHERE
    "effective_timestamp" >= __fb_last_materialized_timestamp
    AND "effective_timestamp" < __fb_current_feature_timestamp
)
WHERE
  "col_text" IS NOT NULL

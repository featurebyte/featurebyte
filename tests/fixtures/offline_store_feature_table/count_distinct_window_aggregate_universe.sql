SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM (
  SELECT
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."sf_table"
  WHERE
    "event_timestamp" >= CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
    ) / 1800) * 1800 + 300 - 600 - 172800 AS TIMESTAMP)
    AND "event_timestamp" < CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
    ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
)
WHERE
  "cust_id" IS NOT NULL

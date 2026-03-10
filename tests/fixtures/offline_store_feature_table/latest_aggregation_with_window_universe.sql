SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM (
  SELECT
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."sf_table"
  WHERE
    (
      "event_timestamp" >= DATEADD(MONTH, -1, DATEADD(MINUTE, -129600, __fb_current_feature_timestamp))
      AND "event_timestamp" <= DATEADD(MONTH, 1, __fb_current_feature_timestamp)
    )
    AND (
      "event_timestamp" >= CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
      ) / 1800) * 1800 + 300 - 600 - 7776000 AS TIMESTAMP)
      AND "event_timestamp" < CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
      ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
    )
)
WHERE
  "cust_id" IS NOT NULL

SELECT
  ts,
  cust_id,
  value
FROM my_table
WHERE
  "ts" >= CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 1800
  ) / 3600) * 3600 + 1800 - 900 - 10800 - 86400 AS TIMESTAMP)
  AND "ts" < CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 1800
  ) / 3600) * 3600 + 1800 - 900 - 10800 AS TIMESTAMP)

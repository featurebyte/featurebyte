CONVERT_TIMEZONE(
  "tz_col",
  'UTC',
  CAST(TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
)

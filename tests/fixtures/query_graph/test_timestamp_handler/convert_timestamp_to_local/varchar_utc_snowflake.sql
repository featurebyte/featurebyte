CONVERT_TIMEZONE(
  'UTC',
  'Asia/Singapore',
  CAST(TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
)

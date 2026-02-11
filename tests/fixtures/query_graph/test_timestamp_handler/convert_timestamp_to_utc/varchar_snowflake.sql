CONVERT_TIMEZONE(
  'Asia/Singapore',
  'UTC',
  CAST(TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)
)

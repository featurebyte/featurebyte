DATETIME(
  CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `original_timestamp`) AS DATETIME) AS TIMESTAMP),
  'Asia/Singapore'
)
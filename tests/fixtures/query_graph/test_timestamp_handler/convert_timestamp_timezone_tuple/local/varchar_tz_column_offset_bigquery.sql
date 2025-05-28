DATETIME(
  CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_VALUE(`zipped_timestamp_tuple`, '$.timestamp')) AS DATETIME) AS TIMESTAMP),
  JSON_VALUE(`zipped_timestamp_tuple`, '$.timezone')
)

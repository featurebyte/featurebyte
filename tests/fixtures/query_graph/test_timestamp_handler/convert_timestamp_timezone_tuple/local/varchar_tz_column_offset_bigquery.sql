DATETIME(
  CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', `zipped_timestamp_tuple`.`timestamp`) AS DATETIME) AS TIMESTAMP),
  `zipped_timestamp_tuple`.`timezone`
)

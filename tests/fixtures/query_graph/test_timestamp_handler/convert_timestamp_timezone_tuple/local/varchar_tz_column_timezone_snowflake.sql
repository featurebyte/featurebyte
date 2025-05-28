CONVERT_TIMEZONE(
  'UTC',
  CAST(GET(PARSE_JSON("zipped_timestamp_tuple"), 'timezone') AS VARCHAR),
  TO_TIMESTAMP(
    CAST(GET(PARSE_JSON("zipped_timestamp_tuple"), 'timestamp') AS VARCHAR),
    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
  )
)

DATEADD(
  SECOND,
  F_TIMEZONE_OFFSET_TO_SECOND(CAST(GET(PARSE_JSON("zipped_timestamp_tuple"), 'timezone') AS VARCHAR)),
  CAST(TO_TIMESTAMP(
    CAST(GET(PARSE_JSON("zipped_timestamp_tuple"), 'timestamp') AS VARCHAR),
    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
  ) AS TIMESTAMP)
)

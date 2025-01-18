OBJECT_CONSTRUCT(
  'timestamp',
  TO_CHAR(CONVERT_TIMEZONE("tz_col", 'UTC', "timestamp_col"), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
  'timezone',
  "tz_col"
)

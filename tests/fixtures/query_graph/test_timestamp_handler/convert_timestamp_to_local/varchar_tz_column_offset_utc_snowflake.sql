DATEADD(
  SECOND,
  F_TIMEZONE_OFFSET_TO_SECOND("tz_col"),
  TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S')
)

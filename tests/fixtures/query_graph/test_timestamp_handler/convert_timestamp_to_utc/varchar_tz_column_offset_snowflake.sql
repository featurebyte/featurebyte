CAST(CONVERT_TIMEZONE(
  'UTC',
  TO_TIMESTAMP_TZ(
    CONCAT(
      TO_CHAR(TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S'), 'YYYY-MM-DD HH24:MI:SS'),
      ' ',
      "tz_col"
    )
  )
) AS TIMESTAMP)

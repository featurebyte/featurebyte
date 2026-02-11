CAST(CONVERT_TIMEZONE(
  'UTC',
  TO_TIMESTAMP_TZ(
    CONCAT(
      TO_CHAR(
        CAST(TO_TIMESTAMP("original_timestamp", '%Y-%m-%d %H:%M:%S') AS TIMESTAMP),
        'YYYY-MM-DD HH24:MI:SS'
      ),
      ' ',
      "tz_col"
    )
  )
) AS TIMESTAMP)

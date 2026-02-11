TO_JSON(
  OBJECT_CONSTRUCT(
    'timestamp',
    TO_CHAR(
      CONVERT_TIMEZONE("tz_col", 'UTC', CAST("timestamp_col" AS TIMESTAMP)),
      'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    ),
    'timezone',
    "tz_col"
  )
)

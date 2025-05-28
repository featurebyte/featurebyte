TO_JSON(
  NAMED_STRUCT(
    'timestamp',
    DATE_FORMAT(TO_UTC_TIMESTAMP(`timestamp_col`, `tz_col`), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''),
    'timezone',
    `tz_col`
  )
)

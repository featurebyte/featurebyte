TO_JSON_STRING(
  STRUCT(
    FORMAT_DATETIME(
      '%Y-%m-%dT%H:%M:%SZ',
      CAST(TIMESTAMP(CAST(`timestamp_col` AS DATETIME), `tz_col`) AS DATETIME)
    ) AS `timestamp`,
    `tz_col` AS `timezone`
  )
)

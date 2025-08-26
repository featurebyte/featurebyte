SELECT
  TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), `tz_offset`) AS DATETIME) AS TIMESTAMP),
      'UTC'
    ),
    HOUR
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

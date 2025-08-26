SELECT
  TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), `tz_name`) AS DATETIME) AS TIMESTAMP),
      'America/Los_Angeles'
    ),
    DAY
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), `tz_name`) AS DATETIME) AS TIMESTAMP),
      'America/Los_Angeles'
    ),
    DAY
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

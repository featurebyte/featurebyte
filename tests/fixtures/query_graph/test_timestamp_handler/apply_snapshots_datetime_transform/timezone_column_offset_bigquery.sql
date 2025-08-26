SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), `tz_offset`) AS DATETIME) AS TIMESTAMP),
      'UTC'
    ),
    HOUR
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATETIME_SUB(CAST(TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), 'America/Los_Angeles') AS DATETIME) AS TIMESTAMP),
      'Asia/Singapore'
    ),
    DAY
  ) AS DATETIME), INTERVAL 86400 SECOND) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

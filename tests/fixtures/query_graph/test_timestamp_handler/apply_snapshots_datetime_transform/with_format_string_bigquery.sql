SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  FORMAT_DATETIME(
    '%Y-%m-%d %H:%M:%S',
    DATETIME_SUB(CAST(TIMESTAMP_TRUNC(
      DATETIME(
        CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `event_timestamp`) AS DATETIME) AS TIMESTAMP),
        'Europe/London'
      ),
      MINUTE
    ) AS DATETIME), INTERVAL 900 SECOND)
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

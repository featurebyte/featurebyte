SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  FORMAT_DATETIME(
    'YYYY-MM-DD',
    TIMESTAMP_TRUNC(CAST(PARSE_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', `event_timestamp`) AS DATETIME), DAY)
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

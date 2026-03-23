SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  FORMAT_DATETIME('YYYY-MM-DD', TIMESTAMP_TRUNC(`event_timestamp`, DAY)) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

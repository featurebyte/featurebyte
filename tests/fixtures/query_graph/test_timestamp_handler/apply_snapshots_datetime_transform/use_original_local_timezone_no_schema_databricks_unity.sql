SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATE_FORMAT(DATE_TRUNC('DAY', `event_timestamp`), 'YYYY-MM-DD') AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

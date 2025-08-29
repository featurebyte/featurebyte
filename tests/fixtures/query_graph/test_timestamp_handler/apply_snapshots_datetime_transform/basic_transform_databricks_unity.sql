SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATE_TRUNC('HOUR', FROM_UTC_TIMESTAMP(`event_timestamp`, 'America/New_York')) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

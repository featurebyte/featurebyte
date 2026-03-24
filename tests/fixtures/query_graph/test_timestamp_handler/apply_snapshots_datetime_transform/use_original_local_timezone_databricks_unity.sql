SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATE_FORMAT(
    DATE_TRUNC('DAY', TO_TIMESTAMP(`event_timestamp`, 'YYYY-MM-DD HH24:MI:SS')),
    'YYYY-MM-DD'
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

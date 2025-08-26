SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATE_TRUNC(
    'DAY',
    FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, `tz_name`), 'America/Los_Angeles')
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

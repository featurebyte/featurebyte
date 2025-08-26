SELECT
  DATE_TRUNC(
    'DAY',
    FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, `tz_name`), 'America/Los_Angeles')
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

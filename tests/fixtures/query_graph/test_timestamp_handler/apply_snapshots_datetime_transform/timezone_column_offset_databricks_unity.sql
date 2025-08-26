SELECT
  DATE_TRUNC(
    'HOUR',
    FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, `tz_offset`), 'UTC')
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

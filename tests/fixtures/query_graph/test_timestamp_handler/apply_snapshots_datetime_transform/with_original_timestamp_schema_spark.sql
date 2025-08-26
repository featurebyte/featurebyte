SELECT
  DATE_TRUNC(
    'DAY',
    FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, 'America/Los_Angeles'), 'Asia/Singapore')
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

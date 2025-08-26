SELECT
  DATE_FORMAT(
    DATE_TRUNC(
      'MINUTE',
      FROM_UTC_TIMESTAMP(TO_TIMESTAMP(`event_timestamp`, '%Y-%m-%d %H:%M:%S'), 'Europe/London')
    ),
    '%Y-%m-%d %H:%M:%S'
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

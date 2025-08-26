SELECT
  DATE_TRUNC('HOUR', FROM_UTC_TIMESTAMP(`event_timestamp`, 'America/New_York')) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

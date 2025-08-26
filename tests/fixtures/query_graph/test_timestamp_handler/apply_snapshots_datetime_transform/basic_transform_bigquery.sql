SELECT
  TIMESTAMP_TRUNC(DATETIME(CAST(`event_timestamp` AS TIMESTAMP), 'America/New_York'), HOUR) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

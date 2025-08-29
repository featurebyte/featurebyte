SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  TIMESTAMP_TRUNC(DATETIME(CAST(`event_timestamp` AS TIMESTAMP), 'America/New_York'), HOUR) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

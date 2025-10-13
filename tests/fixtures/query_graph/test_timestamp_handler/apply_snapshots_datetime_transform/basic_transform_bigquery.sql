SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATETIME_SUB(CAST(TIMESTAMP_TRUNC(DATETIME(CAST(`event_timestamp` AS TIMESTAMP), 'America/New_York'), HOUR) AS DATETIME), INTERVAL 3600 SECOND) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

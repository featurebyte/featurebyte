SELECT
  TIMESTAMP_TRUNC(
    DATETIME(
      CAST(CAST(TIMESTAMP(CAST(`event_timestamp` AS DATETIME), 'America/Los_Angeles') AS DATETIME) AS TIMESTAMP),
      'Asia/Singapore'
    ),
    DAY
  ) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

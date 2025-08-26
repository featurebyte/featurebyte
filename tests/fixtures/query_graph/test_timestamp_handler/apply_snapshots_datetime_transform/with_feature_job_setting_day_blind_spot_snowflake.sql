SELECT
  DATEADD(
    SECOND,
    -172800,
    DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'UTC', "event_timestamp"))
  ) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

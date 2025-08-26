SELECT
  DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'America/New_York', "event_timestamp")) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

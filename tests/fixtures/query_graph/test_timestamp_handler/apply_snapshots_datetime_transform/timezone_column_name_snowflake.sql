SELECT
  DATE_TRUNC(
    'day',
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CONVERT_TIMEZONE("tz_name", 'UTC', "event_timestamp"))
  ) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

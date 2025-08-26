SELECT
  DATE_TRUNC(
    'day',
    CONVERT_TIMEZONE(
      'UTC',
      'Asia/Singapore',
      CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', "event_timestamp")
    )
  ) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

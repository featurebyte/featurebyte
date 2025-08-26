SELECT
  TO_CHAR(
    DATE_TRUNC(
      'minute',
      CONVERT_TIMEZONE('UTC', 'Europe/London', TO_TIMESTAMP("event_timestamp", '%Y-%m-%d %H:%M:%S'))
    ),
    '%Y-%m-%d %H:%M:%S'
  ) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

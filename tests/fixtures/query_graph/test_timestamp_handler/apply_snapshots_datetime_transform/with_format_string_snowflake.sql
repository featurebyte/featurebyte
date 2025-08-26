SELECT
  "event_timestamp",
  "user_id",
  "amount",
  TO_CHAR(
    DATE_TRUNC(
      'minute',
      CONVERT_TIMEZONE('UTC', 'Europe/London', TO_TIMESTAMP("event_timestamp", '%Y-%m-%d %H:%M:%S'))
    ),
    '%Y-%m-%d %H:%M:%S'
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

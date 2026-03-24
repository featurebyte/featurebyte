SELECT
  "event_timestamp",
  "user_id",
  "amount",
  TO_CHAR(
    DATE_TRUNC('day', TO_TIMESTAMP("event_timestamp", 'YYYY-MM-DD HH24:MI:SS')),
    'YYYY-MM-DD'
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

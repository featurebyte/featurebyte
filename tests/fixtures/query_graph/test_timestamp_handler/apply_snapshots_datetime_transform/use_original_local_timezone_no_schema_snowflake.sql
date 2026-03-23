SELECT
  "event_timestamp",
  "user_id",
  "amount",
  TO_CHAR(DATE_TRUNC('day', "event_timestamp"), 'YYYY-MM-DD') AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

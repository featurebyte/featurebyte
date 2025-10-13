SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATEADD(
    SECOND,
    -172800,
    DATEADD(SECOND, -86400, DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'UTC', "event_timestamp")))
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

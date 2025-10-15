SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATEADD(
    SECOND,
    -3600,
    DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'America/New_York', "event_timestamp"))
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

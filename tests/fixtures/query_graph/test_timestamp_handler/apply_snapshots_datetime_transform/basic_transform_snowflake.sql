SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATEADD(
    SECOND,
    -3600,
    DATE_TRUNC(
      'hour',
      CONVERT_TIMEZONE('UTC', 'America/New_York', CAST("event_timestamp" AS TIMESTAMP))
    )
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

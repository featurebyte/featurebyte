SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATEADD(
    SECOND,
    -86400,
    DATE_TRUNC(
      'day',
      CONVERT_TIMEZONE(
        'UTC',
        'America/Los_Angeles',
        CAST(CONVERT_TIMEZONE("tz_name", 'UTC', CAST("event_timestamp" AS TIMESTAMP)) AS TIMESTAMP)
      )
    )
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

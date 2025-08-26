SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATE_TRUNC(
    'hour',
    CONVERT_TIMEZONE(
      'UTC',
      'UTC',
      CAST(CONVERT_TIMEZONE(
        'UTC',
        TO_TIMESTAMP_TZ(CONCAT(TO_CHAR("event_timestamp", 'YYYY-MM-DD HH24:MI:SS'), ' ', "tz_offset"))
      ) AS TIMESTAMP)
    )
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

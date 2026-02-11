SELECT
  "event_timestamp",
  "user_id",
  "amount",
  DATEADD(
    SECOND,
    -3600,
    DATE_TRUNC(
      'hour',
      CONVERT_TIMEZONE(
        'UTC',
        'UTC',
        CAST(CAST(CONVERT_TIMEZONE(
          'UTC',
          TO_TIMESTAMP_TZ(
            CONCAT(
              TO_CHAR(CAST("event_timestamp" AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS'),
              ' ',
              "tz_offset"
            )
          )
        ) AS TIMESTAMP) AS TIMESTAMP)
      )
    )
  ) AS "__FB_SNAPSHOTS_ADJUSTED_event_timestamp"
FROM events

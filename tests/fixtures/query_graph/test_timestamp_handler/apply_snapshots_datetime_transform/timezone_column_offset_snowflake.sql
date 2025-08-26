SELECT
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
  ) AS "event_timestamp",
  "user_id",
  "amount"
FROM events

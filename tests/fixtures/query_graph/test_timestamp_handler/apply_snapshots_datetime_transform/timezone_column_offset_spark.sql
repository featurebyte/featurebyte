SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  TIMESTAMPADD(
    second,
    -3600,
    DATE_TRUNC(
      'HOUR',
      FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, `tz_offset`), 'UTC')
    )
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

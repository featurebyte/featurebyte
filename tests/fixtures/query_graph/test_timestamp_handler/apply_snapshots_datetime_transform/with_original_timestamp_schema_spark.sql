SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  TIMESTAMPADD(
    second,
    -86400,
    DATE_TRUNC(
      'DAY',
      FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(`event_timestamp`, 'America/Los_Angeles'), 'Asia/Singapore')
    )
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

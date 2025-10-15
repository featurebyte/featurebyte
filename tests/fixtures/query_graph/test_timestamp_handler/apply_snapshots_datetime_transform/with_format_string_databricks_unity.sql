SELECT
  `event_timestamp`,
  `user_id`,
  `amount`,
  DATE_FORMAT(
    TIMESTAMPADD(
      second,
      -900,
      DATE_TRUNC(
        'MINUTE',
        FROM_UTC_TIMESTAMP(TO_TIMESTAMP(`event_timestamp`, '%Y-%m-%d %H:%M:%S'), 'Europe/London')
      )
    ),
    '%Y-%m-%d %H:%M:%S'
  ) AS `__FB_SNAPSHOTS_ADJUSTED_event_timestamp`
FROM events

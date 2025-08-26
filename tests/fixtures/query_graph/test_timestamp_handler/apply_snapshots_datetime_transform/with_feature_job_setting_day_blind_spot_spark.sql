SELECT
  TIMESTAMPADD(second, -172800, DATE_TRUNC('DAY', FROM_UTC_TIMESTAMP(`event_timestamp`, 'UTC'))) AS `event_timestamp`,
  `user_id`,
  `amount`
FROM events

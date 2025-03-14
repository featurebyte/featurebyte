SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM FROM_UTC_TIMESTAMP(TO_TIMESTAMP(`ts`, '%Y-%m-%d %H:%M:%S'), `tz_offset`)) AS `hour`
FROM `db`.`public`.`event_table`

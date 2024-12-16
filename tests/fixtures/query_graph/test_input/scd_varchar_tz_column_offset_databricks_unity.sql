SELECT
  TO_UTC_TIMESTAMP(TO_TIMESTAMP(`ts`, '%Y-%m-%d %H:%M:%S'), `tz_col`) AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

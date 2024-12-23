SELECT
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `ts`) AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

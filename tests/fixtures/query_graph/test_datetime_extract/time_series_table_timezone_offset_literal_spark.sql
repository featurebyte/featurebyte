SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM TO_TIMESTAMP(`ts`, '%Y-%m-%d %H:%M:%S')) AS `hour`
FROM `db`.`public`.`event_table`

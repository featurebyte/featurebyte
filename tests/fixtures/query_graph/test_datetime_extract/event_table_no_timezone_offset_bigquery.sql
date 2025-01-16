SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM `ts`) AS `hour`
FROM `db`.`public`.`event_table`

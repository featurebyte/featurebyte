SELECT
  CAST(CAST(CAST(`ts` AS TIMESTAMP) AS DOUBLE) + 86400 AS TIMESTAMP) AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

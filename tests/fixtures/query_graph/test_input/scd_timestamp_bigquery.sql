SELECT
  CAST(TIMESTAMP(CAST(`ts` AS DATETIME), 'Asia/Singapore') AS DATETIME) AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

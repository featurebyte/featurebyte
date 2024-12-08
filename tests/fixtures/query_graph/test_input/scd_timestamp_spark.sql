SELECT
  TO_UTC_TIMESTAMP(`ts`, 'Asia/Singapore') AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

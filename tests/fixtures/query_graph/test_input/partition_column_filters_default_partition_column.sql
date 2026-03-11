SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`
WHERE
  `ts` >= DATE_FORMAT(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
  AND `ts` <= DATE_FORMAT(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')

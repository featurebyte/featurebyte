SELECT
  `partition_col` AS `partition_col`,
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`
WHERE
  `partition_col` >= DATE_FORMAT(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
  AND `partition_col` <= DATE_FORMAT(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')

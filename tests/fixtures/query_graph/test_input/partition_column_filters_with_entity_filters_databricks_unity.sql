SELECT
  `partition_col` AS `partition_col`,
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      `partition_col`,
      `ts`,
      `cust_id`,
      `a`
    FROM `db`.`public`.`event_table`
    WHERE
      `partition_col` >= DATE_FORMAT(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
      AND `partition_col` <= DATE_FORMAT(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
  ) AS R
    ON R.`cust_id` = __FB_ENTITY_TABLE_NAME.`serving_cust_id`
)

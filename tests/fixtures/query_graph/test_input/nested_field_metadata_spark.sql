SELECT
  `partition_col`,
  `ts`,
  `cust_id`,
  `a`,
  `user_city`,
  `user_country_code`
FROM (
  SELECT
    `partition_col` AS `partition_col`,
    `ts` AS `ts`,
    `cust_id` AS `cust_id`,
    `a` AS `a`,
    `user_city` AS `user_city`,
    `user_country_code` AS `user_country_code`,
    (
      LAG(`a`, 1) OVER (PARTITION BY `cust_id` ORDER BY `ts` NULLS LAST) = 1
    ) AS `__fb_qualify_condition_column`
  FROM (
    SELECT
      `partition_col` AS `partition_col`,
      `ts` AS `ts`,
      `cust_id` AS `cust_id`,
      `a` AS `a`,
      `user_info`.`address`.`city` AS `user_city`,
      `user_info`.`address`.`billing_address`.`country_code` AS `user_country_code`
    FROM `db`.`public`.`event_table`
  )
  WHERE
    `partition_col` >= DATE_FORMAT(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
    AND `partition_col` <= DATE_FORMAT(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
)
WHERE
  `__fb_qualify_condition_column`

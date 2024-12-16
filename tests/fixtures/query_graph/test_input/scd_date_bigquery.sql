SELECT
  CAST(TIMESTAMP(
    CAST(DATETIME_ADD(CAST(`ts` AS DATETIME), INTERVAL (CAST(86400 AS INT64)) SECOND) AS DATETIME),
    'Asia/Singapore'
  ) AS DATETIME) AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`
FROM `db`.`public`.`event_table`

SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM DATETIME_ADD(CAST(`ts` AS DATETIME), INTERVAL (CAST(`my_db`.`my_schema`.F_TIMEZONE_OFFSET_TO_SECOND('+08:00') AS INT64)) SECOND)) AS `hour`
FROM `db`.`public`.`event_table`

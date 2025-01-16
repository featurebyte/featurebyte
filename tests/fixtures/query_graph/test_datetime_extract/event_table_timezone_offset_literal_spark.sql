SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM CAST(CAST(CAST(`ts` AS TIMESTAMP) AS DOUBLE) + F_TIMEZONE_OFFSET_TO_SECOND('+08:00') AS TIMESTAMP)) AS `hour`
FROM `db`.`public`.`event_table`

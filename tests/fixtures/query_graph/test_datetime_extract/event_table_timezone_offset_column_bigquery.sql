SELECT
  `ts` AS `ts`,
  `tz_offset` AS `tz_offset`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM DATETIME_ADD(CAST(`ts` AS DATETIME), INTERVAL (CAST(`my_db`.`my_schema`.F_TIMEZONE_OFFSET_TO_SECOND(`tz_offset`) AS INT64)) SECOND)) AS `hour`
FROM `db`.`public`.`event_table`

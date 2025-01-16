SELECT
  `ts` AS `ts`,
  `tz_offset` AS `tz_offset`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM CAST(CAST(CAST(`ts` AS TIMESTAMP) AS DOUBLE) + F_TIMEZONE_OFFSET_TO_SECOND(`tz_offset`) AS TIMESTAMP)) AS `hour`
FROM `db`.`public`.`event_table`

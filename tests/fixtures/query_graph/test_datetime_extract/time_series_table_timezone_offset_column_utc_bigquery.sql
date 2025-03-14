SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM DATETIME(
    CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `ts`) AS DATETIME) AS TIMESTAMP),
    `tz_offset`
  )) AS `hour`
FROM `db`.`public`.`event_table`

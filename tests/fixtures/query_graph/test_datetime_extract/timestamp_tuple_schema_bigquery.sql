SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM DATETIME(
    CAST(CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', `ts`.`timestamp`) AS DATETIME) AS TIMESTAMP),
    `ts`.`timezone`
  )) AS `hour`
FROM `db`.`public`.`event_table`

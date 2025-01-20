SELECT
  `ts` AS `ts`,
  `cust_id` AS `cust_id`,
  `a` AS `a`,
  EXTRACT(hour FROM FROM_UTC_TIMESTAMP(TO_TIMESTAMP(`ts`.timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''), `ts`.timezone)) AS `hour`
FROM `db`.`public`.`event_table`

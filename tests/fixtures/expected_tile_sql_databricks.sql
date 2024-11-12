WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      `ts` AS `ts`,
      `cust_id` AS `cust_id`,
      `a` AS `a`,
      `b` AS `b`,
      (
        `a` + `b`
      ) AS `c`
    FROM `db`.`public`.`event_table`
  )
  WHERE
    `ts` >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND `ts` < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  `cust_id`,
  SUM(`a`) AS sum_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35,
  COUNT(`a`) AS count_value_avg_f37862722c21105449ad882409cf62a1ff7f5b35
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(`ts` AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  `cust_id`

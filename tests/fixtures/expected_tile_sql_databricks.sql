WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      `ts` AS `ts`,
      `cust_id` AS `cust_id`,
      `a` AS `input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f`
    FROM `db`.`public`.`event_table`
  )
  WHERE
    `ts` >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND `ts` < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  `cust_id`,
  SUM(`input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f`) AS sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
  COUNT(`input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f`) AS count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(`ts` AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  `cust_id`

SELECT DISTINCT
  CAST(`store_id` AS INT64) AS `cust_id`
FROM (
  SELECT
    `col_int` AS `col_int`,
    `col_float` AS `col_float`,
    `col_char` AS `col_char`,
    `col_text` AS `col_text`,
    `col_binary` AS `col_binary`,
    `col_boolean` AS `col_boolean`,
    `date` AS `date`,
    `store_id` AS `store_id`,
    `another_timestamp_col` AS `another_timestamp_col`
  FROM `sf_database`.`sf_schema`.`time_series_table`
  WHERE
    CAST(CAST(PARSE_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', `date`) AS DATETIME) AS DATETIME) >= DATETIME_SUB(CAST(DATETIME_SUB(CAST(TIMESTAMP_TRUNC(`__fb_current_feature_timestamp`, MONTH) AS DATETIME), INTERVAL 3 MONTH) AS DATETIME), INTERVAL 3 MONTH)
    AND CAST(CAST(PARSE_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', `date`) AS DATETIME) AS DATETIME) < DATETIME_SUB(CAST(TIMESTAMP_TRUNC(`__fb_current_feature_timestamp`, MONTH) AS DATETIME), INTERVAL 3 MONTH)
)
WHERE
  NOT `store_id` IS NULL

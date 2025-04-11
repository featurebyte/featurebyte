SELECT
  COUNT(*) AS `row_count`
FROM (
  SELECT
    CAST(CAST(`event_timestamp` AS TIMESTAMP) AS DATETIME) AS `POINT_IN_TIME`,
    `col_int` AS `col_int`
  FROM (
    SELECT
      `col_int` AS `col_int`,
      `col_float` AS `col_float`,
      `col_char` AS `col_char`,
      `col_text` AS `col_text`,
      `col_binary` AS `col_binary`,
      `col_boolean` AS `col_boolean`,
      `event_timestamp` AS `event_timestamp`,
      `cust_id` AS `cust_id`
    FROM `sf_database`.`sf_schema`.`sf_table`
  )
);

CREATE TABLE `sf_database`.`sf_schema`.`my_materialized_table` AS
SELECT
  `POINT_IN_TIME`,
  `col_int`
FROM (
  SELECT
    RAND() AS `prob`,
    `POINT_IN_TIME`,
    `col_int`
  FROM (
    SELECT
      CAST(CAST(`event_timestamp` AS TIMESTAMP) AS DATETIME) AS `POINT_IN_TIME`,
      `col_int` AS `col_int`
    FROM (
      SELECT
        `col_int` AS `col_int`,
        `col_float` AS `col_float`,
        `col_char` AS `col_char`,
        `col_text` AS `col_text`,
        `col_binary` AS `col_binary`,
        `col_boolean` AS `col_boolean`,
        `event_timestamp` AS `event_timestamp`,
        `cust_id` AS `cust_id`
      FROM `sf_database`.`sf_schema`.`sf_table`
    )
  )
)
WHERE
  `prob` <= 0.15000000000000002
ORDER BY
  `prob`
LIMIT 100;

WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_int" AS "input_col_sum_fd6e13fefe228f132931e13dbdc31b2b037c2b74"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      "col_text" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
      AND "col_text" <= TO_CHAR(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_fd6e13fefe228f132931e13dbdc31b2b037c2b74") AS value_sum_fd6e13fefe228f132931e13dbdc31b2b037c2b74
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

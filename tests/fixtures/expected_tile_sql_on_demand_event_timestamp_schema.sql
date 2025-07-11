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
      "col_int" AS "input_col_sum_fefdc3e84902d2f76f04cc746b8bba9256212347"
    FROM "sf_database"."sf_schema"."sf_table_no_tz"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND (
      R."event_timestamp" >= DATEADD(DAY, -7, __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE)
      AND R."event_timestamp" <= DATEADD(DAY, 7, __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE)
    )
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_fefdc3e84902d2f76f04cc746b8bba9256212347") AS value_sum_fefdc3e84902d2f76f04cc746b8bba9256212347
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(
      CAST(CONVERT_TIMEZONE(
        'UTC',
        TO_TIMESTAMP_TZ(CONCAT(TO_CHAR("event_timestamp", 'YYYY-MM-DD HH24:MI:SS'), ' ', "tz_offset"))
      ) AS TIMESTAMP),
      300,
      600,
      30
    ) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

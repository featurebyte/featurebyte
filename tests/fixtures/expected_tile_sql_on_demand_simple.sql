WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id"
    FROM "db"."public"."event_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_56cb3bb7a2c08d13a73d1b5fafdb31bad53fff3a
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

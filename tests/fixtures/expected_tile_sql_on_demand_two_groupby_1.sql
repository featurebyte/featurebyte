WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "a" AS "input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM "db"."public"."event_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f") AS sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
  COUNT("input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f") AS count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "a" AS "a",
      "b" AS "b"
    FROM "db"."public"."event_table"
  )
  WHERE
    "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  value_latest_414e1c5ab2e329a43aabe6dc95bd30d1d9c311b0
FROM (
  SELECT
    index,
    "cust_id",
    ROW_NUMBER() OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
    FIRST_VALUE("a") OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_414e1c5ab2e329a43aabe6dc95bd30d1d9c311b0
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
    FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
  )
)
WHERE
  "__FB_ROW_NUMBER" = 1

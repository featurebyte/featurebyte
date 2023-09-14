WITH data AS (
  SELECT
    "a" AS "a"
  FROM "db"."public"."event_table"
  ORDER BY
    RANDOM(1234)
  LIMIT 50000
), casted_data AS (
  SELECT
    CAST("a" AS STRING) AS "a"
  FROM data
)
SELECT
  "a" AS "a",
  COUNTS AS "count"
FROM (
  SELECT
    "a",
    COUNT('*') AS "COUNTS"
  FROM casted_data
  GROUP BY
    "a"
  ORDER BY
    COUNTS DESC NULLS LAST
  LIMIT 1000
)

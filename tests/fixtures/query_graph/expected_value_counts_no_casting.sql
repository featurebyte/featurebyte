WITH "data" AS (
  SELECT
    "a" AS "a"
  FROM "db"."public"."event_table"
  ORDER BY
    RANDOM(1234)
  LIMIT 50000
)
SELECT
  "a" AS "key",
  "__FB_COUNTS" AS "count"
FROM (
  SELECT
    "a",
    COUNT(*) AS "__FB_COUNTS"
  FROM "data"
  GROUP BY
    "a"
  ORDER BY
    "__FB_COUNTS" DESC NULLS LAST
  LIMIT 1000
)

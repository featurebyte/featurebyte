SELECT
  "Unnamed0"
FROM (
  SELECT
    (
      "a" + 123
    ) AS "Unnamed0",
    (
      LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
    ) AS "__fb_qualify_condition_column"
  FROM "db"."public"."event_table"
  WHERE
    (
      "a" = 123
    )
)
WHERE
  "__fb_qualify_condition_column"

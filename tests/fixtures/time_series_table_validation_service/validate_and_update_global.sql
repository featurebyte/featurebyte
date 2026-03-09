SELECT
  MAX("snapshot_date") AS "RESULT"
FROM (
  SELECT
    TO_TIMESTAMP("snapshot_date", '%Y-%m-%d') AS "snapshot_date"
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "snapshot_date" IS NOT NULL
  LIMIT 50000
);

SELECT
  "ALIGNED_DT",
  COUNT(*) AS "DUPLICATE_COUNT"
FROM (
  SELECT
    DATE_TRUNC('day', TO_TIMESTAMP("snapshot_date", '%Y-%m-%d')) AS "ALIGNED_DT"
  FROM (
    SELECT
      "snapshot_date" AS "snapshot_date"
    FROM "my_db"."my_schema"."my_table"
  )
  WHERE
    "snapshot_date" IS NOT NULL
)
GROUP BY
  "ALIGNED_DT"
HAVING
  "DUPLICATE_COUNT" > 1
LIMIT 1;

SELECT
  COUNT(DISTINCT "snapshot_date") AS "snapshot_date"
FROM "my_db"."my_schema"."my_table";

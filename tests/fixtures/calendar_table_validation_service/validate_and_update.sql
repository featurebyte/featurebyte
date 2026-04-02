SELECT
  MAX("snapshot_date") AS "RESULT"
FROM (
  SELECT
    TO_TIMESTAMP("snapshot_date", 'YYYY-MM-DD') AS "snapshot_date"
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "snapshot_date" IS NOT NULL
  LIMIT 50000
);

SELECT
  "snapshot_date"
FROM "my_db"."my_schema"."my_table"
WHERE
  "snapshot_date" IS NOT NULL
  AND "snapshot_date" <> TO_CHAR(DATE_TRUNC('day', TO_TIMESTAMP("snapshot_date", 'YYYY-MM-DD')), 'YYYY-MM-DD')
LIMIT 10;

SELECT
  "snapshot_date",
  "series_id",
  COUNT(*) AS "DUPLICATE_COUNT"
FROM "my_db"."my_schema"."my_table"
WHERE
  "snapshot_date" IS NOT NULL
GROUP BY
  "snapshot_date",
  "series_id"
HAVING
  "DUPLICATE_COUNT" > 1
LIMIT 1;

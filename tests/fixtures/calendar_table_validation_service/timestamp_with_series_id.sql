SELECT
  "snapshot_date"
FROM "my_db"."my_schema"."my_table"
WHERE
  "snapshot_date" IS NOT NULL
  AND "snapshot_date" <> DATE_TRUNC('day', "snapshot_date")
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

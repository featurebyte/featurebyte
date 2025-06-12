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
  COUNT(DISTINCT "snapshot_date") AS "snapshot_date"
FROM "my_db"."my_schema"."my_table";

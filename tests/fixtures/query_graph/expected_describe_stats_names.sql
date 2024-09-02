WITH stats AS (
  SELECT
    MIN(
      IFF(
        "ts" < CAST('1900-01-01' AS TIMESTAMP) OR "ts" > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "ts"
      )
    ) AS "min__0",
    MAX(
      IFF(
        "ts" < CAST('1900-01-01' AS TIMESTAMP) OR "ts" > CAST('2200-01-01' AS TIMESTAMP),
        NULL,
        "ts"
      )
    ) AS "max__0",
    NULL AS "min__1",
    NULL AS "max__1",
    MIN("a") AS "min__2",
    MAX("a") AS "max__2",
    MIN("b") AS "min__3",
    MAX("b") AS "max__3",
    MIN("a_copy") AS "min__4",
    MAX("a_copy") AS "max__4"
  FROM "__FB_INPUT_TABLE_SQL_PLACEHOLDER"
), joined_tables_0 AS (
  SELECT
    *
  FROM stats
)
SELECT
  'TIMESTAMP' AS "dtype__0",
  "min__0",
  "max__0",
  'VARCHAR' AS "dtype__1",
  "min__1",
  "max__1",
  'FLOAT' AS "dtype__2",
  "min__2",
  "max__2",
  'INT' AS "dtype__3",
  "min__3",
  "max__3",
  'FLOAT' AS "dtype__4",
  "min__4",
  "max__4"
FROM joined_tables_0

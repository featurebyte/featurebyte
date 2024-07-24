WITH stats AS (
  SELECT
    MIN("b") AS "min__3",
    MAX("b") AS "max__3",
    MIN("a_copy") AS "min__4",
    MAX("a_copy") AS "max__4"
  FROM "__TEMP_SAMPLED_DATA_000000000000000000000000"
), joined_tables_0 AS (
  SELECT
    *
  FROM stats
)
SELECT
  'INT' AS "dtype__3",
  "min__3",
  "max__3",
  'FLOAT' AS "dtype__4",
  "min__4",
  "max__4"
FROM joined_tables_0

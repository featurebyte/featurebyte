CREATE TABLE my_table AS SELECT * FROM another_table;

SELECT
  MAX("row_index_count") AS "max_row_index_count"
FROM (
  SELECT
    COUNT(*) AS "row_index_count"
  FROM "my_table"
  GROUP BY
    __FB_TABLE_ROW_INDEX
);

CREATE TABLE output_table AS SELECT * FROM my_table;

SELECT
  MAX("row_index_count") AS "max_row_index_count"
FROM (
  SELECT
    COUNT(*) AS "row_index_count"
  FROM "output_table"
  GROUP BY
    __FB_TABLE_ROW_INDEX
);

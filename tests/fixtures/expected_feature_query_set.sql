CREATE TABLE my_table AS SELECT * FROM another_table;

SELECT
  COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
FROM "my_table";

CREATE TABLE output_table AS SELECT * FROM my_table;

SELECT
  COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
FROM "output_table";

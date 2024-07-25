WITH "casted_data" AS (
  SELECT
    CAST("ts" AS STRING) AS "ts",
    CAST("cust_id" AS STRING) AS "cust_id",
    CAST("a" AS STRING) AS "a",
    CAST("b" AS STRING) AS "b",
    CAST("a_copy" AS STRING) AS "a_copy"
  FROM "__TEMP_SAMPLED_DATA_000000000000000000000000"
), counts__1 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__1",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__1",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__1"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "cust_id" IS NULL THEN '__MISSING__' ELSE "cust_id" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "cust_id",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "cust_id"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), joined_tables_0 AS (
  SELECT
    *
  FROM counts__1
)
SELECT
  'TIMESTAMP' AS "dtype__0",
  NULL AS "entropy__0",
  'VARCHAR' AS "dtype__1",
  "entropy__1",
  'FLOAT' AS "dtype__2",
  NULL AS "entropy__2",
  'INT' AS "dtype__3",
  NULL AS "entropy__3",
  'FLOAT' AS "dtype__4",
  NULL AS "entropy__4"
FROM joined_tables_0

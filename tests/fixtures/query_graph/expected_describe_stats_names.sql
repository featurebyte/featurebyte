WITH data AS (
  SELECT
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "a" AS "a",
    "b" AS "b",
    "a" AS "a_copy"
  FROM "db"."public"."event_table"
  ORDER BY
    RANDOM(1234)
  LIMIT 10
), casted_data AS (
  SELECT
    CAST("ts" AS STRING) AS "ts",
    CAST("cust_id" AS STRING) AS "cust_id",
    CAST("a" AS STRING) AS "a",
    CAST("b" AS STRING) AS "b",
    CAST("a_copy" AS STRING) AS "a_copy"
  FROM data
), counts__0 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__0",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__0"
  FROM (
    SELECT
      OBJECT_AGG("ts", "__FB_COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "ts",
        COUNT(*) AS "__FB_COUNTS"
      FROM casted_data
      GROUP BY
        "ts"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__1 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__1",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__1",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__1"
  FROM (
    SELECT
      OBJECT_AGG("cust_id", "__FB_COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "cust_id",
        COUNT(*) AS "__FB_COUNTS"
      FROM casted_data
      GROUP BY
        "cust_id"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__2 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__2",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__2"
  FROM (
    SELECT
      OBJECT_AGG("a", "__FB_COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "a",
        COUNT(*) AS "__FB_COUNTS"
      FROM casted_data
      GROUP BY
        "a"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__3 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__3",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__3"
  FROM (
    SELECT
      OBJECT_AGG("b", "__FB_COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "b",
        COUNT(*) AS "__FB_COUNTS"
      FROM casted_data
      GROUP BY
        "b"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__4 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__4",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__4"
  FROM (
    SELECT
      OBJECT_AGG("a_copy", "__FB_COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "a_copy",
        COUNT(*) AS "__FB_COUNTS"
      FROM casted_data
      GROUP BY
        "a_copy"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    MIN("ts") AS "min__0",
    MAX("ts") AS "max__0",
    NULL AS "min__1",
    NULL AS "max__1",
    MIN("a") AS "min__2",
    MAX("a") AS "max__2",
    MIN("b") AS "min__3",
    MAX("b") AS "max__3",
    MIN("a_copy") AS "min__4",
    MAX("a_copy") AS "max__4"
  FROM data
)
SELECT
  'TIMESTAMP' AS "dtype__0",
  stats."min__0",
  stats."max__0",
  'VARCHAR' AS "dtype__1",
  stats."min__1",
  stats."max__1",
  'FLOAT' AS "dtype__2",
  stats."min__2",
  stats."max__2",
  'INT' AS "dtype__3",
  stats."min__3",
  stats."max__3",
  'FLOAT' AS "dtype__4",
  stats."min__4",
  stats."max__4"
FROM stats
LEFT JOIN counts__0
LEFT JOIN counts__1
LEFT JOIN counts__2
LEFT JOIN counts__3
LEFT JOIN counts__4

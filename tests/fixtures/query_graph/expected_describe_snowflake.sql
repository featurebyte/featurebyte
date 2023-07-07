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
      OBJECT_AGG("ts", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "ts",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "ts"
      ORDER BY
        COUNTS DESC NULLS LAST
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
      OBJECT_AGG("cust_id", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "cust_id",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "cust_id"
      ORDER BY
        COUNTS DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__2 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__2",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__2"
  FROM (
    SELECT
      OBJECT_AGG("a", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "a",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "a"
      ORDER BY
        COUNTS DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__3 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__3",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__3"
  FROM (
    SELECT
      OBJECT_AGG("b", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "b",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "b"
      ORDER BY
        COUNTS DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__4 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__4",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__4"
  FROM (
    SELECT
      OBJECT_AGG("a_copy", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "a_copy",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "a_copy"
      ORDER BY
        COUNTS DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    COUNT(DISTINCT "ts") AS "unique__0",
    (
      1.0 - COUNT("ts") / COUNT('*')
    ) * 100 AS "%missing__0",
    NULL AS "%empty__0",
    NULL AS "mean__0",
    NULL AS "std__0",
    MIN("ts") AS "min__0",
    NULL AS "25%__0",
    NULL AS "50%__0",
    NULL AS "75%__0",
    MAX("ts") AS "max__0",
    NULL AS "min TZ offset__0",
    NULL AS "max TZ offset__0",
    COUNT(DISTINCT "cust_id") AS "unique__1",
    (
      1.0 - COUNT("cust_id") / COUNT('*')
    ) * 100 AS "%missing__1",
    COUNT_IF("cust_id" = '') AS "%empty__1",
    NULL AS "mean__1",
    NULL AS "std__1",
    NULL AS "min__1",
    NULL AS "25%__1",
    NULL AS "50%__1",
    NULL AS "75%__1",
    NULL AS "max__1",
    NULL AS "min TZ offset__1",
    NULL AS "max TZ offset__1",
    COUNT(DISTINCT "a") AS "unique__2",
    (
      1.0 - COUNT("a") / COUNT('*')
    ) * 100 AS "%missing__2",
    NULL AS "%empty__2",
    AVG(CAST("a" AS DOUBLE)) AS "mean__2",
    STDDEV(CAST("a" AS DOUBLE)) AS "std__2",
    MIN("a") AS "min__2",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "a") AS "25%__2",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "a") AS "50%__2",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "a") AS "75%__2",
    MAX("a") AS "max__2",
    NULL AS "min TZ offset__2",
    NULL AS "max TZ offset__2",
    COUNT(DISTINCT "b") AS "unique__3",
    (
      1.0 - COUNT("b") / COUNT('*')
    ) * 100 AS "%missing__3",
    NULL AS "%empty__3",
    AVG(CAST("b" AS DOUBLE)) AS "mean__3",
    STDDEV(CAST("b" AS DOUBLE)) AS "std__3",
    MIN("b") AS "min__3",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "b") AS "25%__3",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "b") AS "50%__3",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "b") AS "75%__3",
    MAX("b") AS "max__3",
    NULL AS "min TZ offset__3",
    NULL AS "max TZ offset__3",
    COUNT(DISTINCT "a_copy") AS "unique__4",
    (
      1.0 - COUNT("a_copy") / COUNT('*')
    ) * 100 AS "%missing__4",
    NULL AS "%empty__4",
    AVG(CAST("a_copy" AS DOUBLE)) AS "mean__4",
    STDDEV(CAST("a_copy" AS DOUBLE)) AS "std__4",
    MIN("a_copy") AS "min__4",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "a_copy") AS "25%__4",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "a_copy") AS "50%__4",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "a_copy") AS "75%__4",
    MAX("a_copy") AS "max__4",
    NULL AS "min TZ offset__4",
    NULL AS "max TZ offset__4"
  FROM data
)
SELECT
  'TIMESTAMP' AS "dtype__0",
  stats."unique__0",
  stats."%missing__0",
  stats."%empty__0",
  NULL AS "entropy__0",
  counts__0."top__0",
  counts__0."freq__0",
  stats."mean__0",
  stats."std__0",
  stats."min__0",
  stats."25%__0",
  stats."50%__0",
  stats."75%__0",
  stats."max__0",
  stats."min TZ offset__0",
  stats."max TZ offset__0",
  'VARCHAR' AS "dtype__1",
  stats."unique__1",
  stats."%missing__1",
  stats."%empty__1",
  counts__1."entropy__1",
  counts__1."top__1",
  counts__1."freq__1",
  stats."mean__1",
  stats."std__1",
  stats."min__1",
  stats."25%__1",
  stats."50%__1",
  stats."75%__1",
  stats."max__1",
  stats."min TZ offset__1",
  stats."max TZ offset__1",
  'FLOAT' AS "dtype__2",
  stats."unique__2",
  stats."%missing__2",
  stats."%empty__2",
  NULL AS "entropy__2",
  counts__2."top__2",
  counts__2."freq__2",
  stats."mean__2",
  stats."std__2",
  stats."min__2",
  stats."25%__2",
  stats."50%__2",
  stats."75%__2",
  stats."max__2",
  stats."min TZ offset__2",
  stats."max TZ offset__2",
  'INT' AS "dtype__3",
  stats."unique__3",
  stats."%missing__3",
  stats."%empty__3",
  NULL AS "entropy__3",
  counts__3."top__3",
  counts__3."freq__3",
  stats."mean__3",
  stats."std__3",
  stats."min__3",
  stats."25%__3",
  stats."50%__3",
  stats."75%__3",
  stats."max__3",
  stats."min TZ offset__3",
  stats."max TZ offset__3",
  'FLOAT' AS "dtype__4",
  stats."unique__4",
  stats."%missing__4",
  stats."%empty__4",
  NULL AS "entropy__4",
  counts__4."top__4",
  counts__4."freq__4",
  stats."mean__4",
  stats."std__4",
  stats."min__4",
  stats."25%__4",
  stats."50%__4",
  stats."75%__4",
  stats."max__4",
  stats."min TZ offset__4",
  stats."max TZ offset__4"
FROM stats
LEFT JOIN counts__0
LEFT JOIN counts__1
LEFT JOIN counts__2
LEFT JOIN counts__3
LEFT JOIN counts__4

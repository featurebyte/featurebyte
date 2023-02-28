WITH data AS (
  SELECT
    "col_float" AS "col_float",
    "col_text" AS "col_text"
  FROM "sf_database"."sf_schema"."sf_table"
  WHERE
    "event_timestamp" >= CAST('2012-11-24T11:00:00' AS TIMESTAMPNTZ)
    AND "event_timestamp" < CAST('2019-11-24T11:00:00' AS TIMESTAMPNTZ)
), casted_data AS (
  SELECT
    CAST("col_float" AS STRING) AS "col_float",
    CAST("col_text" AS STRING) AS "col_text"
  FROM data
), counts__0 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__0",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__0"
  FROM (
    SELECT
      OBJECT_AGG("col_float", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "col_float",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "col_float"
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
      OBJECT_AGG("col_text", "COUNTS") AS "COUNT_DICT"
    FROM (
      SELECT
        "col_text",
        COUNT('*') AS "COUNTS"
      FROM casted_data
      GROUP BY
        "col_text"
      ORDER BY
        COUNTS DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    COUNT(DISTINCT "col_float") AS "unique__0",
    (
      1.0 - COUNT("col_float") / COUNT('*')
    ) * 100 AS "%missing__0",
    NULL AS "%empty__0",
    AVG(CAST("col_float" AS DOUBLE)) AS "mean__0",
    STDDEV(CAST("col_float" AS DOUBLE)) AS "std__0",
    MIN("col_float") AS "min__0",
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
      "col_float") AS "25%__0",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      "col_float") AS "50%__0",
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
      "col_float") AS "75%__0",
    MAX("col_float") AS "max__0",
    NULL AS "min TZ offset__0",
    NULL AS "max TZ offset__0",
    COUNT(DISTINCT "col_text") AS "unique__1",
    (
      1.0 - COUNT("col_text") / COUNT('*')
    ) * 100 AS "%missing__1",
    COUNT_IF("col_text" = '') AS "%empty__1",
    NULL AS "mean__1",
    NULL AS "std__1",
    NULL AS "min__1",
    NULL AS "25%__1",
    NULL AS "50%__1",
    NULL AS "75%__1",
    NULL AS "max__1",
    NULL AS "min TZ offset__1",
    NULL AS "max TZ offset__1"
  FROM data
)
SELECT
  'FLOAT' AS "dtype__0",
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
  stats."max TZ offset__1"
FROM stats
LEFT JOIN counts__0
LEFT JOIN counts__1

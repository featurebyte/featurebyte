WITH "casted_data" AS (
  SELECT
    CAST("col_float" AS VARCHAR) AS "col_float",
    CAST("col_text" AS VARCHAR) AS "col_text"
  FROM "__TEMP_SAMPLED_DATA_000000000000000000000000"
), counts__1 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict."COUNT_DICT") AS "entropy__1",
    F_COUNT_DICT_MOST_FREQUENT(count_dict."COUNT_DICT") AS "top__1",
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict."COUNT_DICT") AS "freq__1"
  FROM (
    SELECT
      OBJECT_AGG(
        CASE WHEN "col_text" IS NULL THEN '__MISSING__' ELSE "col_text" END,
        TO_VARIANT("__FB_COUNTS")
      ) AS "COUNT_DICT"
    FROM (
      SELECT
        "col_text",
        COUNT(*) AS "__FB_COUNTS"
      FROM "casted_data"
      GROUP BY
        "col_text"
      ORDER BY
        "__FB_COUNTS" DESC NULLS LAST
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    COUNT(DISTINCT "col_float") AS "unique__0",
    (
      1.0 - COUNT("col_float") / NULLIF(COUNT(*), 0)
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
      1.0 - COUNT("col_text") / NULLIF(COUNT(*), 0)
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
  FROM "__TEMP_SAMPLED_DATA_000000000000000000000000"
), joined_tables_0 AS (
  SELECT
    *
  FROM stats
  LEFT JOIN counts__1
)
SELECT
  'FLOAT' AS "dtype__0",
  "unique__0",
  "%missing__0",
  "%empty__0",
  NULL AS "entropy__0",
  NULL AS "top__0",
  NULL AS "freq__0",
  "mean__0",
  "std__0",
  "min__0",
  "25%__0",
  "50%__0",
  "75%__0",
  "max__0",
  "min TZ offset__0",
  "max TZ offset__0",
  'VARCHAR' AS "dtype__1",
  "unique__1",
  "%missing__1",
  "%empty__1",
  "entropy__1",
  "top__1",
  "freq__1",
  "mean__1",
  "std__1",
  "min__1",
  "25%__1",
  "50%__1",
  "75%__1",
  "max__1",
  "min TZ offset__1",
  "max TZ offset__1"
FROM joined_tables_0

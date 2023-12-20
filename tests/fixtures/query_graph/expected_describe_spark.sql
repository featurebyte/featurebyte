WITH data AS (
  SELECT
    `ts` AS `ts`,
    `cust_id` AS `cust_id`,
    `a` AS `a`,
    `b` AS `b`,
    `a` AS `a_copy`
  FROM `db`.`public`.`event_table`
  ORDER BY
    RANDOM(1234)
  LIMIT 10
), casted_data AS (
  SELECT
    CAST(`ts` AS STRING) AS `ts`,
    CAST(`cust_id` AS STRING) AS `cust_id`,
    CAST(`a` AS STRING) AS `a`,
    CAST(`b` AS STRING) AS `b`,
    CAST(`a_copy` AS STRING) AS `a_copy`
  FROM data
), counts__0 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict.`COUNT_DICT`) AS `top__0`,
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict.`COUNT_DICT`) AS `freq__0`
  FROM (
    SELECT
      MAP_FROM_ENTRIES(
        COLLECT_LIST(STRUCT(CASE WHEN `ts` IS NULL THEN '__MISSING__' ELSE `ts` END, `__FB_COUNTS`))
      ) AS `COUNT_DICT`
    FROM (
      SELECT
        `ts`,
        COUNT(*) AS `__FB_COUNTS`
      FROM casted_data
      GROUP BY
        `ts`
      ORDER BY
        `__FB_COUNTS` DESC
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__1 AS (
  SELECT
    F_COUNT_DICT_ENTROPY(count_dict.`COUNT_DICT`) AS `entropy__1`,
    F_COUNT_DICT_MOST_FREQUENT(count_dict.`COUNT_DICT`) AS `top__1`,
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict.`COUNT_DICT`) AS `freq__1`
  FROM (
    SELECT
      MAP_FROM_ENTRIES(
        COLLECT_LIST(
          STRUCT(CASE WHEN `cust_id` IS NULL THEN '__MISSING__' ELSE `cust_id` END, `__FB_COUNTS`)
        )
      ) AS `COUNT_DICT`
    FROM (
      SELECT
        `cust_id`,
        COUNT(*) AS `__FB_COUNTS`
      FROM casted_data
      GROUP BY
        `cust_id`
      ORDER BY
        `__FB_COUNTS` DESC
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__2 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict.`COUNT_DICT`) AS `top__2`,
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict.`COUNT_DICT`) AS `freq__2`
  FROM (
    SELECT
      MAP_FROM_ENTRIES(
        COLLECT_LIST(STRUCT(CASE WHEN `a` IS NULL THEN '__MISSING__' ELSE `a` END, `__FB_COUNTS`))
      ) AS `COUNT_DICT`
    FROM (
      SELECT
        `a`,
        COUNT(*) AS `__FB_COUNTS`
      FROM casted_data
      GROUP BY
        `a`
      ORDER BY
        `__FB_COUNTS` DESC
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__3 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict.`COUNT_DICT`) AS `top__3`,
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict.`COUNT_DICT`) AS `freq__3`
  FROM (
    SELECT
      MAP_FROM_ENTRIES(
        COLLECT_LIST(STRUCT(CASE WHEN `b` IS NULL THEN '__MISSING__' ELSE `b` END, `__FB_COUNTS`))
      ) AS `COUNT_DICT`
    FROM (
      SELECT
        `b`,
        COUNT(*) AS `__FB_COUNTS`
      FROM casted_data
      GROUP BY
        `b`
      ORDER BY
        `__FB_COUNTS` DESC
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), counts__4 AS (
  SELECT
    F_COUNT_DICT_MOST_FREQUENT(count_dict.`COUNT_DICT`) AS `top__4`,
    F_COUNT_DICT_MOST_FREQUENT_VALUE(count_dict.`COUNT_DICT`) AS `freq__4`
  FROM (
    SELECT
      MAP_FROM_ENTRIES(
        COLLECT_LIST(
          STRUCT(CASE WHEN `a_copy` IS NULL THEN '__MISSING__' ELSE `a_copy` END, `__FB_COUNTS`)
        )
      ) AS `COUNT_DICT`
    FROM (
      SELECT
        `a_copy`,
        COUNT(*) AS `__FB_COUNTS`
      FROM casted_data
      GROUP BY
        `a_copy`
      ORDER BY
        `__FB_COUNTS` DESC
      LIMIT 500
    ) AS cat_counts
  ) AS count_dict
), stats AS (
  SELECT
    COUNT(DISTINCT `ts`) AS `unique__0`,
    (
      1.0 - COUNT(`ts`) / NULLIF(COUNT(*), 0)
    ) * 100 AS `%missing__0`,
    NULL AS `%empty__0`,
    NULL AS `mean__0`,
    NULL AS `std__0`,
    MIN(`ts`) AS `min__0`,
    NULL AS `25%__0`,
    NULL AS `50%__0`,
    NULL AS `75%__0`,
    MAX(`ts`) AS `max__0`,
    NULL AS `min TZ offset__0`,
    NULL AS `max TZ offset__0`,
    COUNT(DISTINCT `cust_id`) AS `unique__1`,
    (
      1.0 - COUNT(`cust_id`) / NULLIF(COUNT(*), 0)
    ) * 100 AS `%missing__1`,
    COUNT_IF(`cust_id` = '') AS `%empty__1`,
    NULL AS `mean__1`,
    NULL AS `std__1`,
    NULL AS `min__1`,
    NULL AS `25%__1`,
    NULL AS `50%__1`,
    NULL AS `75%__1`,
    NULL AS `max__1`,
    NULL AS `min TZ offset__1`,
    NULL AS `max TZ offset__1`,
    COUNT(DISTINCT `a`) AS `unique__2`,
    (
      1.0 - COUNT(`a`) / NULLIF(COUNT(*), 0)
    ) * 100 AS `%missing__2`,
    NULL AS `%empty__2`,
    AVG(CAST(`a` AS DOUBLE)) AS `mean__2`,
    STDDEV(CAST(`a` AS DOUBLE)) AS `std__2`,
    MIN(`a`) AS `min__2`,
    PERCENTILE(`a`, 0.25) AS `25%__2`,
    PERCENTILE(`a`, 0.5) AS `50%__2`,
    PERCENTILE(`a`, 0.75) AS `75%__2`,
    MAX(`a`) AS `max__2`,
    NULL AS `min TZ offset__2`,
    NULL AS `max TZ offset__2`,
    COUNT(DISTINCT `b`) AS `unique__3`,
    (
      1.0 - COUNT(`b`) / NULLIF(COUNT(*), 0)
    ) * 100 AS `%missing__3`,
    NULL AS `%empty__3`,
    AVG(CAST(`b` AS DOUBLE)) AS `mean__3`,
    STDDEV(CAST(`b` AS DOUBLE)) AS `std__3`,
    MIN(`b`) AS `min__3`,
    PERCENTILE(`b`, 0.25) AS `25%__3`,
    PERCENTILE(`b`, 0.5) AS `50%__3`,
    PERCENTILE(`b`, 0.75) AS `75%__3`,
    MAX(`b`) AS `max__3`,
    NULL AS `min TZ offset__3`,
    NULL AS `max TZ offset__3`,
    COUNT(DISTINCT `a_copy`) AS `unique__4`,
    (
      1.0 - COUNT(`a_copy`) / NULLIF(COUNT(*), 0)
    ) * 100 AS `%missing__4`,
    NULL AS `%empty__4`,
    AVG(CAST(`a_copy` AS DOUBLE)) AS `mean__4`,
    STDDEV(CAST(`a_copy` AS DOUBLE)) AS `std__4`,
    MIN(`a_copy`) AS `min__4`,
    PERCENTILE(`a_copy`, 0.25) AS `25%__4`,
    PERCENTILE(`a_copy`, 0.5) AS `50%__4`,
    PERCENTILE(`a_copy`, 0.75) AS `75%__4`,
    MAX(`a_copy`) AS `max__4`,
    NULL AS `min TZ offset__4`,
    NULL AS `max TZ offset__4`
  FROM data
), joined_tables_0 AS (
  SELECT
    *
  FROM stats
  LEFT JOIN counts__0
), joined_tables_1 AS (
  SELECT
    *
  FROM counts__1
  LEFT JOIN counts__2
), joined_tables_2 AS (
  SELECT
    *
  FROM counts__3
  LEFT JOIN counts__4
)
SELECT
  'TIMESTAMP' AS `dtype__0`,
  `unique__0`,
  `%missing__0`,
  `%empty__0`,
  NULL AS `entropy__0`,
  `top__0`,
  `freq__0`,
  `mean__0`,
  `std__0`,
  `min__0`,
  `25%__0`,
  `50%__0`,
  `75%__0`,
  `max__0`,
  `min TZ offset__0`,
  `max TZ offset__0`,
  'VARCHAR' AS `dtype__1`,
  `unique__1`,
  `%missing__1`,
  `%empty__1`,
  `entropy__1`,
  `top__1`,
  `freq__1`,
  `mean__1`,
  `std__1`,
  `min__1`,
  `25%__1`,
  `50%__1`,
  `75%__1`,
  `max__1`,
  `min TZ offset__1`,
  `max TZ offset__1`,
  'FLOAT' AS `dtype__2`,
  `unique__2`,
  `%missing__2`,
  `%empty__2`,
  NULL AS `entropy__2`,
  `top__2`,
  `freq__2`,
  `mean__2`,
  `std__2`,
  `min__2`,
  `25%__2`,
  `50%__2`,
  `75%__2`,
  `max__2`,
  `min TZ offset__2`,
  `max TZ offset__2`,
  'INT' AS `dtype__3`,
  `unique__3`,
  `%missing__3`,
  `%empty__3`,
  NULL AS `entropy__3`,
  `top__3`,
  `freq__3`,
  `mean__3`,
  `std__3`,
  `min__3`,
  `25%__3`,
  `50%__3`,
  `75%__3`,
  `max__3`,
  `min TZ offset__3`,
  `max TZ offset__3`,
  'FLOAT' AS `dtype__4`,
  `unique__4`,
  `%missing__4`,
  `%empty__4`,
  NULL AS `entropy__4`,
  `top__4`,
  `freq__4`,
  `mean__4`,
  `std__4`,
  `min__4`,
  `25%__4`,
  `50%__4`,
  `75%__4`,
  `max__4`,
  `min TZ offset__4`,
  `max TZ offset__4`
FROM joined_tables_0
LEFT JOIN joined_tables_1
LEFT JOIN joined_tables_2

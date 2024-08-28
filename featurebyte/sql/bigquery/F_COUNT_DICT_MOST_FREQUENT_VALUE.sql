CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT_VALUE`(counts JSON)
  RETURNS FLOAT64
  AS (
    `{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE`(counts, false).count
  );

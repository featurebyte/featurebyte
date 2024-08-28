CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_LEAST_FREQUENT`(counts JSON)
  RETURNS STRING
  AS (
    `{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE`(counts, true).key
  );

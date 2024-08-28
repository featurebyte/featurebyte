CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT`(counts JSON)
  RETURNS STRING
  AS (
    CAST(`{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE`(counts, false).key AS STRING)
  );

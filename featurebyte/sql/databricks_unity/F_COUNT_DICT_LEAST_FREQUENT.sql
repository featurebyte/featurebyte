CREATE OR REPLACE FUNCTION F_COUNT_DICT_LEAST_FREQUENT(counts MAP<STRING, DOUBLE>)
  RETURNS string
  LANGUAGE SQL
    return CAST(F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE(counts, TRUE).most_frequent_key AS string);
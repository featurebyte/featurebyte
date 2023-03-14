CREATE OR REPLACE FUNCTION F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE(counts variant)
  RETURNS variant
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return [null, null];
  }
  var most_frequent_key = null;
  var most_frequent_count = 0;
  for (const k in COUNTS) {
    if (COUNTS[k] > most_frequent_count) {
      most_frequent_count = COUNTS[k];
      most_frequent_key = k;
    } else if (COUNTS[k] == most_frequent_count && k < most_frequent_key) {
      most_frequent_key = k;
    }
  }
  return [most_frequent_key, most_frequent_count];
$$
;

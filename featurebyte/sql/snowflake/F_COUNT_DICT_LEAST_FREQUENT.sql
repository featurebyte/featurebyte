CREATE OR REPLACE FUNCTION F_COUNT_DICT_LEAST_FREQUENT(counts variant)
  RETURNS string
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  var least_frequent_key = null;
  var least_frequent_count = Infinity;
  for (const k in COUNTS) {
    if (!COUNTS[k]) {
      continue;
    }
    if (COUNTS[k] < least_frequent_count) {
      least_frequent_count = COUNTS[k];
      least_frequent_key = k;
    } else if (COUNTS[k] == least_frequent_count && k < least_frequent_key) {
      least_frequent_key = k;
    }
  }
  if (least_frequent_key === null) {
    return null;
  }
  return least_frequent_key
$$
;

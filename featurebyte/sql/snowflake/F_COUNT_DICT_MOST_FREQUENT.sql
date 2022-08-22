CREATE OR REPLACE FUNCTION F_COUNT_DICT_MOST_FREQUENT(counts variant)
  RETURNS variant
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  return Object.keys(COUNTS).reduce(function(a, b){ return COUNTS[a] > COUNTS[b] ? a : b });
$$
;

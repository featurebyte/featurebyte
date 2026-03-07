CREATE OR REPLACE FUNCTION F_COUNT_DICT_DIVIDE(counts variant, divisor float)
  RETURNS variant
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS || COUNTS === null) {
    return null;
  }
  if (DIVISOR === null || DIVISOR === 0) {
    return null;
  }
  var result = {};
  for (var key in COUNTS) {
    result[key] = COUNTS[key] / DIVISOR;
  }
  return result;
$$
;

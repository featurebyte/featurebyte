CREATE OR REPLACE FUNCTION F_COUNT_DICT_NORMALIZE(counts variant)
  RETURNS variant
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  var total = Object.values(COUNTS).reduce((partialSum, a) => partialSum + a, 0);
  if (total === 0) {
    return {};
  }
  var result = {};
  for (var key in COUNTS) {
    result[key] = COUNTS[key] / total;
  }
  return result;
$$
;

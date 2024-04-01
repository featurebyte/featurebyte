CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY(counts variant)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  var counts_arr = Object.values(COUNTS);
  var total = counts_arr.reduce((partialSum, a) => partialSum + Math.abs(a), 0);
  var entropy = 0.0;
  var count_length = counts_arr.length;
  for (var i = 0; i < count_length; i++) {
    if (counts_arr[i] == 0) {
      continue;
    }
    var p = Math.abs(counts_arr[i]) / total;
    entropy += p * Math.log(p);
  }
  entropy *= -1.0;
  return entropy;
$$
;

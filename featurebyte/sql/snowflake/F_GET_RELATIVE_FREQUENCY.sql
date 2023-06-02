CREATE OR REPLACE FUNCTION F_GET_RELATIVE_FREQUENCY(counts variant, key_to_use VARCHAR)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return;
  }
  if (KEY_TO_USE === undefined) {
    return;
  }
  if (!(KEY_TO_USE in COUNTS)) {
    return 0;
  }
  var counts_arr = Object.values(COUNTS);
  var total = counts_arr.reduce((partialSum, a) => partialSum + a, 0);
  var key_value = COUNTS[KEY_TO_USE]
  return key_value / total;
$$
;

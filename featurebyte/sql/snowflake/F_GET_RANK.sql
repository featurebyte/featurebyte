CREATE OR REPLACE FUNCTION F_GET_RANK(counts variant, key_to_use VARCHAR, ordering VARCHAR)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  var counts_arr = Object.values(COUNTS);
  counts_arr.sort()
  if (ordering == "True") {
    counts_arr.reverse()
  }
  // Return index + 1 since we want our ranks to start from 1
  return counts_arr.findIndex(key_to_use) + 1
$$
;

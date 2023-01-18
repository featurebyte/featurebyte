CREATE OR REPLACE FUNCTION F_GET_RANK(counts variant, key_to_use VARCHAR, ordering VARCHAR)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return null;
  }
  // Return index -1 if we are unable to find the item.
  if (!KEY_TO_USE in COUNTS) {
    return -1;
  }

  // Create an array of key-value pairs
  var counts_tuple_arr = Object.keys(COUNTS).map((key) => { return [key, COUNTS[key]] });
  counts_tuple_arr.sort(
    (first, second) => { return first[1] - second[1] }
  )
  if (ORDERING == "True") {
    counts_tuple_arr.reverse()
  }
  for (var i = 0; i < counts_tuple_arr.length; i++) {
    var current_element = counts_tuple_arr[i];
    if (current_element[0] == KEY_TO_USE) {
      // Return index + 1 since we want our ranks to start from 1
      return i + 1;
    }
  }
  return -1
$$
;

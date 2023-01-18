CREATE OR REPLACE FUNCTION F_GET_RANK(counts variant, key_to_use VARCHAR, is_descending VARCHAR)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  var counts_keys = Object.keys(COUNTS)
  if (!COUNTS || counts_keys.length == 0) {
    return null;
  }
  // Return index -1 if we are unable to find the item.
  if (!KEY_TO_USE in COUNTS) {
    return -1;
  }

  // Create an array of key-value pairs
  var counts_tuple_arr = counts_keys.map((key) => { return [key, COUNTS[key]] });
  counts_tuple_arr.sort(
    (first, second) => { return first[1] - second[1] }
  )
  if (IS_DESCENDING == "True") {
    counts_tuple_arr.reverse()
  }
  var first_element = counts_tuple_arr[0]
  var previous_key = first_element[0]
  if (previous_key == KEY_TO_USE) {
    return 1;
  }

  // If values are the same, we will return the highest rank.
  // eg.
  // {"a": 2, "b": 2}
  // get_rank("b") == get_rank("a") == 1
  var current_rank = 1;
  for (var i = 1; i < counts_tuple_arr.length; i++) {
    var current_element = counts_tuple_arr[i];
    var current_key = current_element[0]
    if (current_key == KEY_TO_USE) {
      // Return index + 1 since we want our ranks to start from 1
      return current_rank
    } else if (current_key != previous_key) {
      current_rank += 1
    }
  }
  return -1
$$
;

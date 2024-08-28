CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_GET_RANK`(COUNTS JSON, KEY_TO_USE STRING, IS_DESCENDING BOOLEAN)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""
  if (!COUNTS) {{
    return
  }}
  var counts_keys = Object.keys(COUNTS)
  if (counts_keys.length == 0) {{
    return;
  }}
  // Return index -1 if we are unable to find the item.
  if (!(KEY_TO_USE in COUNTS)) {{
    return;
  }}

  // Create an array of key-value pairs
  var counts_tuple_arr = counts_keys.map((key) => {{ return [key, COUNTS[key]] }});
  counts_tuple_arr.sort(
    (first, second) => {{ return first[1] - second[1] }}
  )
  if (IS_DESCENDING) {{
    counts_tuple_arr.reverse()
  }}

  var first_element = counts_tuple_arr[0]
  if (first_element[0] == KEY_TO_USE) {{
    return 1;
  }}

  var previous_value = first_element[1]

  // If values are the same, we will return the highest rank.
  // eg.
  // {{"a": 2, "b": 2}}
  // get_rank("b") == get_rank("a") == 1
  var current_rank = 1;
  for (var i = 1; i < counts_tuple_arr.length; i++) {{
    var current_element = counts_tuple_arr[i];
    var current_key = current_element[0]
    var current_value = current_element[1]
    // Correct key found
    if (current_key == KEY_TO_USE) {{
      if (current_value == previous_value) {{
        return current_rank
      }}
      return i + 1
    }}

    // Haven't found the correct key yet, update state if the value is new
    if (current_value != previous_value) {{
      current_rank = i + 1
      previous_value = current_value
    }}
  }}
  return
r""";

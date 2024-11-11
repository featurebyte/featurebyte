CREATE OR REPLACE FUNCTION F_GET_RANK(counts MAP<STRING, DOUBLE>, key_to_use STRING, is_descending BOOLEAN)
  RETURNS DOUBLE
  LANGUAGE PYTHON
AS
$$
  if not counts:
    return

  if key_to_use not in counts:
    return

  counts_tuple_arr = sorted(counts.items(), key=lambda x: x[1], reverse=is_descending)

  first_element = counts_tuple_arr[0]
  if first_element[0] == key_to_use:
    return 1

  previous_value = first_element[1]
  current_rank = 1
  for i in range(1, len(counts_tuple_arr)):
    current_element = counts_tuple_arr[i]
    current_key, current_value = current_element

    if current_key == key_to_use:
      if current_value == previous_value:
        return current_rank
      return i + 1

    if current_value != previous_value:
      current_rank = i + 1
      previous_value = current_value

  return
$$
;

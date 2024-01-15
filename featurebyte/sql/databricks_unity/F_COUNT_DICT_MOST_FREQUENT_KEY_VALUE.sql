CREATE OR REPLACE FUNCTION F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE(counts MAP<STRING, DOUBLE>, reversed BOOLEAN)
  RETURNS STRUCT<most_frequent_key:STRING, most_frequent_count:DOUBLE>
  LANGUAGE PYTHON
AS
$$
  import math

  if not counts:
    return None, None

  most_frequent_key = None
  most_frequent_count = -math.inf
  for k, v in counts.items():
    if not v:
      continue

    if reversed:
      v = -1 * v

    if v > most_frequent_count:
      most_frequent_count = v
      most_frequent_key = k
    elif v == most_frequent_count and k < most_frequent_key:
      most_frequent_key = k

  if most_frequent_key is None:
    return None, None
  return most_frequent_key, most_frequent_count
$$
;

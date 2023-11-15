CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY(counts MAP<STRING, INT>)
  RETURNS FLOAT
  LANGUAGE PYTHON
AS
$$
  import math

  if not counts:
    return None

  counts_arr = list(counts.values())
  total = sum(counts_arr)
  entropy = 0.0
  count_length = len(counts_arr)

  for i in range(count_length):
    p = counts_arr[i] / total
    entropy += p * math.log(p)

  return entropy * -1.0
$$
;

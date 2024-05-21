CREATE OR REPLACE FUNCTION F_COUNT_DICT_COSINE_SIMILARITY(counts1 MAP<STRING, INT>, counts2 MAP<STRING, INT>)
  RETURNS float
  LANGUAGE PYTHON
AS
$$
  import math

  if counts1 is None or counts2 is None:
    return None
  if not counts1 or not counts2:
    return 0
  if (len(counts1) < len(counts2)):
    counts = counts1
    counts_other = counts2
  else:
    counts = counts2
    counts_other = counts1

  dot_product = 0
  norm = 0
  norm_other = 0

  for k, v in counts.items():
    v = v or 0
    if k in counts_other:
      v_other = counts_other[k] or 0
      dot_product += v * v_other
    norm += v * v
  for k, v in counts_other.items():
    v = v or 0
    norm_other += v * v
  norm_product = math.sqrt(norm) * math.sqrt(norm_other)
  if norm_product == 0:
    return 0
  return dot_product / norm_product
$$
;

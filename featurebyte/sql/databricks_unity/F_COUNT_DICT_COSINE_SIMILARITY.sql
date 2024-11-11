CREATE OR REPLACE FUNCTION F_COUNT_DICT_COSINE_SIMILARITY(counts1 MAP<STRING, DOUBLE>, counts2 MAP<STRING, DOUBLE>)
  RETURNS DOUBLE
  LANGUAGE PYTHON
AS
$$
  import math
  import numpy as np

  def isnan(x):
    return x is None or np.isnan(x)

  def zero_if_nan(x):
    if isnan(x):
      return 0.0
    return x

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
    v = zero_if_nan(v)
    if k in counts_other:
      v_other = zero_if_nan(counts_other[k])
      dot_product += v * v_other
    norm += v * v
  for k, v in counts_other.items():
    v = zero_if_nan(v)
    norm_other += v * v
  norm_product = math.sqrt(norm) * math.sqrt(norm_other)
  if norm_product == 0:
    return 0
  return dot_product / norm_product
$$
;

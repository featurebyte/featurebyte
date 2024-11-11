CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY(counts MAP<STRING, DOUBLE>)
  RETURNS DOUBLE
  LANGUAGE PYTHON
AS
$$
    import numpy as np
    from scipy.stats import entropy
    if counts is None:
        return None
    if not counts:
        return 0.0
    value = entropy([abs(v) for v in counts.values()])
    if np.isnan(value):
        return 0
    return value
$$
;

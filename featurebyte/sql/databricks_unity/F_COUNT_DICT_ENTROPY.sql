CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY(counts MAP<STRING, INT>)
  RETURNS FLOAT
  LANGUAGE PYTHON
AS
$$
    from scipy.stats import entropy
    if counts is None:
        return None
    if not counts:
        return 0.0
    return entropy(list(counts.values()))
$$
;

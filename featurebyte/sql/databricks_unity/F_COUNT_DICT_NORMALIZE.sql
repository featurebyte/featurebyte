CREATE OR REPLACE FUNCTION F_COUNT_DICT_NORMALIZE(counts MAP<STRING, DOUBLE>)
  RETURNS MAP<STRING, DOUBLE>
  LANGUAGE PYTHON
AS
$$
    if counts is None:
        return None
    if not counts:
        return {}
    total = sum(counts.values())
    if total == 0:
        return {}
    return {k: v / total for k, v in counts.items()}
$$
;

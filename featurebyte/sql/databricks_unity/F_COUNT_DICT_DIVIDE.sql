CREATE OR REPLACE FUNCTION F_COUNT_DICT_DIVIDE(counts MAP<STRING, DOUBLE>, divisor DOUBLE)
  RETURNS MAP<STRING, DOUBLE>
  LANGUAGE PYTHON
AS
$$
    if counts is None:
        return None
    if divisor is None or divisor == 0:
        return None
    return {k: v / divisor for k, v in counts.items()}
$$
;

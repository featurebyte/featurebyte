CREATE OR REPLACE FUNCTION F_COUNT_DICT_NUM_UNIQUE(counts MAP<STRING, DOUBLE>)
  RETURNS DOUBLE
  LANGUAGE PYTHON
AS
$$
  if not counts:
    return 0
  return len(counts)
$$
;

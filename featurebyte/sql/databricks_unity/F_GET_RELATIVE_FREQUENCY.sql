CREATE OR REPLACE FUNCTION F_GET_RELATIVE_FREQUENCY(counts MAP<STRING, DOUBLE>, key_to_use STRING)
  RETURNS DOUBLE
  LANGUAGE PYTHON
AS
$$
  if not counts:
    return

  if not key_to_use:
    return

  if key_to_use not in counts:
    return 0

  count_sums = sum(counts.values())
  if count_sums == 0:
    return

  return counts[key_to_use] / count_sums
$$
;

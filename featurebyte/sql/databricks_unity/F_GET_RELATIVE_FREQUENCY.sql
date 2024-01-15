CREATE OR REPLACE FUNCTION F_GET_RELATIVE_FREQUENCY(counts MAP<STRING, INT>, key_to_use STRING)
  RETURNS FLOAT
  LANGUAGE PYTHON
AS
$$
  if not counts:
    return

  if not key_to_use:
    return

  if key_to_use not in counts:
    return 0

  return counts[key_to_use] / sum(counts.values())
$$
;

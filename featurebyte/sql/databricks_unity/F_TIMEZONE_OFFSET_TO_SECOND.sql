CREATE OR REPLACE FUNCTION F_TIMEZONE_OFFSET_TO_SECOND(offset_string STRING)
  RETURNS FLOAT
  LANGUAGE PYTHON
AS
$$
  import re

  if not offset_string:
    return

  groups = re.match(r'^([+-])(\d{2}):(\d{2})$', offset_string)

  if not groups:
    raise ValueError(f"Invalid timezone offset format: {offset_string}")

  sign = 1 if groups[1] == '+' else -1
  hours = int(groups[2])
  minutes = int(groups[3])

  if abs(hours) > 18 or abs(minutes) > 59:
    raise ValueError(f"Invalid timezone offset format: {offset_string}")

  return sign * (hours * 60 + minutes) * 60
$$
;

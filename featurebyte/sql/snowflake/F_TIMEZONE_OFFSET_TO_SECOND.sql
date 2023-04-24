CREATE OR REPLACE FUNCTION F_TIMEZONE_OFFSET_TO_SECOND(offset_string VARCHAR)
  RETURNS FLOAT
  LANGUAGE JAVASCRIPT
AS
$$
  if (!OFFSET_STRING) {
    return
  }

  const regex = /^([+-])(\d{2}):(\d{2})$/;
  const match = OFFSET_STRING.match(regex);

  if (!match) {
    throw new Error("Invalid timezone offset format: " + OFFSET_STRING);
  }

  const sign = match[1] === '+' ? 1 : -1;
  const hours = parseInt(match[2], 10);
  const minutes = parseInt(match[3], 10);

  if (Math.abs(hours) > 18 || Math.abs(minutes) > 59) {
    throw new Error("Invalid timezone offset format: " + OFFSET_STRING);
  }

  return sign * (hours * 60 + minutes) * 60;
$$
;

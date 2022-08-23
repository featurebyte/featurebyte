CREATE OR REPLACE FUNCTION F_COUNT_DICT_NUM_UNIQUE(counts variant)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS) {
    return 0;
  }
  return Object.keys(COUNTS).length;
$$
;

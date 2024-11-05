SELECT
  index,
  "cust_id",
  SUM("ord_size") AS sum_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec,
  COUNT("ord_size") AS count_value_avg_5b9baeccc6b74c1d85cd9bb42307af39c7f53cec
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

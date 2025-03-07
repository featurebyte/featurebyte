AGGREGATE(
  COLLECT_LIST(`arr`),
  ARRAY_REPEAT(CAST(0.0 AS DOUBLE), SIZE(FIRST(`arr`) IGNORE NULLS)),
  (acc, x) -> ZIP_WITH(acc, x, (a, b) -> a + b / COUNT(*))
)

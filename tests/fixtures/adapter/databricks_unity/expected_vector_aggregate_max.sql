AGGREGATE(
  COLLECT_LIST(`arr`),
  ARRAY_REPEAT(CAST('-inf' AS DOUBLE), SIZE(FIRST(`arr`) IGNORE NULLS)),
  (acc, x) -> ZIP_WITH(acc, x, (a, b) -> GREATEST(a, b))
)

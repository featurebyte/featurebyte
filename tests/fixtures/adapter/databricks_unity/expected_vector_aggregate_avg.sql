ZIP_WITH(
  AGGREGATE(
    COLLECT_LIST(`arr`),
    ARRAY_REPEAT(CAST(0 AS DOUBLE), SIZE(FIRST(`arr`) IGNORE NULLS)),
    (acc, x) -> ZIP_WITH(acc, x, (a, b) -> a + b)
  ),
  ARRAY_REPEAT(CAST(SUM(`count`) AS DOUBLE), SIZE(FIRST(`arr`) IGNORE NULLS)),
  (element, total_sum) -> element / total_sum
)

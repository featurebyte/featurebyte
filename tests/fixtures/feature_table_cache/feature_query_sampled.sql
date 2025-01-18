SELECT
  T0."__FB_TABLE_ROW_INDEX",
  T0."cust_id",
  T0."col_hash_1" AS "featureA",
  T1."col_hash_4" AS "featureB",
  T2."col_hash_7" AS "featureC",
  T2."col_hash_8" AS "featureD"
FROM (
  SELECT
    "__FB_TABLE_ROW_INDEX",
    "cust_id",
    "col_hash_1"
  FROM (
    SELECT
      CAST(BITAND(RANDOM(42), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
      "__FB_TABLE_ROW_INDEX",
      "cust_id",
      "col_hash_1"
    FROM (
      SELECT
        "__FB_TABLE_ROW_INDEX",
        "cust_id",
        "col_hash_1"
      FROM "FEATURE_TABLE_CACHE_000000000000000000000000"
    )
  )
  WHERE
    "prob" <= 0.003
  ORDER BY
    "prob"
  LIMIT 2
) AS T0
LEFT JOIN "FEATURE_TABLE_CACHE_000000000000000000000001" AS T1
  ON T0."__FB_TABLE_ROW_INDEX" = T1."__FB_TABLE_ROW_INDEX"
LEFT JOIN "FEATURE_TABLE_CACHE_000000000000000000000003" AS T2
  ON T0."__FB_TABLE_ROW_INDEX" = T2."__FB_TABLE_ROW_INDEX"

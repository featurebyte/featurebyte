SELECT
  T0."__FB_TABLE_ROW_INDEX",
  T0."cust_id",
  T0."col_hash_1" AS "featureA",
  T1."col_hash_4" AS "featureB",
  T2."col_hash_7" AS "featureC",
  T2."col_hash_8" AS "featureD"
FROM "FEATURE_TABLE_CACHE_000000000000000000000000_1" AS T0
LEFT JOIN "FEATURE_TABLE_CACHE_000000000000000000000000_2" AS T1
  ON T0."__FB_TABLE_ROW_INDEX" = T1."__FB_TABLE_ROW_INDEX"
LEFT JOIN "FEATURE_TABLE_CACHE_000000000000000000000000_4" AS T2
  ON T0."__FB_TABLE_ROW_INDEX" = T2."__FB_TABLE_ROW_INDEX"
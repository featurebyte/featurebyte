SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM online_store_1d1bc6c6b7e37001082cdc2f77e909b292fa4e50
WHERE
  "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2'
  AND "cust_id" IS NOT NULL

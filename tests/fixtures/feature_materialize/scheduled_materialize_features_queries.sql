INSERT INTO "fb_230525_271fb0_cust_id_30m_5m_10m_ttl" (
  "__feature_timestamp",
  "cust_id",
  "sum_30m_V220101"
) SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMPNTZ) AS "__feature_timestamp",
  "cust_id",
  "sum_30m_V220101"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

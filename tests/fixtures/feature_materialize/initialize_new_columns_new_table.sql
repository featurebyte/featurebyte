SELECT
  *
FROM "fb_230525_271fb0_cust_id_30m_5m_10m_ttl"
LIMIT 1;

CREATE TABLE "sf_db"."sf_schema"."fb_230525_271fb0_cust_id_30m_5m_10m_ttl" AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMPNTZ) AS "__feature_timestamp",
  "cust_id",
  "sum_30m_V220101"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

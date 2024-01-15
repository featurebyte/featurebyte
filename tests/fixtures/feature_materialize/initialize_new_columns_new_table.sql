SELECT
  *
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db"
LIMIT 1;

CREATE TABLE "sf_db"."sf_schema"."fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db" AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMPNTZ) AS "__feature_timestamp",
  "cust_id",
  "sum_30m_V220101"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

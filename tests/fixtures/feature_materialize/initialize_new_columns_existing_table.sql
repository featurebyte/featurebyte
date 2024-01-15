SELECT
  *
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db"
LIMIT 1;

ALTER TABLE "fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db"   ADD COLUMN "sum_30m_V220101" FLOAT;

SELECT
  MAX("__feature_timestamp") AS RESULT
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db";

MERGE INTO "fb_entity_cust_id_fjs_1800_300_600_ttl_646f6c1c0ed28a5271fb02db" AS offline_store_table USING (
  SELECT
    "cust_id",
    "sum_30m_V220101"
  FROM "TEMP_FEATURE_TABLE_000000000000000000000000"
) AS materialized_features ON offline_store_table."cust_id" = materialized_features."cust_id"
AND "__feature_timestamp" = TO_TIMESTAMP('2022-10-15 10:00:00')   WHEN MATCHED THEN UPDATE SET offline_store_table."sum_30m_V220101" = materialized_features."sum_30m_V220101"
  WHEN NOT MATCHED THEN INSERT ("__feature_timestamp", "cust_id", "sum_30m_V220101") VALUES (TO_TIMESTAMP('2022-10-15 10:00:00'), materialized_features."cust_id", materialized_features."sum_30m_V220101");

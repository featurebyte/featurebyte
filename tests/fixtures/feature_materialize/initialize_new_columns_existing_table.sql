SELECT
  *
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl"
LIMIT 1;

ALTER TABLE "fb_entity_cust_id_fjs_1800_300_600_ttl"   ADD COLUMN "sum_30m" FLOAT;

SELECT
  MAX("feature_timestamp") AS RESULT
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl";

MERGE INTO "fb_entity_cust_id_fjs_1800_300_600_ttl" AS offline_store_table USING (
  SELECT
    "cust_id",
    "sum_30m"
  FROM "TEMP_FEATURE_TABLE_000000000000000000000000"
) AS materialized_features ON offline_store_table."cust_id" = materialized_features."cust_id"
AND "feature_timestamp" = TO_TIMESTAMP('2022-10-15 10:00:00')   WHEN MATCHED THEN UPDATE SET offline_store_table."sum_30m" = materialized_features."sum_30m"
  WHEN NOT MATCHED THEN INSERT ("feature_timestamp", "cust_id", "sum_30m") VALUES (TO_TIMESTAMP('2022-10-15 10:00:00'), materialized_features."cust_id", materialized_features."sum_30m");

SELECT
  *
FROM "cat1_cust_id_30m"
LIMIT 1;

ALTER TABLE "cat1_cust_id_30m" ADD COLUMN "sum_30m_V220101" FLOAT;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

SELECT
  MAX("__feature_timestamp") AS RESULT
FROM "cat1_cust_id_30m";

MERGE INTO "cat1_cust_id_30m" AS offline_store_table USING (
  SELECT
    "cust_id",
    "sum_30m_V220101"
  FROM "TEMP_FEATURE_TABLE_000000000000000000000000"
) AS materialized_features ON offline_store_table."cust_id" = materialized_features."cust_id"
AND "__feature_timestamp" = TO_TIMESTAMP('2022-10-15 10:00:00')   WHEN MATCHED THEN UPDATE SET offline_store_table."sum_30m_V220101" = materialized_features."sum_30m_V220101"
  WHEN NOT MATCHED THEN INSERT ("__feature_timestamp", "cust_id", "sum_30m_V220101") VALUES (TO_TIMESTAMP('2022-10-15 10:00:00'), materialized_features."cust_id", materialized_features."sum_30m_V220101");

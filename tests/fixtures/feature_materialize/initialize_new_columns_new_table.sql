SELECT
  *
FROM "fb_entity_cust_id_fjs_1800_300_600_ttl"
LIMIT 1;

CREATE TABLE "sf_db"."sf_schema"."fb_entity_cust_id_fjs_1800_300_600_ttl" AS
SELECT
  SYSDATE() AS "feature_timestamp",
  "cust_id",
  "sum_30m"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

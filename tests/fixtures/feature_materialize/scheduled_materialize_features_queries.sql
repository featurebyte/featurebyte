INSERT INTO "fb_entity_cust_id_fjs_1800_300_600_ttl" (
  "__feature_timestamp",
  "cust_id",
  "sum_30m"
) SELECT
  SYSDATE() AS "__feature_timestamp",
  "cust_id",
  "sum_30m"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

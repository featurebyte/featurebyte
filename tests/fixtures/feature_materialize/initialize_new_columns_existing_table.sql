SELECT
  COUNT(*)
FROM "cat1_cust_id_30m";

ALTER TABLE "cat1_cust_id_30m" ADD COLUMN "sum_30m_V220101" FLOAT;

CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C
WHERE
  "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'
  AND NOT "cust_id" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        """cust_id""" AS "cust_id",
        "'_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'" AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
      FROM (
        SELECT
          "cust_id",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              "AGGREGATION_RESULT_NAME",
              "LATEST_VERSION"
            FROM (VALUES
              (
                '_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295',
                _fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_30m_V220101"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id",
  T0."sum_30m_V220101"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

SELECT
  COUNT(*)
FROM "cat1_cust_id_30m";

ALTER TABLE "cat1_cust_id_30m" ADD COLUMN "sum_30m_V220101" FLOAT;

SELECT
  MAX("__feature_timestamp") AS RESULT
FROM "cat1_cust_id_30m";

MERGE INTO "cat1_cust_id_30m" AS offline_store_table
USING (
  SELECT
    "cust_id",
    "sum_30m_V220101"
  FROM "TEMP_FEATURE_TABLE_000000000000000000000000"
) AS materialized_features
ON offline_store_table."cust_id" = materialized_features."cust_id"
AND "__feature_timestamp" = CAST('2022-10-15 10:00:00' AS TIMESTAMP)
WHEN MATCHED THEN UPDATE SET offline_store_table."sum_30m_V220101" = materialized_features."sum_30m_V220101"
WHEN NOT MATCHED THEN INSERT ("__feature_timestamp", "cust_id", "sum_30m_V220101") VALUES (
  CAST('2022-10-15 10:00:00' AS TIMESTAMP),
  materialized_features."cust_id",
  materialized_features."sum_30m_V220101"
);

SELECT
  COUNT(*)
FROM "cat1_cust_id_30m";

ALTER TABLE "cat1_cust_id_30m" ADD COLUMN "sum_30m_V220101" FLOAT;

CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  "cust_id"
FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    CAST('2022-01-01 00:00:00' AS TIMESTAMPNTZ) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), _FB_AGGREGATED AS (
  SELECT
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
              ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295', _fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_30m_V220101"
FROM _FB_AGGREGATED AS AGG;

MERGE INTO "cat1_cust_id_30m" AS offline_store_table USING (
  SELECT
    "cust_id",
    "sum_30m_V220101"
  FROM "TEMP_FEATURE_TABLE_000000000000000000000000"
) AS materialized_features ON FALSE   WHEN MATCHED THEN UPDATE SET offline_store_table."sum_30m_V220101" = materialized_features."sum_30m_V220101"
  WHEN NOT MATCHED THEN INSERT ("__feature_timestamp", "cust_id", "sum_30m_V220101") VALUES (TO_TIMESTAMP('2022-01-01T00:00:00'), materialized_features."cust_id", materialized_features."sum_30m_V220101");

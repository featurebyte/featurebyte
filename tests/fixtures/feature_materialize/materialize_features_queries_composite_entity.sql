CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id",
  "another_key"
FROM ONLINE_STORE_39866085BBE5CA4054C0978E965930D2F26CC229
WHERE
  "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c'
  AND (
    NOT "cust_id" IS NULL AND NOT "another_key" IS NULL
  );

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
    REQ."another_key",
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."another_key",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "another_key" AS "another_key",
      "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
    FROM (
      SELECT
        """cust_id""" AS "cust_id",
        """another_key""" AS "another_key",
        "'_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c'" AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
      FROM (
        SELECT
          "cust_id",
          "another_key",
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
                '_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c',
                _fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_39866085BBE5CA4054C0978E965930D2F26CC229 AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id" AND REQ."another_key" = T0."another_key"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  AGG."another_key",
  CAST("_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS DOUBLE) AS "composite_entity_feature_1d_V220101",
  CAST((
    "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" + 123
  ) AS DOUBLE) AS "composite_entity_feature_1d_plus_123_V220101"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id",
  REQ."another_key",
  T0."composite_entity_feature_1d_V220101",
  T0."composite_entity_feature_1d_plus_123_V220101",
  COALESCE(
    CONCAT(CAST(REQ."cust_id" AS VARCHAR), '::', CAST(REQ."another_key" AS VARCHAR)),
    ''
  ) AS "cust_id x another_key"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  "cust_id",
  "another_key"
FROM online_store_39866085bbe5ca4054c0978e965930d2f26cc229;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    REQ."another_key",
    CAST('2022-01-01 00:00:00' AS TIMESTAMPNTZ) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."another_key",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "another_key" AS "another_key",
      "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
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
              ('_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c', _fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_39866085bbe5ca4054c0978e965930d2f26cc229 AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id" AND REQ."another_key" = T0."another_key"
)
SELECT
  AGG."cust_id",
  AGG."another_key",
  "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "composite_entity_feature_1d_V220101",
  COALESCE(CONCAT(CAST("cust_id" AS VARCHAR), '::', CAST("another_key" AS VARCHAR)), '') AS "cust_id x another_key"
FROM _FB_AGGREGATED AS AGG;

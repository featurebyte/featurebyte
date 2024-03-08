CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    SYSDATE() AS POINT_IN_TIME
  FROM (
    SELECT
      1 AS "cust_id",
      0 AS "__FB_TABLE_ROW_INDEX"
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64" AS "_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64"
    FROM (
      SELECT
        "'_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64'" AS "_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64"
      FROM (
        SELECT
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
              ('_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64', _fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_e4d5f4dff76dea2d344a4cc284d7881e0b183981 AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64'))
    )
  ) AS T0
    ON TRUE
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  CASE
    WHEN (
      "_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64" IS NULL
    )
    THEN 0
    ELSE "_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64"
  END AS "count_1d"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_000000000000000000000000_1" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    SYSDATE() AS POINT_IN_TIME
  FROM (
    SELECT
      1 AS "cust_id",
      0 AS "__FB_TABLE_ROW_INDEX"
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        """cust_id""" AS "cust_id",
        "'_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
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
              ('_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295', _fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG;

SELECT
  REQ."__FB_TABLE_ROW_INDEX",
  REQ."cust_id",
  T1."sum_1d",
  T0."count_1d"
FROM REQUEST_TABLE_1 AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX"
LEFT JOIN "__TEMP_000000000000000000000000_1" AS T1
  ON REQ."__FB_TABLE_ROW_INDEX" = T1."__FB_TABLE_ROW_INDEX";

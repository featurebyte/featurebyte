WITH ONLINE_MY_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T0"."_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
  FROM ONLINE_MY_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
      "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        "CUSTOMER_ID",
        "'_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
        "'_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
      FROM (
        SELECT
          "CUSTOMER_ID",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb', '_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb', '_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb'))
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "a_2h_average",
  (
    "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" + 123
  ) AS "a_48h_average plus 123"
FROM _FB_AGGREGATED AS AGG

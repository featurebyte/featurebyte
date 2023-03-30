WITH ONLINE_MY_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T0"."_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
  FROM ONLINE_MY_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
      "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
    FROM online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "a_2h_average",
  (
    "_fb_internal_window_w172800_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" + 123
  ) AS "a_48h_average plus 123"
FROM _FB_AGGREGATED AS AGG

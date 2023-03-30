WITH ONLINE_MY_REQUEST_TABLE AS (
  SELECT
    REQ."CUSTOMER_ID",
    SYSDATE() AS POINT_IN_TIME
  FROM "MY_REQUEST_TABLE" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."CUSTOMER_ID",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" AS "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65",
    "T1"."_fb_internal_window_w604800_sum_6e33dd8addc3595450df495cd997ffd55efad68c" AS "_fb_internal_window_w604800_sum_6e33dd8addc3595450df495cd997ffd55efad68c"
  FROM ONLINE_MY_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65"
    FROM online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "BUSINESS_ID" AS "BUSINESS_ID",
      "_fb_internal_window_w604800_sum_6e33dd8addc3595450df495cd997ffd55efad68c"
    FROM online_store_b8cd14c914ca8a3a31bbfdf21e684d0d6c1936f3
  ) AS T1
    ON REQ."BUSINESS_ID" = T1."BUSINESS_ID"
)
SELECT
  AGG."CUSTOMER_ID",
  (
    "_fb_internal_window_w7200_avg_47938f0bfcde2a5c7d483ce1926aa72900653d65" / NULLIF("_fb_internal_window_w604800_sum_6e33dd8addc3595450df495cd997ffd55efad68c", 0)
  ) AS "a_2h_avg_by_user_div_7d_by_biz"
FROM _FB_AGGREGATED AS AGG

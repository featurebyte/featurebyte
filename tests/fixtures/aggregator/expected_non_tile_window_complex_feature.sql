WITH "REQUEST_TABLE_NO_TILE_W7200_F43200_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 43200) * 43200 + 1800 - 900 - 7200 AS "__FB_WINDOW_START_EPOCH",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 43200) * 43200 + 1800 - 900 AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 43200) * 43200 + 1800 - 900 - 172800 AS "__FB_WINDOW_START_EPOCH",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 43200) * 43200 + 1800 - 900 AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "VIEW_4e0f035d263d9d0d" AS (
  SELECT
    *,
    DATE_PART(EPOCH_SECOND, "ts") AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "biz_id" AS "biz_id",
      "product_type" AS "product_type",
      "a" AS "a",
      "b" AS "b"
    FROM "db"."public"."event_table"
  )
), "VIEW_3accf61d961cce7a" AS (
  SELECT
    *,
    DATE_PART(EPOCH_SECOND, "ts") AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "biz_id" AS "biz_id",
      "product_type" AS "product_type",
      "a" AS "a",
      "b" AS "b"
    FROM "db"."public"."event_table"
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W7200_43200_1800_900_0_input_1" AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W7200_43200_1800_900_0_input_1",
    "T1"."_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1" AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W7200_43200_1800_900_0_input_1"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."a"
      FROM "REQUEST_TABLE_NO_TILE_W7200_F43200_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_4e0f035d263d9d0d" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 7200) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 7200)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."a"
      FROM "REQUEST_TABLE_NO_TILE_W7200_F43200_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_4e0f035d263d9d0d" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 7200) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 7200)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."a"
      FROM "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_3accf61d961cce7a" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 172800) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 172800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."a"
      FROM "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_3accf61d961cce7a" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 172800) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 172800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST((
    "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W7200_43200_1800_900_0_input_1" / NULLIF(
      "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1",
      0
    )
  ) AS DOUBLE) AS "a_2h_48h_sum_ratio_no_tile"
FROM _FB_AGGREGATED AS AGG

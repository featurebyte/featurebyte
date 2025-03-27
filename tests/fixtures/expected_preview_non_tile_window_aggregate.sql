WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_JOB_SCHEDULE_EPOCH"
  FROM (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 43200) * 43200 + 1800 AS "__FB_JOB_SCHEDULE_EPOCH"
    FROM "REQUEST_TABLE"
  )
), "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS (
  SELECT
    "CUSTOMER_ID",
    "__FB_JOB_SCHEDULE_EPOCH",
    "__FB_JOB_SCHEDULE_EPOCH" - 900 - 172800 AS "__FB_WINDOW_START_EPOCH",
    "__FB_JOB_SCHEDULE_EPOCH" - 900 AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "CUSTOMER_ID",
      "__FB_JOB_SCHEDULE_EPOCH"
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        FLOOR((
          DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
        ) / 43200) * 43200 + 1800 AS "__FB_JOB_SCHEDULE_EPOCH"
      FROM "REQUEST_TABLE"
    )
  )
), "VIEW_069717ed253b3a84" AS (
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
    "T0"."_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1" AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
      DISTINCT_POINT_IN_TIME."CUSTOMER_ID",
      AGGREGATED."_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1"
    FROM "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS DISTINCT_POINT_IN_TIME
    LEFT JOIN (
      SELECT
        "__FB_JOB_SCHEDULE_EPOCH" AS "__FB_JOB_SCHEDULE_EPOCH",
        "CUSTOMER_ID" AS "CUSTOMER_ID",
        SUM("a") AS "_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1"
      FROM (
        SELECT
          REQ."__FB_JOB_SCHEDULE_EPOCH",
          REQ."CUSTOMER_ID",
          VIEW."a"
        FROM "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
        INNER JOIN "VIEW_069717ed253b3a84" AS VIEW
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 172800) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 172800)
          AND REQ."CUSTOMER_ID" = VIEW."cust_id"
        WHERE
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
        UNION ALL
        SELECT
          REQ."__FB_JOB_SCHEDULE_EPOCH",
          REQ."CUSTOMER_ID",
          VIEW."a"
        FROM "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
        INNER JOIN "VIEW_069717ed253b3a84" AS VIEW
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 172800) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 172800)
          AND REQ."CUSTOMER_ID" = VIEW."cust_id"
        WHERE
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      )
      GROUP BY
        "__FB_JOB_SCHEDULE_EPOCH",
        "CUSTOMER_ID"
    ) AS AGGREGATED
      ON AGGREGATED."__FB_JOB_SCHEDULE_EPOCH" = DISTINCT_POINT_IN_TIME."__FB_JOB_SCHEDULE_EPOCH"
      AND AGGREGATED."CUSTOMER_ID" = DISTINCT_POINT_IN_TIME."CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_non_tile_window_sum_a_cust_id_None_W172800_43200_1800_900_0_input_1" AS DOUBLE) AS "a_48h_sum_no_tile"
FROM _FB_AGGREGATED AS AGG

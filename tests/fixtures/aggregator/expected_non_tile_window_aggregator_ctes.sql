WITH "REQUEST_TABLE_NO_TILE_W172800_F43200_BS900_M1800_CUSTOMER_ID" AS (
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
), "VIEW_f2d5e243bf091276" AS (
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
)
SELECT
  1

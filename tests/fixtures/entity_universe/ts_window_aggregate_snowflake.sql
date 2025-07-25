SELECT DISTINCT
  CAST("store_id" AS BIGINT) AS "cust_id"
FROM (
  SELECT
    "col_int" AS "col_int",
    "col_float" AS "col_float",
    "col_char" AS "col_char",
    "col_text" AS "col_text",
    "col_binary" AS "col_binary",
    "col_boolean" AS "col_boolean",
    "date" AS "date",
    "store_id" AS "store_id",
    "another_timestamp_col" AS "another_timestamp_col"
  FROM "sf_database"."sf_schema"."time_series_table"
  WHERE
    (
      "date" >= TO_CHAR(
        DATEADD(MONTH, -1, DATEADD(MONTH, -3, __fb_current_feature_timestamp)),
        'YYYY-MM-DD HH24:MI:SS'
      )
      AND "date" <= TO_CHAR(DATEADD(MONTH, 1, __fb_current_feature_timestamp), 'YYYY-MM-DD HH24:MI:SS')
    )
    AND (
      TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS') >= DATEADD(MONTH, -3, DATEADD(MONTH, -3, DATE_TRUNC('MONTH', "__fb_current_feature_timestamp")))
      AND TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS') < DATEADD(MONTH, -3, DATE_TRUNC('MONTH', "__fb_current_feature_timestamp"))
    )
)
WHERE
  NOT "store_id" IS NULL

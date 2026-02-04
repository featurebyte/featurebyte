SELECT DISTINCT
  CAST("store_id" AS BIGINT) AS "cust_id"
FROM (
  SELECT
    "store_id" AS "store_id"
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
      TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') >= DATEADD(MONTH, -3, DATE_TRUNC('MONTH', "__fb_current_feature_timestamp"))
      AND TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') < DATE_TRUNC('MONTH', "__fb_current_feature_timestamp")
    )
)
WHERE
  NOT "store_id" IS NULL

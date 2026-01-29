WITH _FB_AGGREGATED AS (
  SELECT
    SNAPSHOTS."col_binary" AS "another_key",
    SUM(SNAPSHOTS."col_float") AS "_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1",
    {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
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
    FROM "sf_database"."sf_schema"."snapshots_table"
    WHERE
      "date" >= TO_CHAR(
        DATEADD(MONTH, -1, DATEADD(MINUTE, -4320, {{ CURRENT_TIMESTAMP }})),
        'YYYY-MM-DD HH24:MI:SS'
      )
      AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
  ) AS SNAPSHOTS
  WHERE
    TO_CHAR(
      DATEADD(SECOND, -259200, DATEADD(SECOND, -86400, DATE_TRUNC('day', {{ CURRENT_TIMESTAMP }}))),
      'YYYY-MM-DD HH24:MI:SS'
    ) = SNAPSHOTS."date"
  GROUP BY
    SNAPSHOTS."col_binary"
)
SELECT
  AGG."another_key",
  CAST("_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1" AS DOUBLE) AS "snapshots_asat_col_int_sum",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG
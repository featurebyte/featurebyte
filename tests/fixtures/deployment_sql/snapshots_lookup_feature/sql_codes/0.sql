WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."transaction_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      "col_int" AS "transaction_id"
    FROM (
      SELECT
        "col_int" AS "col_int"
      FROM "sf_database"."sf_schema"."snapshots_table"
      WHERE
        (
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MINUTE, -4320, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
        AND (
          "date" >= TO_CHAR(
            DATEADD(DAY, -7, CAST('1970-01-01 00:00:00' AS TIMESTAMP)),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(DAY, 7, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
    )
    WHERE
      NOT "col_int" IS NULL
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."transaction_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_transaction_id_lookup_col_float_project_1" AS "_fb_internal_transaction_id_lookup_col_float_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "transaction_id",
      "date",
      ANY_VALUE("_fb_internal_transaction_id_lookup_col_float_project_1") AS "_fb_internal_transaction_id_lookup_col_float_project_1"
    FROM (
      SELECT
        "col_int" AS "transaction_id",
        "date",
        "col_float" AS "_fb_internal_transaction_id_lookup_col_float_project_1"
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
      )
    )
    GROUP BY
      "date",
      "transaction_id"
  ) AS T0
    ON TO_CHAR(
      DATEADD(SECOND, -259200, DATEADD(SECOND, -86400, DATE_TRUNC('day', REQ."POINT_IN_TIME"))),
      'YYYY-MM-DD HH24:MI:SS'
    ) = T0."date"
    AND REQ."transaction_id" = T0."transaction_id"
)
SELECT
  AGG."transaction_id",
  CAST("_fb_internal_transaction_id_lookup_col_float_project_1" AS DOUBLE) AS "snapshots_lookup_feature",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG
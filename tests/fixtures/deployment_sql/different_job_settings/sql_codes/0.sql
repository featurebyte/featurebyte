WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."transaction_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      CHILD."col_int" AS "transaction_id"
    FROM (
      SELECT DISTINCT
        CHILD."col_int" AS "transaction_id"
      FROM (
        SELECT DISTINCT
          CAST("cust_id" AS BIGINT) AS "cust_id",
          "col_text" AS "another_key"
        FROM (
          SELECT
            "col_text" AS "col_text",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
          WHERE
            (
              "event_timestamp" >= DATEADD(MONTH, -1, DATEADD(MINUTE, -1440, {{ CURRENT_TIMESTAMP }}))
              AND "event_timestamp" <= DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }})
            )
            AND (
              "event_timestamp" >= CAST(FLOOR(
                (
                  EXTRACT(epoch_second FROM CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
                ) / 1800
              ) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
              AND "event_timestamp" < CAST(FLOOR(
                (
                  EXTRACT(epoch_second FROM CAST({{ CURRENT_TIMESTAMP }} AS TIMESTAMP)) - 300
                ) / 1800
              ) * 1800 + 300 - 600 AS TIMESTAMP)
            )
        )
        WHERE
          NOT "cust_id" IS NULL AND NOT "col_text" IS NULL
      ) AS PARENT
      LEFT JOIN "sf_database"."sf_schema"."snapshots_table" AS CHILD
        ON PARENT."cust_id" = CHILD."store_id"
    ) AS PARENT
    LEFT JOIN "sf_database"."sf_schema"."snapshots_table" AS CHILD
      ON PARENT."another_key" = CHILD."col_binary"
    UNION
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
), JOINED_PARENTS_DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."transaction_id" AS "transaction_id",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."transaction_id_000000000000000000000000" AS "transaction_id_000000000000000000000000",
    REQ."transaction_id_000000000000000000000000" AS "transaction_id_000000000000000000000000"
  FROM (
    SELECT
      REQ."transaction_id" AS "transaction_id",
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."transaction_id_000000000000000000000000" AS "transaction_id_000000000000000000000000",
      "T0"."transaction_id_000000000000000000000000" AS "transaction_id_000000000000000000000000"
    FROM (
      SELECT
        REQ."transaction_id",
        REQ."POINT_IN_TIME",
        "T0"."transaction_id_000000000000000000000000" AS "transaction_id_000000000000000000000000"
      FROM "DEPLOYMENT_REQUEST_TABLE" AS REQ
      LEFT JOIN (
        SELECT
          "transaction_id",
          "date",
          ANY_VALUE("transaction_id_000000000000000000000000") AS "transaction_id_000000000000000000000000"
        FROM (
          SELECT
            "col_int" AS "transaction_id",
            "date",
            "col_binary" AS "transaction_id_000000000000000000000000"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "date" AS "date",
              "created_at" AS "created_at",
              "store_id" AS "store_id",
              "another_timestamp_col" AS "another_timestamp_col"
            FROM "sf_database"."sf_schema"."snapshots_table"
          )
        )
        GROUP BY
          "date",
          "transaction_id"
      ) AS T0
        ON TO_CHAR(
          DATEADD(
            SECOND,
            -259200,
            DATEADD(
              SECOND,
              -86400,
              DATE_TRUNC('day', REQ."__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Etc/UTC_Etc/UTC")
            )
          ),
          'YYYY-MM-DD HH24:MI:SS'
        ) = T0."date"
        AND REQ."transaction_id" = T0."transaction_id"
    ) AS REQ
    LEFT JOIN (
      SELECT
        "transaction_id",
        "date",
        ANY_VALUE("transaction_id_000000000000000000000000") AS "transaction_id_000000000000000000000000"
      FROM (
        SELECT
          "col_int" AS "transaction_id",
          "date",
          "store_id" AS "transaction_id_000000000000000000000000"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "date" AS "date",
            "created_at" AS "created_at",
            "store_id" AS "store_id",
            "another_timestamp_col" AS "another_timestamp_col"
          FROM "sf_database"."sf_schema"."snapshots_table"
        )
      )
      GROUP BY
        "date",
        "transaction_id"
    ) AS T0
      ON TO_CHAR(
        DATEADD(
          SECOND,
          -259200,
          DATEADD(
            SECOND,
            -86400,
            DATE_TRUNC('day', REQ."__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Etc/UTC_Etc/UTC")
          )
        ),
        'YYYY-MM-DD HH24:MI:SS'
      ) = T0."date"
      AND REQ."transaction_id" = T0."transaction_id"
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."transaction_id",
    REQ."POINT_IN_TIME",
    REQ."transaction_id_000000000000000000000000",
    REQ."transaction_id_000000000000000000000000",
    "T0"."_fb_internal_transaction_id_lookup_col_float_project_1" AS "_fb_internal_transaction_id_lookup_col_float_project_1",
    "T1"."_fb_internal_transaction_id_000000000000000000000000_transaction_id_000000000000000000000000_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "_fb_internal_transaction_id_000000000000000000000000_transaction_id_000000000000000000000000_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
  FROM JOINED_PARENTS_DEPLOYMENT_REQUEST_TABLE AS REQ
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
  LEFT JOIN (
    SELECT
      "cust_id" AS "transaction_id_000000000000000000000000",
      "col_text" AS "transaction_id_000000000000000000000000",
      SUM("col_float") AS "_fb_internal_transaction_id_000000000000000000000000_transaction_id_000000000000000000000000_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        (
          "event_timestamp" >= DATEADD(MONTH, -1, DATEADD(MINUTE, -1440, {{ CURRENT_TIMESTAMP }}))
          AND "event_timestamp" <= DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }})
        )
        AND (
          "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
          AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
        )
    )
    GROUP BY
      "cust_id",
      "col_text"
  ) AS T1
    ON REQ."transaction_id_000000000000000000000000" = T1."transaction_id_000000000000000000000000"
    AND REQ."transaction_id_000000000000000000000000" = T1."transaction_id_000000000000000000000000"
)
SELECT
  AGG."transaction_id",
  CAST((
    "_fb_internal_transaction_id_lookup_col_float_project_1" + "_fb_internal_transaction_id_000000000000000000000000_transaction_id_000000000000000000000000_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
  ) AS DOUBLE) AS "my_feature",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG
SELECT
  index,
  "cust_id",
  SUM("added_feature") AS value_sum_ae7ea38511285eab9c15813aa9fe9c1b16f25fb4
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "cust_id" AS "cust_id",
        "col_float_scd" AS "col_float_scd",
        "col_binary_scd" AS "col_binary_scd",
        "col_boolean_scd" AS "col_boolean_scd",
        "created_at_scd" AS "created_at_scd",
        "cust_id_scd" AS "cust_id_scd",
        (
          "_fb_internal_item_sum_item_amount_event_id_col_None_join_1" + "col_float_scd"
        ) AS "added_feature"
      FROM (
        SELECT
          REQ."col_int",
          REQ."col_float",
          REQ."col_char",
          REQ."col_text",
          REQ."col_binary",
          REQ."col_boolean",
          REQ."event_timestamp",
          REQ."cust_id",
          REQ."col_float_scd",
          REQ."col_binary_scd",
          REQ."col_boolean_scd",
          REQ."created_at_scd",
          REQ."cust_id_scd",
          "T0"."_fb_internal_item_sum_item_amount_event_id_col_None_join_1" AS "_fb_internal_item_sum_item_amount_event_id_col_None_join_1"
        FROM (
          SELECT
            L."col_int" AS "col_int",
            L."col_float" AS "col_float",
            L."col_char" AS "col_char",
            L."col_text" AS "col_text",
            L."col_binary" AS "col_binary",
            L."col_boolean" AS "col_boolean",
            L."event_timestamp" AS "event_timestamp",
            L."cust_id" AS "cust_id",
            R."col_float" AS "col_float_scd",
            R."col_binary" AS "col_binary_scd",
            R."col_boolean" AS "col_boolean_scd",
            R."created_at" AS "created_at_scd",
            R."cust_id" AS "cust_id_scd"
          FROM (
            SELECT
              "__FB_KEY_COL_0",
              "__FB_LAST_TS",
              "col_int",
              "col_float",
              "col_char",
              "col_text",
              "col_binary",
              "col_boolean",
              "event_timestamp",
              "cust_id"
            FROM (
              SELECT
                "__FB_KEY_COL_0",
                LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
                "col_int",
                "col_float",
                "col_char",
                "col_text",
                "col_binary",
                "col_boolean",
                "event_timestamp",
                "cust_id",
                "__FB_EFFECTIVE_TS_COL"
              FROM (
                SELECT
                  CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
                  "cust_id" AS "__FB_KEY_COL_0",
                  NULL AS "__FB_EFFECTIVE_TS_COL",
                  2 AS "__FB_TS_TIE_BREAKER_COL",
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "cust_id" AS "cust_id"
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
                    "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
                    AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
                )
                UNION ALL
                SELECT
                  CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
                  "col_text" AS "__FB_KEY_COL_0",
                  "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
                  1 AS "__FB_TS_TIE_BREAKER_COL",
                  NULL AS "col_int",
                  NULL AS "col_float",
                  NULL AS "col_char",
                  NULL AS "col_text",
                  NULL AS "col_binary",
                  NULL AS "col_boolean",
                  NULL AS "event_timestamp",
                  NULL AS "cust_id"
                FROM (
                  SELECT
                    "col_int" AS "col_int",
                    "col_float" AS "col_float",
                    "col_text" AS "col_text",
                    "col_binary" AS "col_binary",
                    "col_boolean" AS "col_boolean",
                    "effective_timestamp" AS "effective_timestamp",
                    "end_timestamp" AS "end_timestamp",
                    "created_at" AS "created_at",
                    "cust_id" AS "cust_id"
                  FROM "sf_database"."sf_schema"."scd_table"
                )
              )
            )
            WHERE
              "__FB_EFFECTIVE_TS_COL" IS NULL
          ) AS L
          LEFT JOIN (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "effective_timestamp" AS "effective_timestamp",
              "end_timestamp" AS "end_timestamp",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."scd_table"
          ) AS R
            ON L."__FB_LAST_TS" = R."effective_timestamp" AND L."__FB_KEY_COL_0" = R."col_text"
        ) AS REQ
        LEFT JOIN (
          SELECT
            ITEM."event_id_col" AS "cust_id",
            SUM(ITEM."item_amount") AS "_fb_internal_item_sum_item_amount_event_id_col_None_join_1"
          FROM (
            SELECT
              L."event_id_col" AS "event_id_col",
              L."item_id_col" AS "item_id_col",
              L."item_type" AS "item_type",
              L."item_amount" AS "item_amount",
              L."created_at" AS "created_at",
              L."event_timestamp" AS "event_timestamp",
              R."event_timestamp" AS "event_timestamp_event_table"
            FROM (
              SELECT
                "event_id_col" AS "event_id_col",
                "item_id_col" AS "item_id_col",
                "item_type" AS "item_type",
                "item_amount" AS "item_amount",
                "created_at" AS "created_at",
                "event_timestamp" AS "event_timestamp"
              FROM "sf_database"."sf_schema"."items_table"
            ) AS L
            INNER JOIN (
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
                "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
                AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
            ) AS R
              ON L."event_id_col" = R."col_int"
          ) AS ITEM
          GROUP BY
            ITEM."event_id_col"
        ) AS T0
          ON REQ."cust_id" = T0."cust_id"
      )
    )
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id"

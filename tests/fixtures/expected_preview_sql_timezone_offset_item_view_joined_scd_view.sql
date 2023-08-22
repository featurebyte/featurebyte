SELECT
  L."event_id_col" AS "event_id_col",
  L."item_id_col" AS "item_id_col",
  L."item_type" AS "item_type",
  L."item_amount" AS "item_amount",
  CAST(L."created_at" AS STRING) AS "created_at",
  CAST(L."event_timestamp" AS STRING) AS "event_timestamp",
  L."event_timestamp_event_table" AS "event_timestamp_event_table",
  L."cust_id_event_table" AS "cust_id_event_table",
  L."tz_offset_event_table" AS "tz_offset_event_table",
  R."col_float" AS "col_float_joined",
  R."col_binary" AS "col_binary_joined",
  R."col_boolean" AS "col_boolean_joined",
  R."date_of_birth" AS "date_of_birth_joined",
  CAST(R."created_at" AS STRING) AS "created_at_joined",
  R."cust_id" AS "cust_id_joined",
  CASE
    WHEN (
      EXTRACT(month FROM DATEADD(
        second,
        F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
        L."event_timestamp_event_table"
      )) < EXTRACT(month FROM R."date_of_birth")
    )
    THEN (
      (
        EXTRACT(year FROM DATEADD(
          second,
          F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
          L."event_timestamp_event_table"
        )) - EXTRACT(year FROM R."date_of_birth")
      ) - 1
    )
    ELSE (
      EXTRACT(year FROM DATEADD(
        second,
        F_TIMEZONE_OFFSET_TO_SECOND(L."tz_offset_event_table"),
        L."event_timestamp_event_table"
      )) - EXTRACT(year FROM R."date_of_birth")
    )
  END AS "customer_age"
FROM (
  SELECT
    "__FB_KEY_COL_0",
    "__FB_LAST_TS",
    "event_id_col",
    "item_id_col",
    "item_type",
    "item_amount",
    "created_at",
    "event_timestamp",
    "event_timestamp_event_table",
    "cust_id_event_table",
    "tz_offset_event_table"
  FROM (
    SELECT
      "__FB_KEY_COL_0",
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
      "event_id_col",
      "item_id_col",
      "item_type",
      "item_amount",
      "created_at",
      "event_timestamp",
      "event_timestamp_event_table",
      "cust_id_event_table",
      "tz_offset_event_table",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "event_timestamp_event_table") AS TIMESTAMP) AS "__FB_TS_COL",
        "cust_id_event_table" AS "__FB_KEY_COL_0",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        2 AS "__FB_TS_TIE_BREAKER_COL",
        "event_id_col" AS "event_id_col",
        "item_id_col" AS "item_id_col",
        "item_type" AS "item_type",
        "item_amount" AS "item_amount",
        "created_at" AS "created_at",
        "event_timestamp" AS "event_timestamp",
        "event_timestamp_event_table" AS "event_timestamp_event_table",
        "cust_id_event_table" AS "cust_id_event_table",
        "tz_offset_event_table" AS "tz_offset_event_table"
      FROM (
        SELECT
          L."event_id_col" AS "event_id_col",
          L."item_id_col" AS "item_id_col",
          L."item_type" AS "item_type",
          L."item_amount" AS "item_amount",
          L."created_at" AS "created_at",
          L."event_timestamp" AS "event_timestamp",
          R."event_timestamp" AS "event_timestamp_event_table",
          R."cust_id" AS "cust_id_event_table",
          R."tz_offset" AS "tz_offset_event_table"
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
        LEFT JOIN (
          SELECT
            "event_timestamp" AS "event_timestamp",
            "col_int" AS "col_int",
            "cust_id" AS "cust_id",
            "tz_offset" AS "tz_offset"
          FROM "sf_database"."sf_schema"."sf_table_no_tz"
        ) AS R
          ON L."event_id_col" = R."col_int"
      )
      UNION ALL
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
        "col_text" AS "__FB_KEY_COL_0",
        "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "event_id_col",
        NULL AS "item_id_col",
        NULL AS "item_type",
        NULL AS "item_amount",
        NULL AS "created_at",
        NULL AS "event_timestamp",
        NULL AS "event_timestamp_event_table",
        NULL AS "cust_id_event_table",
        NULL AS "tz_offset_event_table"
      FROM (
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "effective_timestamp" AS "effective_timestamp",
          "end_timestamp" AS "end_timestamp",
          "date_of_birth" AS "date_of_birth",
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
    "date_of_birth" AS "date_of_birth",
    "created_at" AS "created_at",
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."scd_table"
) AS R
  ON L."__FB_LAST_TS" = R."effective_timestamp" AND L."__FB_KEY_COL_0" = R."col_text"
LIMIT 10

SELECT
  index,
  "cust_id_event_table",
  SUM("item_amount") AS value_sum_0bb04ab8ff3d46acca6e7cd7e0b9f597b4883fee
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp_event_table"), 300, 600, 30) AS index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        L."event_id_col" AS "event_id_col",
        L."item_id_col" AS "item_id_col",
        L."item_type" AS "item_type",
        L."item_amount" AS "item_amount",
        L."created_at" AS "created_at",
        L."event_timestamp" AS "event_timestamp",
        L."event_timestamp_event_table" AS "event_timestamp_event_table",
        L."cust_id_event_table" AS "cust_id_event_table",
        R."col_float" AS "col_float"
      FROM (
        SELECT
          L."event_id_col" AS "event_id_col",
          L."item_id_col" AS "item_id_col",
          L."item_type" AS "item_type",
          L."item_amount" AS "item_amount",
          L."created_at" AS "created_at",
          L."event_timestamp" AS "event_timestamp",
          R."event_timestamp" AS "event_timestamp_event_table",
          R."cust_id" AS "cust_id_event_table"
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
    )
    WHERE
      "event_timestamp_event_table" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
      AND "event_timestamp_event_table" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id_event_table"

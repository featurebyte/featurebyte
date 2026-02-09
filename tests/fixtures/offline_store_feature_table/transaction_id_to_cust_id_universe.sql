WITH ENTITY_UNIVERSE AS (
  SELECT
    __fb_current_feature_timestamp AS "POINT_IN_TIME",
    "transaction_id"
  FROM (
    SELECT DISTINCT
      "col_int" AS "transaction_id"
    FROM (
      SELECT
        "col_int" AS "col_int"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        "event_timestamp" >= __fb_last_materialized_timestamp
        AND "event_timestamp" < __fb_current_feature_timestamp
    )
    WHERE
      "col_int" IS NOT NULL
  )
), JOINED_PARENTS_ENTITY_UNIVERSE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."transaction_id" AS "transaction_id",
    REQ."cust_id" AS "cust_id"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."transaction_id",
      CASE
        WHEN REQ."POINT_IN_TIME" < "T0"."event_timestamp"
        THEN NULL
        ELSE "T0"."cust_id"
      END AS "cust_id"
    FROM "ENTITY_UNIVERSE" AS REQ
    LEFT JOIN (
      SELECT
        "transaction_id",
        ANY_VALUE("cust_id") AS "cust_id",
        ANY_VALUE("event_timestamp") AS "event_timestamp"
      FROM (
        SELECT
          "col_int" AS "transaction_id",
          "cust_id" AS "cust_id",
          "event_timestamp"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        )
      )
      GROUP BY
        "transaction_id"
    ) AS T0
      ON REQ."transaction_id" = T0."transaction_id"
  ) AS REQ
)
SELECT
  "POINT_IN_TIME",
  "transaction_id",
  "cust_id"
FROM JOINED_PARENTS_ENTITY_UNIVERSE

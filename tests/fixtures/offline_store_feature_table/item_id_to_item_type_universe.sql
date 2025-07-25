WITH ENTITY_UNIVERSE AS (
  SELECT
    __fb_current_feature_timestamp AS "POINT_IN_TIME",
    "item_id"
  FROM (
    SELECT DISTINCT
      "item_id_col"
    FROM "sf_database"."sf_schema"."items_table" AS ITEM
    INNER JOIN (
      SELECT
        "col_int"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        "event_timestamp" >= __fb_last_materialized_timestamp
        AND "event_timestamp" < __fb_current_feature_timestamp
    ) AS EVENT
      ON ITEM."event_id_col" = EVENT."col_int"
  )
), JOINED_PARENTS_ENTITY_UNIVERSE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."item_id" AS "item_id",
    REQ."item_type" AS "item_type"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."item_id",
      "T0"."item_type" AS "item_type"
    FROM "ENTITY_UNIVERSE" AS REQ
    LEFT JOIN (
      SELECT
        "item_id",
        ANY_VALUE("item_type") AS "item_type"
      FROM (
        SELECT
          "item_id_col" AS "item_id",
          "item_type" AS "item_type"
        FROM (
          SELECT
            "event_id_col" AS "event_id_col",
            "item_id_col" AS "item_id_col",
            "item_type" AS "item_type",
            "item_amount" AS "item_amount",
            "created_at" AS "created_at",
            "event_timestamp" AS "event_timestamp"
          FROM "sf_database"."sf_schema"."items_table"
        )
      )
      GROUP BY
        "item_id"
    ) AS T0
      ON REQ."item_id" = T0."item_id"
  ) AS REQ
)
SELECT
  "POINT_IN_TIME",
  "item_id",
  "item_type"
FROM JOINED_PARENTS_ENTITY_UNIVERSE

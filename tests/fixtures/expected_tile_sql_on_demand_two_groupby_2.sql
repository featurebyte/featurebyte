SELECT
  index,
  "biz_id",
  SUM("a") AS value_sum_d5ebb5711120ac12cb84f6136654c6dba7e21774
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      __FB_ENTITY_TABLE_SQL_PLACEHOLDER
    )
    SELECT
      R.*
    FROM __FB_ENTITY_TABLE_NAME
    INNER JOIN (
      SELECT
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "a" AS "a",
        "b" AS "b",
        (
          "a" + "b"
        ) AS "c"
      FROM "db"."public"."event_table"
    ) AS R
      ON R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
      AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
      AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
  )
)
GROUP BY
  index,
  "biz_id"

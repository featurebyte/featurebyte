WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      (
        "col_int" + 0.0
      ) AS "input_col_max_8ebea967ee814d7990b591359b045c522c642448",
      (
        "col_int" + 123.0
      ) AS "input_col_max_66fc16e366c1b5c9aabc812f2af4d08732087415",
      (
        "col_int" + 246.0
      ) AS "input_col_max_04b1aa48269a5bb5baf5c28888aa65a5e9f9315a",
      (
        "col_int" + 369.0
      ) AS "input_col_max_7a0389ed6a775ef2372511b34eee2f96274dc372",
      (
        "col_int" + 492.0
      ) AS "input_col_max_cf117d83e48aafafe17949305d6c886fcf246994",
      (
        "col_int" + 615.0
      ) AS "input_col_max_f81ae93bcda992e9fe17828c530998d961537513",
      (
        "col_int" + 738.0
      ) AS "input_col_max_294e76fd8322a9b9d0146b0a32167786dab74ce4",
      (
        "col_int" + 861.0
      ) AS "input_col_max_a53041702830fee7cc0f450244a06c71c041de49",
      (
        "col_int" + 984.0
      ) AS "input_col_max_217f2549e9dc7f90b4ef458d1ac6b5505fe732da",
      (
        "col_int" + 1107.0
      ) AS "input_col_max_5f9c186c56c2240cf93139cf9efea25b5138e848"
    FROM "sf_database"."sf_schema"."sf_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  MAX("input_col_max_8ebea967ee814d7990b591359b045c522c642448") AS value_max_8ebea967ee814d7990b591359b045c522c642448,
  MAX("input_col_max_66fc16e366c1b5c9aabc812f2af4d08732087415") AS value_max_66fc16e366c1b5c9aabc812f2af4d08732087415,
  MAX("input_col_max_04b1aa48269a5bb5baf5c28888aa65a5e9f9315a") AS value_max_04b1aa48269a5bb5baf5c28888aa65a5e9f9315a,
  MAX("input_col_max_7a0389ed6a775ef2372511b34eee2f96274dc372") AS value_max_7a0389ed6a775ef2372511b34eee2f96274dc372,
  MAX("input_col_max_cf117d83e48aafafe17949305d6c886fcf246994") AS value_max_cf117d83e48aafafe17949305d6c886fcf246994,
  MAX("input_col_max_f81ae93bcda992e9fe17828c530998d961537513") AS value_max_f81ae93bcda992e9fe17828c530998d961537513,
  MAX("input_col_max_294e76fd8322a9b9d0146b0a32167786dab74ce4") AS value_max_294e76fd8322a9b9d0146b0a32167786dab74ce4,
  MAX("input_col_max_a53041702830fee7cc0f450244a06c71c041de49") AS value_max_a53041702830fee7cc0f450244a06c71c041de49,
  MAX("input_col_max_217f2549e9dc7f90b4ef458d1ac6b5505fe732da") AS value_max_217f2549e9dc7f90b4ef458d1ac6b5505fe732da,
  MAX("input_col_max_5f9c186c56c2240cf93139cf9efea25b5138e848") AS value_max_5f9c186c56c2240cf93139cf9efea25b5138e848
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"

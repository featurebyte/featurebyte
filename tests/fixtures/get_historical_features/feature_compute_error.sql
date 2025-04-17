SELECT
  MIN("POINT_IN_TIME") AS "min",
  MAX("POINT_IN_TIME") AS "max"
FROM "REQUEST_TABLE_1";

CREATE TABLE "sf_db"."sf_schema"."ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000" AS
SELECT
  "cust_id" AS "cust_id",
  CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 300
  ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
  DATEADD(
    MICROSECOND,
    (
      (
        300 - 600
      ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
    ),
    CAST('1970-01-01' AS TIMESTAMP)
  ) AS __FB_ENTITY_TABLE_START_DATE
FROM "REQUEST_TABLE_1"
GROUP BY
  "cust_id";

CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000000" AS
SELECT * FROM (
            select
                index,
                "cust_id", value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295,
                current_timestamp() as created_at
            from (WITH __FB_ENTITY_TABLE_NAME AS (
  SELECT
    *
  FROM "ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000"
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_float" AS "input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM "sf_database"."sf_schema"."sf_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295") AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id")
        );

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE_1
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."cust_id" AS "cust_id",
    REQ."_fb_internal_cust_id_lookup_col_boolean_project_2" AS "_fb_internal_cust_id_lookup_col_boolean_project_2",
    "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."cust_id" AS "cust_id",
      R."col_boolean" AS "_fb_internal_cust_id_lookup_col_boolean_project_2"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TS_COL",
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "cust_id"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TS_COL",
          "__FB_TABLE_ROW_INDEX",
          "POINT_IN_TIME",
          "cust_id",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."POINT_IN_TIME",
              REQ."cust_id"
            FROM REQUEST_TABLE_1 AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "col_text" AS "__FB_KEY_COL_0",
            "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "POINT_IN_TIME",
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
              "date_of_birth" AS "date_of_birth",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."scd_table"
            WHERE
              "effective_timestamp" IS NOT NULL
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        ANY_VALUE("col_int") AS "col_int",
        ANY_VALUE("col_float") AS "col_float",
        "col_text",
        ANY_VALUE("col_binary") AS "col_binary",
        ANY_VALUE("col_boolean") AS "col_boolean",
        "effective_timestamp",
        ANY_VALUE("end_timestamp") AS "end_timestamp",
        ANY_VALUE("date_of_birth") AS "date_of_birth",
        ANY_VALUE("created_at") AS "created_at",
        ANY_VALUE("cust_id") AS "cust_id"
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
        WHERE
          "effective_timestamp" IS NOT NULL
      )
      GROUP BY
        "effective_timestamp",
        "col_text"
    ) AS R
      ON L."__FB_LAST_TS" = R."effective_timestamp"
      AND L."__FB_KEY_COL_0" = R."col_text"
      AND (
        L."__FB_TS_COL" < CAST(CONVERT_TIMEZONE('UTC', R."end_timestamp") AS TIMESTAMP)
        OR R."end_timestamp" IS NULL
      )
  ) AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d",
  "_fb_internal_cust_id_lookup_col_boolean_project_2" AS "some_lookup_feature"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_000000000000000000000001_1" AS
WITH _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."cust_id" AS "cust_id",
    REQ."_fb_internal_cust_id_lookup_col_boolean_project_2" AS "_fb_internal_cust_id_lookup_col_boolean_project_2"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."cust_id" AS "cust_id",
      R."col_boolean" AS "_fb_internal_cust_id_lookup_col_boolean_project_2"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TS_COL",
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "cust_id"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TS_COL",
          "__FB_TABLE_ROW_INDEX",
          "POINT_IN_TIME",
          "cust_id",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."POINT_IN_TIME",
              REQ."cust_id"
            FROM REQUEST_TABLE_1 AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "col_text" AS "__FB_KEY_COL_0",
            "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "POINT_IN_TIME",
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
              "date_of_birth" AS "date_of_birth",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."scd_table"
            WHERE
              "effective_timestamp" IS NOT NULL
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        ANY_VALUE("col_int") AS "col_int",
        ANY_VALUE("col_float") AS "col_float",
        "col_text",
        ANY_VALUE("col_binary") AS "col_binary",
        ANY_VALUE("col_boolean") AS "col_boolean",
        "effective_timestamp",
        ANY_VALUE("end_timestamp") AS "end_timestamp",
        ANY_VALUE("date_of_birth") AS "date_of_birth",
        ANY_VALUE("created_at") AS "created_at",
        ANY_VALUE("cust_id") AS "cust_id"
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
        WHERE
          "effective_timestamp" IS NOT NULL
      )
      GROUP BY
        "effective_timestamp",
        "col_text"
    ) AS R
      ON L."__FB_LAST_TS" = R."effective_timestamp"
      AND L."__FB_KEY_COL_0" = R."col_text"
      AND (
        L."__FB_TS_COL" < CAST(CONVERT_TIMEZONE('UTC', R."end_timestamp") AS TIMESTAMP)
        OR R."end_timestamp" IS NULL
      )
  ) AS REQ
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  "_fb_internal_cust_id_lookup_col_boolean_project_2" AS "some_lookup_feature"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_000000000000000000000001_2" AS
WITH "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE_1
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."cust_id",
    "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM REQUEST_TABLE_1 AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "SOME_HISTORICAL_FEATURE_TABLE" AS
SELECT
  REQ."POINT_IN_TIME",
  REQ."cust_id",
  T0."sum_1d"
FROM "REQUEST_TABLE_1" AS REQ
LEFT JOIN "__TEMP_000000000000000000000001_2" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

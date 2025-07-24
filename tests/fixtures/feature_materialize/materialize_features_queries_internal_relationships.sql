CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CHILD."col_text" AS "cust_id"
FROM (
  SELECT DISTINCT
    "col_boolean" AS "gender"
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
      "effective_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
      AND "effective_timestamp" < CAST('2022-01-01 00:00:00' AS TIMESTAMP)
  )
  WHERE
    NOT "col_boolean" IS NULL
) AS PARENT
LEFT JOIN "sf_database"."sf_schema"."scd_table" AS CHILD
  ON PARENT."gender" = CHILD."col_boolean"
UNION
SELECT DISTINCT
  "col_text" AS "cust_id"
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
    "effective_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
    AND "effective_timestamp" < CAST('2022-01-01 00:00:00' AS TIMESTAMP)
)
WHERE
  NOT "col_text" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), JOINED_PARENTS_ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."cust_id" AS "cust_id",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."cust_id_000000000000000000000000" AS "cust_id_000000000000000000000000"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."cust_id" AS "cust_id",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      R."col_boolean" AS "cust_id_000000000000000000000000"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TS_COL",
        "__FB_TABLE_ROW_INDEX",
        "cust_id",
        "POINT_IN_TIME"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TS_COL",
          "__FB_TABLE_ROW_INDEX",
          "cust_id",
          "POINT_IN_TIME",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."cust_id",
              REQ."POINT_IN_TIME"
            FROM "ONLINE_REQUEST_TABLE" AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "col_text" AS "__FB_KEY_COL_0",
            "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "cust_id",
            NULL AS "POINT_IN_TIME"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "is_active" AS "is_active",
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
        ANY_VALUE("is_active") AS "is_active",
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
          "is_active" AS "is_active",
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
), "REQUEST_TABLE_POINT_IN_TIME_cust_id_000000000000000000000000" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "cust_id_000000000000000000000000"
  FROM JOINED_PARENTS_ONLINE_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."cust_id" AS "cust_id",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."cust_id_000000000000000000000000" AS "cust_id_000000000000000000000000",
    REQ."_fb_internal_cust_id_lookup_col_boolean_project_1" AS "_fb_internal_cust_id_lookup_col_boolean_project_1",
    "T0"."_fb_internal_cust_id_000000000000000000000000_as_at_count_None_col_boolean_None_project_1" AS "_fb_internal_cust_id_000000000000000000000000_as_at_count_None_col_boolean_None_project_1"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."cust_id" AS "cust_id",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."cust_id_000000000000000000000000" AS "cust_id_000000000000000000000000",
      R."col_boolean" AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TS_COL",
        "__FB_TABLE_ROW_INDEX",
        "cust_id",
        "POINT_IN_TIME",
        "cust_id_000000000000000000000000"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TS_COL",
          "__FB_TABLE_ROW_INDEX",
          "cust_id",
          "POINT_IN_TIME",
          "cust_id_000000000000000000000000",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id_000000000000000000000000" AS "cust_id_000000000000000000000000"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."cust_id",
              REQ."POINT_IN_TIME",
              REQ."cust_id_000000000000000000000000"
            FROM JOINED_PARENTS_ONLINE_REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "col_text" AS "__FB_KEY_COL_0",
            "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "cust_id",
            NULL AS "POINT_IN_TIME",
            NULL AS "cust_id_000000000000000000000000"
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
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."cust_id_000000000000000000000000" AS "cust_id_000000000000000000000000",
      COUNT(*) AS "_fb_internal_cust_id_000000000000000000000000_as_at_count_None_col_boolean_None_project_1"
    FROM "REQUEST_TABLE_POINT_IN_TIME_cust_id_000000000000000000000000" AS REQ
    INNER JOIN (
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
    ) AS SCD
      ON REQ."cust_id_000000000000000000000000" = SCD."col_boolean"
      AND (
        SCD."effective_timestamp" <= REQ."POINT_IN_TIME"
        AND (
          SCD."end_timestamp" > REQ."POINT_IN_TIME" OR SCD."end_timestamp" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."cust_id_000000000000000000000000"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
    AND REQ."cust_id_000000000000000000000000" = T0."cust_id_000000000000000000000000"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  (
    CONCAT(
      (
        CONCAT(CAST("_fb_internal_cust_id_lookup_col_boolean_project_1" AS VARCHAR), '_')
      ),
      CAST("_fb_internal_cust_id_000000000000000000000000_as_at_count_None_col_boolean_None_project_1" AS VARCHAR)
    )
  ) AS "complex_parent_child_feature_V220101"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id",
  T0."complex_parent_child_feature_V220101"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

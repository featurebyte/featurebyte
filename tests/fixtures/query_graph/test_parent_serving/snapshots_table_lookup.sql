SELECT
  REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
  REQ."CITY" AS "CITY",
  REQ."DISTRICT" AS "DISTRICT"
FROM (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CITY",
    "T0"."DISTRICT" AS "DISTRICT"
  FROM "REQUEST_TABLE" AS REQ
  LEFT JOIN (
    SELECT
      "CITY",
      "snapshot_date",
      ANY_VALUE("DISTRICT") AS "DISTRICT"
    FROM (
      SELECT
        "city" AS "CITY",
        "snapshot_date",
        "district" AS "DISTRICT"
      FROM (
        SELECT
          "city" AS "city",
          "district" AS "district",
          "snapshot_date" AS "snapshot_date",
          "series_id" AS "series_id"
        FROM "sf_database"."sf_schema"."snapshots_table"
      )
    )
    GROUP BY
      "snapshot_date",
      "CITY"
  ) AS T0
    ON TO_CHAR(DATE_TRUNC('day', REQ."POINT_IN_TIME"), 'YYYY-MM-DD HH24:MI:SS') = T0."snapshot_date"
    AND REQ."CITY" = T0."CITY"
) AS REQ

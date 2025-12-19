CREATE TABLE `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  `cust_id` AS `cust_id`,
  `POINT_IN_TIME` AS `POINT_IN_TIME`
FROM (
  SELECT
    `cust_id` AS `cust_id`,
    `POINT_IN_TIME` AS `POINT_IN_TIME`
  FROM `sf_database`.`sf_schema`.`dimension_table`
);

CREATE OR REPLACE TABLE `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS `__FB_TABLE_ROW_INDEX`,
  *
FROM `BATCH_REQUEST_TABLE_000000000000000000000000`;

SELECT
  COUNT(*) AS `row_count`
FROM `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000000`;

CREATE TABLE `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000001`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  `cust_id` AS `cust_id`
FROM (
  SELECT
    `cust_id` AS `cust_id`,
    `POINT_IN_TIME` AS `POINT_IN_TIME`
  FROM `sf_database`.`sf_schema`.`dimension_table`
);

CREATE OR REPLACE TABLE `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000001`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS `__FB_TABLE_ROW_INDEX`,
  *
FROM `BATCH_REQUEST_TABLE_000000000000000000000001`;

SELECT
  COUNT(*) AS `row_count`
FROM `sf_database`.`sf_schema`.`BATCH_REQUEST_TABLE_000000000000000000000001`;

CREATE TABLE `JOINED_PARENTS_BATCH_REQUEST_TABLE_000000000000000000000001`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  REQ.`cust_id`,
  REQ.`POINT_IN_TIME`
FROM (
  SELECT
    REQ.`cust_id`,
    CAST('2025-06-19 03:00:00' AS TIMESTAMP) AS `POINT_IN_TIME`
  FROM `BATCH_REQUEST_TABLE_000000000000000000000001` AS REQ
) AS REQ;

CREATE TABLE `sf_database`.`sf_schema`.`ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  `cust_id` AS `cust_id`,
  CAST(FLOOR((
    UNIX_TIMESTAMP(MAX(POINT_IN_TIME)) - 300
  ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
  CAST(CAST(CAST(CAST('1970-01-01' AS TIMESTAMP) AS TIMESTAMP) AS DOUBLE) + (
    (
      300 - 600
    ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
  ) / 1000000.0 AS TIMESTAMP) AS __FB_ENTITY_TABLE_START_DATE
FROM `JOINED_PARENTS_BATCH_REQUEST_TABLE_000000000000000000000001`
GROUP BY
  `cust_id`;

CREATE TABLE `__TEMP_TILE_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT * FROM (
            select
                index,
                `cust_id`, value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295,
                current_timestamp() as created_at
            from (WITH __FB_ENTITY_TABLE_NAME AS (
  SELECT
    *
  FROM `ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000`
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      `event_timestamp` AS `event_timestamp`,
      `cust_id` AS `cust_id`,
      `col_float` AS `input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295`
    FROM `sf_database`.`sf_schema`.`sf_table`
  ) AS R
    ON R.`cust_id` = __FB_ENTITY_TABLE_NAME.`cust_id`
    AND R.`event_timestamp` >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R.`event_timestamp` < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  `cust_id`,
  SUM(`input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295`) AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(`event_timestamp` AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  `cust_id`)
        );

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2025-06-19 03:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_database"."sf_schema"."BATCH_REQUEST_TABLE_000000000000000000000001" AS REQ
), "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 1 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM ONLINE_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) = FLOOR(TILE.INDEX / 1)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN __TEMP_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1) - 1 = FLOOR(TILE.INDEX / 1)
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
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_30m"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE `sf_database`.`sf_schema`.`BATCH_FEATURE_TABLE_000000000000000000000002`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5',
  'delta.feature.allowColumnDefaults'='supported'
) AS
SELECT
  REQ.`cust_id`,
  T0.`sum_30m`,
  CAST('2025-06-19 03:00:00' AS TIMESTAMP) AS `POINT_IN_TIME`
FROM `BATCH_REQUEST_TABLE_000000000000000000000001` AS REQ
LEFT JOIN `__TEMP_000000000000000000000000_0` AS T0
  ON REQ.`__FB_TABLE_ROW_INDEX` = T0.`__FB_TABLE_ROW_INDEX`;

SELECT
  COUNT(*) AS `row_count`
FROM `sf_database`.`sf_schema`.`BATCH_FEATURE_TABLE_000000000000000000000002`;

SELECT
  *
FROM `db`.`schema`.`batch_prediction_table`
LIMIT 0;

INSERT INTO `db`.`schema`.`batch_prediction_table`
REPLACE WHERE `snapshot_date` = '2025-06-19'
SELECT
  CAST('2025-06-19T03:00:00' AS TIMESTAMP) AS `POINT_IN_TIME`,
  '2025-06-19' AS `snapshot_date`,
  `cust_id`,
  `sum_30m`
FROM `sf_database`.`sf_schema`.`BATCH_FEATURE_TABLE_000000000000000000000002`;

OPTIMIZE `db`.`schema`.`batch_prediction_table`
WHERE `snapshot_date` = '2025-06-19';

SELECT
  COUNT(*)
FROM `cat1_cust_id_30m`;

CREATE TABLE `sf_db`.`sf_schema`.`TEMP_REQUEST_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
SELECT DISTINCT
  `cust_id`
FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c
WHERE
  `AGGREGATION_RESULT_NAME` = '_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295';

CREATE OR REPLACE TABLE `sf_db`.`sf_schema`.`TEMP_REQUEST_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS `__FB_TABLE_ROW_INDEX`,
  *
FROM `TEMP_REQUEST_TABLE_000000000000000000000000`;

CREATE TABLE `sf_db`.`sf_schema`.`TEMP_FEATURE_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ.`cust_id`,
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM `sf_db`.`sf_schema`.`TEMP_REQUEST_TABLE_000000000000000000000000` AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ.`cust_id`,
    REQ.`POINT_IN_TIME`,
    `T0`.`_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295` AS `_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295`
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      `cust_id` AS `cust_id`,
      `_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295` AS `_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295`
    FROM (
      SELECT
        `cust_id`,
        `_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295` AS `_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295`
      FROM (
        SELECT
          `cust_id`,
          `AGGREGATION_RESULT_NAME`,
          `VALUE`
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              `AGGREGATION_RESULT_NAME`,
              `LATEST_VERSION`
            FROM VALUES
              ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295', _fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295_VERSION_PLACEHOLDER) AS version_table(`AGGREGATION_RESULT_NAME`, `LATEST_VERSION`)
          ) AS L
          INNER JOIN online_store_d2cb68c5431c0bedbc4f039bfaf8879897ce6817 AS R
            ON R.`AGGREGATION_RESULT_NAME` = L.`AGGREGATION_RESULT_NAME`
            AND R.`VERSION` = L.`LATEST_VERSION`
        )
        WHERE
          `AGGREGATION_RESULT_NAME` IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295')
      )   PIVOT(  FIRST(`VALUE`) FOR `AGGREGATION_RESULT_NAME` IN ('_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'))
    )
  ) AS T0
    ON REQ.`cust_id` = T0.`cust_id`
)
SELECT
  AGG.`cust_id`,
  CAST(`_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295` AS DOUBLE) AS `sum_30m_V220101`
FROM _FB_AGGREGATED AS AGG;

SELECT
  COUNT(*)
FROM `cat1_cust_id_30m`;

CREATE TABLE `sf_db`.`sf_schema`.`cat1_cust_id_30m`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMP) AS `__feature_timestamp`,
  `cust_id`,
  `sum_30m_V220101`
FROM `TEMP_FEATURE_TABLE_000000000000000000000000`;

ALTER TABLE `cat1_cust_id_30m` ALTER COLUMN `__feature_timestamp` SET NOT NULL;

ALTER TABLE `cat1_cust_id_30m` ALTER COLUMN `cust_id` SET NOT NULL;

ALTER TABLE `cat1_cust_id_30m` ADD CONSTRAINT `pk_cat1_cust_id_30m`
PRIMARY KEY(`__feature_timestamp` TIMESERIES, `cust_id`);

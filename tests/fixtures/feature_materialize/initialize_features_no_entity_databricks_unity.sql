SELECT
  COUNT(*)
FROM `cat1__no_entity_1d`;

CREATE TABLE `sf_db`.`sf_schema`.`TEMP_REQUEST_TABLE_000000000000000000000000`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
SELECT
  1 AS `dummy_entity`;

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
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM `sf_db`.`sf_schema`.`TEMP_REQUEST_TABLE_000000000000000000000000` AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ.`POINT_IN_TIME`,
    `T0`.`_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64` AS `_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64`
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      `_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64` AS `_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64`
    FROM (
      SELECT
        `_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64` AS `_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64`
      FROM (
        SELECT
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
              (
                '_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64',
                _fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64_VERSION_PLACEHOLDER
              ) AS version_table(`AGGREGATION_RESULT_NAME`, `LATEST_VERSION`)
          ) AS L
          INNER JOIN ONLINE_STORE_AC883F57E74EE217C9EF9A1DBFBD42301F65A71A AS R
            ON R.`AGGREGATION_RESULT_NAME` = L.`AGGREGATION_RESULT_NAME`
            AND R.`VERSION` = L.`LATEST_VERSION`
        )
        WHERE
          `AGGREGATION_RESULT_NAME` IN ('_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64')
      )
      PIVOT(FIRST(`VALUE`) FOR `AGGREGATION_RESULT_NAME` IN ('_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64'))
    )
  ) AS T0
    ON TRUE
)
SELECT
  CAST(`_fb_internal_window_w86400_count_3178e5d8142ed182c5db45462cb780d18205bd64` AS BIGINT) AS `count_1d_V220101`,
  '0' AS `__featurebyte_dummy_entity`
FROM _FB_AGGREGATED AS AGG;

SELECT
  COUNT(*)
FROM `cat1__no_entity_1d`;

CREATE TABLE `sf_db`.`sf_schema`.`cat1__no_entity_1d`
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'='name',
  'delta.minReaderVersion'='2',
  'delta.minWriterVersion'='5'
) AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMP) AS `__feature_timestamp`,
  `__featurebyte_dummy_entity`,
  `count_1d_V220101`
FROM `TEMP_FEATURE_TABLE_000000000000000000000000`;

ALTER TABLE `cat1__no_entity_1d` ALTER COLUMN `__feature_timestamp` SET NOT NULL;

ALTER TABLE `cat1__no_entity_1d` ALTER COLUMN `__featurebyte_dummy_entity` SET NOT NULL;

ALTER TABLE `cat1__no_entity_1d` ADD CONSTRAINT `pk_cat1__no_entity_1d`
PRIMARY KEY(`__feature_timestamp` TIMESERIES, `__featurebyte_dummy_entity`);

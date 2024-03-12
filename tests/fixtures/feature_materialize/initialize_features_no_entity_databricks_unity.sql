SELECT
  COUNT(*)
FROM `cat1__no_entity_1d`;

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

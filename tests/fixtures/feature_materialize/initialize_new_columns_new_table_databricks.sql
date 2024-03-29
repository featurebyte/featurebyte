SELECT
  COUNT(*)
FROM `cat1_cust_id_30m`;

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

SELECT
  *
FROM `fb_230525_271fb0_cust_id_30m_5m_10m_ttl`
LIMIT 1;

CREATE TABLE `sf_db`.`sf_schema`.`fb_230525_271fb0_cust_id_30m_5m_10m_ttl`
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

ALTER TABLE `fb_230525_271fb0_cust_id_30m_5m_10m_ttl` ALTER COLUMN `__feature_timestamp` SET NOT NULL;

ALTER TABLE `fb_230525_271fb0_cust_id_30m_5m_10m_ttl` ALTER COLUMN `cust_id` SET NOT NULL;

ALTER TABLE `fb_230525_271fb0_cust_id_30m_5m_10m_ttl` ADD CONSTRAINT `pk_fb_230525_271fb0_cust_id_30m_5m_10m_ttl`
PRIMARY KEY(`__feature_timestamp` TIMESERIES, `cust_id`);

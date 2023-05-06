-- url: https://storage.googleapis.com/featurebyte-public-datasets/creditcard_20230506.tar.gz
-- description: CreditCard Dataset

DROP DATABASE IF EXISTS CREDITCARD CASCADE;
CREATE DATABASE CREDITCARD;

-- populate BankCustomer
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/BankCustomer.parquet'
);
CREATE TABLE CREDITCARD.__BANKCUSTOMER USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.BANKCUSTOMER AS
SELECT
	`RowID`,
	`BankCustomerID`,
	`ValidFrom`,
	`ValidTo`,
	`Title`,
	`GivenName`,
	`MiddleInitial`,
	`Surname`,
	`DateOfBirth`,
	`Gender`,
	`StreetAddress`,
	`City`,
	`StateCode`,
	`ZipCode`,
	`Latitude`,
	`Longitude`,
	`record_available_at`,
	`closed_at`
FROM CREDITCARD.__BANKCUSTOMER
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CardFraudStatus
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CardFraudStatus.parquet'
);
CREATE TABLE CREDITCARD.__CARDFRAUDSTATUS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CARDFRAUDSTATUS AS
SELECT
	`RowID`,
	`CardTransactionID`,
	`Status`,
	`ValidFrom`,
	`ValidTo`,
	`record_available_at`
FROM CREDITCARD.__CARDFRAUDSTATUS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CardTransactions
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CardTransactions.parquet'
);
CREATE TABLE CREDITCARD.__CARDTRANSACTIONS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CARDTRANSACTIONS AS
SELECT
	`CardTransactionID`,
	`AccountID`,
	`Timestamp`,
	`tz_offset`,
	`CardTransactionDescription`,
	`Amount`,
	`record_available_at`
FROM CREDITCARD.__CARDTRANSACTIONS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CreditCard
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CreditCard.parquet'
);
CREATE TABLE CREDITCARD.__CREDITCARD USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CREDITCARD AS
SELECT
	`RowID`,
	`AccountID`,
	`BankCustomerID`,
	`ValidFrom`,
	`ValidTo`,
	`record_available_at`,
	`CardExpiry`,
	`CVV2`,
	`closed_at`
FROM CREDITCARD.__CREDITCARD
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate StateDetails
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/StateDetails.parquet'
);
CREATE TABLE CREDITCARD.__STATEDETAILS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.STATEDETAILS AS
SELECT
  `RowID`,
	`StateGuid`,
	`StateCode`,
	`StateName`,
	`CentroidLatitude`,
	`CentroidLongitude`,
	`Area`,
	`CensusRegion`,
	`HospitalCount`,
	`HospitalBedCount`,
	`BelowPovertyLevel`,
	`Aged65Plus`,
	`TotalPopulation`,
	`ValidFrom`,
	`record_available_at`
FROM CREDITCARD.__STATEDETAILS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CardTransactionGroups
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CardTransactionGroups.parquet'
);
CREATE TABLE CREDITCARD.CARDTRANSACTIONGROUPS USING DELTA AS SELECT * FROM temp_table;

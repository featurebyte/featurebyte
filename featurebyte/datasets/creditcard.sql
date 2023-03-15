-- url: https://storage.googleapis.com/featurebyte-public-datasets/creditcard.tar.gz
-- description: CreditCard Dataset

DROP DATABASE IF EXISTS CREDITCARD CASCADE;
CREATE DATABASE CREDITCARD;

-- populate BankCustomer
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/BankCustomer.parquet'
);
CREATE TABLE CREDITCARD.__BANKCUSTOMER USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.BANKCUSTOMER(
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
) as
SELECT * FROM CREDIT_CARD.__BANKCUSTOMER
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CardFraudStatus
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CardFraudStatus.parquet'
);
CREATE TABLE CREDITCARD.__CARDFRAUDSTATUS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CARDFRAUDSTATUS(
	`RowID`,
	`CardTransactionID`,
	`Status`,
	`ValidFrom`,
	`ValidTo`,
	`record_available_at`
) as
SELECT * FROM CREDIT_CARD.__CARDFRAUDSTATUS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CardTransactions
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CardTransactions.parquet'
);
CREATE TABLE CREDITCARD.__CARDTRANSACTIONS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CARDTRANSACTIONS(
	`CardTransactionID`,
	`AccountID`,
	`Timestamp`,
	`record_available_at`,
	`CardTransactionDescription`,
	`Amount`
) as
SELECT * FROM CREDIT_CARD.__CARDTRANSACTIONS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate CreditCard
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/CreditCard.parquet'
);
CREATE TABLE CREDITCARD.__CREDITCARD USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.CREDITCARD(
	`RowID`,
	`AccountID`,
	`BankCustomerID`,
	`ValidFrom`,
	`ValidTo`,
	`record_available_at`,
	`CardExpiry`,
	`CVV2`,
	`closed_at`
) as
SELECT * FROM CREDIT_CARD.__CREDITCARD
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate StateDetails
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/StateDetails.parquet'
);
CREATE TABLE CREDITCARD.__STATEDETAILS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW CREDITCARD.STATEDETAILS(
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
	`ValidTo`,
	`record_available_at`
) as
SELECT * FROM CREDIT_CARD.__STATEDETAILS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

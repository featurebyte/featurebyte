-- url: https://storage.googleapis.com/featurebyte-public-datasets/grocery_20230502.tar.gz
-- description: French Grocery Dataset

DROP DATABASE IF EXISTS GROCERY CASCADE;
CREATE DATABASE GROCERY;

-- populate GroceryCustomer
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryCustomer.parquet'
);
CREATE TABLE GROCERY.__GROCERYCUSTOMER USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW GROCERY.GROCERYCUSTOMER AS
SELECT
  `RowID`,
  `GroceryCustomerGuid`,
  `ValidFrom`,
  `Gender`,
  `Title`,
  `GivenName`,
  `MiddleInitial`,
  `Surname`,
  `StreetAddress`,
  `City`,
  `State`,
  `PostalCode`,
  `BrowserUserAgent`,
  `DateOfBirth`,
  `Latitude`,
  `Longitude`,
  `record_available_at`,
  LAG(`ValidFrom`) OVER (PARTITION BY `GroceryCustomerGuid` ORDER BY `ValidFrom` DESC) IS NULL AS `CurrentRecord`
FROM GROCERY.__GROCERYCUSTOMER
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate GroceryInvoice
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryInvoice.parquet'
);
CREATE TABLE GROCERY.__GROCERYINVOICE USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW GROCERY.GROCERYINVOICE AS
SELECT
    `GroceryInvoiceGuid`,
    `GroceryCustomerGuid`,
    `Timestamp`,
    `tz_offset`,
    `record_available_at`,
    `Amount`
FROM GROCERY.__GROCERYINVOICE
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate InvoiceItems
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/InvoiceItems.parquet'
);
CREATE TABLE GROCERY.__INVOICEITEMS USING DELTA AS SELECT * FROM temp_table;
CREATE OR REPLACE VIEW GROCERY.INVOICEITEMS AS
SELECT
    `GroceryInvoiceItemGuid`,
    `GroceryInvoiceGuid`,
    `GroceryProductGuid`,
    `Quantity`,
    `UnitPrice`,
    `TotalCost`,
    `Discount`,
    `record_available_at`
FROM GROCERY.__INVOICEITEMS
WHERE `record_available_at` <= CURRENT_TIMESTAMP();

-- populate GroceryProduct
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryProduct.parquet'
);
CREATE TABLE GROCERY.GROCERYPRODUCT USING DELTA AS SELECT * FROM temp_table;

DROP VIEW temp_table;

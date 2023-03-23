-- url: https://storage.googleapis.com/featurebyte-public-datasets/doctest_grocery.tar.gz
-- description: Grocery dataset for docs testing

DROP DATABASE IF EXISTS DOCTEST_GROCERY CASCADE;
CREATE DATABASE DOCTEST_GROCERY;

-- populate GroceryCustomer
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryCustomer.parquet'
);
CREATE TABLE DOCTEST_GROCERY.GROCERYCUSTOMER USING DELTA AS SELECT * FROM temp_table;

-- populate GroceryInvoice
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryInvoice.parquet'
);
CREATE TABLE DOCTEST_GROCERY.GROCERYINVOICE USING DELTA AS SELECT * FROM temp_table;

-- populate InvoiceItems
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/InvoiceItems.parquet'
);
CREATE TABLE DOCTEST_GROCERY.INVOICEITEMS USING DELTA AS SELECT * FROM temp_table;

-- populate GroceryProduct
CREATE OR REPLACE TEMP VIEW temp_table
USING parquet OPTIONS (
    path '{staging_path}/GroceryProduct.parquet'
);
CREATE TABLE DOCTEST_GROCERY.GROCERYPRODUCT USING DELTA AS SELECT * FROM temp_table;

DROP VIEW temp_table;

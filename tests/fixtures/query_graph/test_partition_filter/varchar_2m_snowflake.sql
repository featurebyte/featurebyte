"partition_col" >= TO_CHAR(CAST('2022-12-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
AND "partition_col" <= TO_CHAR(CAST('2023-07-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')

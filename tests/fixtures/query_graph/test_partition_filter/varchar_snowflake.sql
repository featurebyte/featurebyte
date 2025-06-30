"partition_col" >= TO_CHAR(CAST('2023-02-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')
AND "partition_col" <= TO_CHAR(CAST('2023-05-01 00:00:00' AS TIMESTAMP), 'yyyy-MM-dd')

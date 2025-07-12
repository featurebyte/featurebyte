`partition_col` >= FORMAT_DATETIME('yyyy-MM-dd', CAST('2022-12-01 00:00:00' AS DATETIME))
AND `partition_col` <= FORMAT_DATETIME('yyyy-MM-dd', CAST('2023-07-01 00:00:00' AS DATETIME))

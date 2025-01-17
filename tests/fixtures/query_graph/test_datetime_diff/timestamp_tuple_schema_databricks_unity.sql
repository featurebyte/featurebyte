(
  DATEDIFF(
    microsecond,
    TO_TIMESTAMP(CAST(GET(zipped_column_2, 'timestamp') AS STRING), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    TO_TIMESTAMP(CAST(GET(zipped_column_1, 'timestamp') AS STRING), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
  ) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT)
)

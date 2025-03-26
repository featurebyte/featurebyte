WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      (
        "col_int" + 1230.0
      ) AS "input_col_max_cc732f2766b32468e3b026f79aa4f62ba0a43cbb",
      (
        "col_int" + 1353.0
      ) AS "input_col_max_8f0fb4310a3f8030aeac8457a91e47570f9fc823",
      (
        "col_int" + 1476.0
      ) AS "input_col_max_e36c6a2d2cb25bd0a07d24f60fe69b191bd31536",
      (
        "col_int" + 1599.0
      ) AS "input_col_max_5c841e4276de72b8ae4395b314186cfd4490d32f",
      (
        "col_int" + 1722.0
      ) AS "input_col_max_8076f9cea53a4b2bb24b10231b225df51e4f022f",
      (
        "col_int" + 1845.0
      ) AS "input_col_max_cc8379055af579870915276f38a470934d61e6bd",
      (
        "col_int" + 1968.0
      ) AS "input_col_max_0d3e7252a0ebaa61c8db7faaf65d27bf278c2152",
      (
        "col_int" + 2091.0
      ) AS "input_col_max_83cc4131d5b87ae06257ef8a595c719a9de1c525",
      (
        "col_int" + 2214.0
      ) AS "input_col_max_777c038d9440890b081de40c56f975b882cc75ac",
      (
        "col_int" + 2337.0
      ) AS "input_col_max_55f3230d7d572d67e1fe57fff6bbb25415a02753"
    FROM "sf_database"."sf_schema"."sf_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  MAX("input_col_max_cc732f2766b32468e3b026f79aa4f62ba0a43cbb") AS value_max_cc732f2766b32468e3b026f79aa4f62ba0a43cbb,
  MAX("input_col_max_8f0fb4310a3f8030aeac8457a91e47570f9fc823") AS value_max_8f0fb4310a3f8030aeac8457a91e47570f9fc823,
  MAX("input_col_max_e36c6a2d2cb25bd0a07d24f60fe69b191bd31536") AS value_max_e36c6a2d2cb25bd0a07d24f60fe69b191bd31536,
  MAX("input_col_max_5c841e4276de72b8ae4395b314186cfd4490d32f") AS value_max_5c841e4276de72b8ae4395b314186cfd4490d32f,
  MAX("input_col_max_8076f9cea53a4b2bb24b10231b225df51e4f022f") AS value_max_8076f9cea53a4b2bb24b10231b225df51e4f022f,
  MAX("input_col_max_cc8379055af579870915276f38a470934d61e6bd") AS value_max_cc8379055af579870915276f38a470934d61e6bd,
  MAX("input_col_max_0d3e7252a0ebaa61c8db7faaf65d27bf278c2152") AS value_max_0d3e7252a0ebaa61c8db7faaf65d27bf278c2152,
  MAX("input_col_max_83cc4131d5b87ae06257ef8a595c719a9de1c525") AS value_max_83cc4131d5b87ae06257ef8a595c719a9de1c525,
  MAX("input_col_max_777c038d9440890b081de40c56f975b882cc75ac") AS value_max_777c038d9440890b081de40c56f975b882cc75ac,
  MAX("input_col_max_55f3230d7d572d67e1fe57fff6bbb25415a02753") AS value_max_55f3230d7d572d67e1fe57fff6bbb25415a02753
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
